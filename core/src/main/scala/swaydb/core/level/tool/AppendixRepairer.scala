/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.level.tool

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.IO
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.AppendixLogCache
import swaydb.core.log.serializer.LogEntryWriter
import swaydb.core.log.{Log, LogEntry}
import swaydb.core.segment.Segment
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.data.config.{ForceSave, MMAP}
import swaydb.slice.order.KeyOrder
import swaydb.data.repairAppendix.AppendixRepairStrategy._
import swaydb.data.repairAppendix.{AppendixRepairStrategy, OverlappingSegmentsException, SegmentInfoUnTyped}
import swaydb.slice.Slice
import swaydb.slice.SliceIOImplicits._
import swaydb.effect.{Effect, Extension}
import swaydb.utils.StorageUnits._

import java.nio.file.Path

private[swaydb] object AppendixRepairer extends LazyLogging {

  def apply(levelPath: Path,
            strategy: AppendixRepairStrategy)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                              fileSweeper: FileSweeper): IO[swaydb.Error.Level, Unit] = {

    import swaydb.core.log.serializer.AppendixLogEntryWriter._
    implicit val memorySweeper: Option[MemorySweeper.KeyValue] = Option.empty
    //mmap is false. FIXME - use ByteBufferCleaner.Disabled instead
    implicit val bufferCleaner: ByteBufferSweeperActor = null
    implicit val forceSaveApplier: ForceSaveApplier.On.type = ForceSaveApplier.On

    IO(Effect.files(levelPath, Extension.Seg)) flatMap {
      files =>
        files
          .mapRecoverIO {
            segmentPath =>
              IO {
                Segment(
                  path = segmentPath,
                  mmap = MMAP.Off(ForceSave.Off),
                  checkExists = true
                )(keyOrder = keyOrder,
                  timeOrder = null,
                  functionStore = null,
                  blockCacheSweeper = None,
                  keyValueMemorySweeper = memorySweeper,
                  fileSweeper = fileSweeper,
                  bufferCleaner = bufferCleaner,
                  forceSaveApplier = forceSaveApplier
                )
              }
          }
          .flatMap {
            segments =>
              checkOverlappingSegments(segments, strategy) flatMap {
                _ =>
                  buildAppendixMap(levelPath.resolve("appendix"), segments.filter(_.existsOnDisk))
              }
          }
    }
  }

  def applyRecovery(segment: Segment,
                    overlappingSegment: Segment,
                    strategy: AppendixRepairStrategy): IO[swaydb.Error.Level, Unit] =
    strategy match {
      case KeepNew =>
        logger.info(
          s"${KeepNew.getClass.getSimpleName.dropRight(1)} recovery strategy selected. Deleting old {}",
          segment.path
        )
        IO(segment.delete)

      case KeepOld =>
        logger.info(
          s"${KeepOld.getClass.getSimpleName.dropRight(1)} recovery strategy selected. Deleting new {}.",
          overlappingSegment.path
        )
        IO(overlappingSegment.delete)

      case ReportFailure =>
        IO(segment.keyValueCount) flatMap {
          segmentKeyValueCount =>
            IO(overlappingSegment.keyValueCount) flatMap {
              overlappingSegmentKeyValueCount =>
                IO.Left(
                  swaydb.Error.Fatal(
                    OverlappingSegmentsException(
                      segmentInfo =
                        SegmentInfoUnTyped(
                          path = segment.path,
                          minKey = segment.minKey,
                          maxKey = segment.maxKey,
                          segmentSize = segment.segmentSize,
                          keyValueCount = segmentKeyValueCount
                        ),
                      overlappingSegmentInfo =
                        SegmentInfoUnTyped(
                          path = overlappingSegment.path,
                          minKey = overlappingSegment.minKey,
                          maxKey = overlappingSegment.maxKey,
                          segmentSize = overlappingSegment.segmentSize,
                          keyValueCount = overlappingSegmentKeyValueCount
                        )
                    )
                  )
                )
            }
        }
    }

  def checkOverlappingSegments(segments: Slice[Segment],
                               strategy: AppendixRepairStrategy)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Level, Int] =
    segments.foldLeftRecoverIO(1) {
      case (position, segment) =>
        logger.info("Checking for overlapping Segments for Segment {}", segment.path)
        segments.drop(position) find {
          targetSegment =>
            val overlaps = Segment.overlaps(segment, targetSegment)
            if (overlaps)
              logger.info(s"Is overlapping with {} = {}", targetSegment.path, overlaps)
            else
              logger.trace(s"Is overlapping with {} = {}", targetSegment.path, overlaps)
            overlaps
        } match {
          case Some(overlappingSegment) =>
            applyRecovery(segment, overlappingSegment, strategy) match {
              case IO.Right(_) =>
                return checkOverlappingSegments(segments.drop(position - 1).filter(_.existsOnDisk), strategy)

              case IO.Left(error) =>
                IO.Left(error)
            }

          case None =>
            IO.Right(position + 1)
        }
    }

  def buildAppendixMap(appendixDir: Path,
                       segments: Slice[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                 fileSweeper: FileSweeper,
                                                 bufferCleaner: ByteBufferSweeperActor,
                                                 forceSaveApplier: ForceSaveApplier,
                                                 writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Segment]]): IO[swaydb.Error.Level, Unit] =
    IO {
      Effect.walkDelete(appendixDir)

      val mmap =
        MMAP.Off(
          forceSave =
            ForceSave.BeforeClose(
              enableBeforeCopy = true,
              enableForReadOnlyMode = true,
              logBenchmark = false
            )
        )

      Log.persistent[Slice[Byte], Segment, AppendixLogCache](
        folder = appendixDir,
        mmap = mmap,
        flushOnOverflow = true,
        fileSize = 1.gb
      )
    } flatMap {
      appendix =>
        segments foreachIO {
          segment =>
            appendix.writeSafe(LogEntry.Put(segment.minKey, segment))
        } match {
          case Some(IO.Left(error)) =>
            IO.Left(error)

          case None =>
            IO.unit
        }
    }
}
