/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level.tool

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.MemorySweeper
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{Effect, ForceSaveApplier}
import swaydb.core.level.AppendixSkipListMerger
import swaydb.core.map.serializer.MapEntryWriter
import swaydb.core.map.{Map, MapEntry, SkipListMerger}
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.util.Extension
import swaydb.data.config.{ForceSave, MMAP}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.repairAppendix.AppendixRepairStrategy._
import swaydb.data.repairAppendix.{AppendixRepairStrategy, OverlappingSegmentsException, SegmentInfoUnTyped}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.util.StorageUnits._

private[swaydb] object AppendixRepairer extends LazyLogging {

  def apply(levelPath: Path,
            strategy: AppendixRepairStrategy)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                              fileSweeper: FileSweeperActor,
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore): IO[swaydb.Error.Level, Unit] = {

    import swaydb.core.map.serializer.AppendixMapEntryWriter._
    implicit val merger = AppendixSkipListMerger
    implicit val memorySweeper = Option.empty[MemorySweeper.KeyValue]
    //mmap is false. FIXME - use ByteBufferCleaner.Disabled instead
    implicit val bufferCleaner: ByteBufferSweeperActor = null
    implicit val forceSaveApplier = ForceSaveApplier.DefaultApplier

    IO(Effect.files(levelPath, Extension.Seg)) flatMap {
      files =>
        files
          .mapRecoverIO {
            segmentPath =>
              IO {
                Segment(
                  path = segmentPath,
                  mmap = MMAP.Disabled(ForceSave.Disabled),
                  checkExists = true
                )(keyOrder = keyOrder,
                  timeOrder = timeOrder,
                  functionStore = functionStore,
                  blockCache = None,
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
        IO(segment.getKeyValueCount()) flatMap {
          segmentKeyValueCount =>
            IO(overlappingSegment.getKeyValueCount()) flatMap {
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
                                                 timeOrder: TimeOrder[Slice[Byte]],
                                                 fileSweeper: FileSweeperActor,
                                                 bufferCleaner: ByteBufferSweeperActor,
                                                 forceSaveApplier: ForceSaveApplier,
                                                 functionStore: FunctionStore,
                                                 writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Segment]],
                                                 skipListMerger: SkipListMerger[SliceOption[Byte], SegmentOption, Slice[Byte], Segment]): IO[swaydb.Error.Level, Unit] =
    IO {
      Effect.walkDelete(appendixDir)

      val mmap =
        MMAP.Disabled(
          forceSave =
            ForceSave.BeforeClose(
              enableBeforeCopy = true,
              enableForReadOnly = true,
              logBenchmark = false
            )
        )

      Map.persistent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](
        nullKey = Slice.Null,
        nullValue = Segment.Null,
        folder = appendixDir,
        mmap = mmap,
        flushOnOverflow = true,
        fileSize = 1.gb
      )
    } flatMap {
      appendix =>
        segments foreachIO {
          segment =>
            appendix.writeSafe(MapEntry.Put(segment.minKey, segment))
        } match {
          case Some(IO.Left(error)) =>
            IO.Left(error)

          case None =>
            IO.unit
        }
    }
}
