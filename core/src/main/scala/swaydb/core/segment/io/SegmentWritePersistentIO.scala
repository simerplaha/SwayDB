/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.io

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO._
import swaydb.core.data.DefIO
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{DBFile, ForceSaveApplier}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment._
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.IDGenerator
import swaydb.data.config.{ForceSave, MMAP}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import java.nio.file.Path
import scala.collection.mutable.ListBuffer

object SegmentWritePersistentIO extends SegmentWriteIO[TransientSegment.Persistent, PersistentSegment] with LazyLogging {

  override def minKey(segment: PersistentSegment): Slice[Byte] =
    segment.minKey

  def persistMerged(pathsDistributor: PathsDistributor,
                    segmentRefCacheWeight: Int,
                    mmap: MMAP.Segment,
                    mergeResult: Iterable[DefIO[SegmentOption, Iterable[TransientSegment.Persistent]]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                                                                        functionStore: FunctionStore,
                                                                                                        fileSweeper: FileSweeper,
                                                                                                        bufferCleaner: ByteBufferSweeperActor,
                                                                                                        keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                        blockCacheSweeper: Option[MemorySweeper.Block],
                                                                                                        segmentReadIO: SegmentReadIO,
                                                                                                        idGenerator: IDGenerator,
                                                                                                        forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[DefIO[SegmentOption, Iterable[PersistentSegment]]]] =
    mergeResult collect {
      //collect the ones with source set or has new segments to write
      case mergeResult if mergeResult.input.isSomeS || mergeResult.output.nonEmpty =>
        mergeResult

    } mapRecoverIO[DefIO[SegmentOption, Iterable[PersistentSegment]]](
      mergeResult =>
        persistTransient(
          pathsDistributor = pathsDistributor,
          segmentRefCacheWeight = segmentRefCacheWeight,
          mmap = mmap,
          transient = mergeResult.output
        ) map {
          segment =>
            mergeResult.copyOutput(segment)
        },
      recover =
        (segments, _) =>
          segments foreach {
            segmentToDelete =>
              segmentToDelete
                .output
                .foreachIO(segment => IO(segment.delete()), failFast = false)
                .foreach {
                  error =>
                    logger.error(s"Failed to delete Segment in recovery", error.exception)
                }
          }
    )

  def persistTransient(pathsDistributor: PathsDistributor,
                       segmentRefCacheWeight: Int,
                       mmap: MMAP.Segment,
                       transient: Iterable[TransientSegment.Persistent])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                                         functionStore: FunctionStore,
                                                                         fileSweeper: FileSweeper,
                                                                         bufferCleaner: ByteBufferSweeperActor,
                                                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                         blockCacheSweeper: Option[MemorySweeper.Block],
                                                                         segmentReadIO: SegmentReadIO,
                                                                         idGenerator: IDGenerator,
                                                                         forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[PersistentSegment]] =
    transient.flatMapRecoverIO[PersistentSegment](
      ioBlock =
        segment =>
          IO {
            if (segment.hasEmptyByteSlice) {
              //This is fatal!! Empty Segments should never be created. If this does have for whatever reason it should
              //not be allowed so that whatever is creating this Segment (eg: compaction) does not progress with a success response.
              throw IO.throwable("Empty key-values submitted to persistent Segment.")
            } else {
              val path = pathsDistributor.next.resolve(IDGenerator.segment(idGenerator.next))

              segment match {
                case segment: TransientSegment.One =>
                  val file: DBFile =
                    segmentFile(
                      path = path,
                      mmap = mmap,
                      segmentSize = segment.segmentSize,
                      byteTransferer =
                        file => {
                          file.append(segment.fileHeader)
                          file.append(segment.bodyBytes)
                        }
                    )

                  Slice(
                    PersistentSegmentOne(
                      file = file,
                      segment = segment
                    )
                  )

                case segment: TransientSegment.RemoteRef =>
                  val segmentSize = segment.segmentSize

                  val file: DBFile =
                    segmentFile(
                      path = path,
                      mmap = mmap,
                      segmentSize = segmentSize,
                      byteTransferer =
                        file => {
                          file.append(segment.fileHeader)
                          segment.ref.segmentBlockCache.transfer(0, segment.segmentSizeIgnoreHeader, file)
                        }
                    )

                  Slice(
                    PersistentSegmentOne(
                      file = file,
                      segment = segment
                    )
                  )

                case segment: TransientSegment.RemotePersistentSegment =>
                  Slice(
                    Segment.copyToPersist(
                      segment = segment.segment,
                      pathsDistributor = pathsDistributor,
                      segmentRefCacheWeight = segmentRefCacheWeight,
                      mmap = mmap
                    )
                  )

                case segment: TransientSegment.Many =>
                  val file: DBFile =
                    segmentFile(
                      path = path,
                      mmap = mmap,
                      segmentSize = segment.segmentSize,
                      byteTransferer =
                        file => {
                          file.append(segment.fileHeader)
                          file.append(segment.listSegment.bodyBytes)

                          writeOrTransfer(
                            segments = segment.segments,
                            target = file
                          )
                        }
                    )

                  Slice(
                    PersistentSegmentMany(
                      file = file,
                      segmentRefCacheWeight = segmentRefCacheWeight,
                      segment = segment
                    )
                  )
              }
            }
          },
      recover =
        (segments: Iterable[PersistentSegment], _: IO.Left[swaydb.Error.Segment, Slice[Segment]]) =>
          segments
            .foreachIO(segment => IO(segment.delete()), failFast = false)
            .foreach {
              error =>
                logger.error(s"Failed to delete Segment in recovery", error.exception)
            }
    )

  /**
   * Write segments to target file and also attempts to batch transfer bytes.
   */
  private def writeOrTransfer(segments: Slice[TransientSegment.OneOrRemoteRef],
                              target: DBFile): Unit = {

    /**
     * Batch transfer segments to remote file. This defers transfer to the operating
     * system skipping the JVM heap.
     */
    def batchTransfer(sameFileRemotes: ListBuffer[TransientSegment.RemoteRef]): Unit =
      if (sameFileRemotes.nonEmpty)
        if (sameFileRemotes.size == 1) {
          val remote = sameFileRemotes.head
          remote.ref.segmentBlockCache.transfer(0, remote.segmentSizeIgnoreHeader, target)
          sameFileRemotes.clear()
        } else {
          val start = sameFileRemotes.head.ref.offset().start
          val end = sameFileRemotes.last.ref.offset().end
          sameFileRemotes.head.ref.segmentBlockCache.transferIgnoreOffset(start, end - start + 1, target)
          sameFileRemotes.clear()
        }

    /**
     * Writes bytes to target file and also tries to transfer Remote Refs which belongs to
     * same file to be transferred as a single IO operation i.e. batchableRemotes.
     */
    val pendingRemotes =
      segments.foldLeft(ListBuffer.empty[TransientSegment.RemoteRef]) {
        case (batchableRemotes, nextRemoteOrOne) =>
          nextRemoteOrOne match {
            case nextRemote: TransientSegment.RemoteRef =>
              if (batchableRemotes.isEmpty || (batchableRemotes.last.ref.path.getParent == nextRemote.ref.path.getParent && batchableRemotes.last.ref.offset().end + 1 == nextRemote.ref.offset().start)) {
                batchableRemotes += nextRemote
              } else {
                batchTransfer(batchableRemotes)
                batchableRemotes += nextRemote
              }

            case nextOne: TransientSegment.One =>
              batchTransfer(batchableRemotes)
              target.append(nextOne.bodyBytes)
              batchableRemotes
          }
      }

    //transfer any remaining remotes.
    batchTransfer(pendingRemotes)
  }

  private def segmentFile(path: Path,
                          mmap: MMAP.Segment,
                          segmentSize: Int,
                          byteTransferer: DBFile => Unit)(implicit segmentReadIO: SegmentReadIO,
                                                          fileSweeper: FileSweeper,
                                                          bufferCleaner: ByteBufferSweeperActor,
                                                          forceSaveApplier: ForceSaveApplier): DBFile =
    mmap match {
      case MMAP.On(deleteAfterClean, forceSave) => //if both read and writes are mmaped. Keep the file open.
        DBFile.mmapWriteAndReadTransfer(
          path = path,
          fileOpenIOStrategy = segmentReadIO.fileOpenIO,
          autoClose = true,
          deleteAfterClean = deleteAfterClean,
          forceSave = forceSave,
          bufferSize = segmentSize,
          transfer = byteTransferer
        )

      case MMAP.ReadOnly(deleteAfterClean) =>
        val channelWrite =
          DBFile.channelWrite(
            path = path,
            fileOpenIOStrategy = segmentReadIO.fileOpenIO,
            autoClose = true,
            forceSave = ForceSave.Off
          )

        try
          byteTransferer(channelWrite)
        catch {
          case throwable: Throwable =>
            logger.error(s"Failed to write $mmap file with applier. Closing file: $path", throwable)
            channelWrite.close()
            throw throwable
        }

        channelWrite.close()

        DBFile.mmapRead(
          path = channelWrite.path,
          fileOpenIOStrategy = segmentReadIO.fileOpenIO,
          autoClose = true,
          deleteAfterClean = deleteAfterClean
        )

      case _: MMAP.Off =>
        val channelWrite =
          DBFile.channelWrite(
            path = path,
            fileOpenIOStrategy = segmentReadIO.fileOpenIO,
            autoClose = true,
            forceSave = ForceSave.Off
          )

        try
          byteTransferer(channelWrite)
        catch {
          case throwable: Throwable =>
            logger.error(s"Failed to write $mmap file with applier. Closing file: $path", throwable)
            channelWrite.close()
            throw throwable
        }

        channelWrite.close()

        DBFile.channelRead(
          path = channelWrite.path,
          fileOpenIOStrategy = segmentReadIO.fileOpenIO,
          autoClose = true
        )

      //another case if mmapReads is false, write bytes in mmaped mode and then close and re-open for read. Currently not inuse.
      //    else if (mmap.mmapWrites && !mmap.mmapReads) {
      //      val file =
      //        DBFile.mmapWriteAndRead(
      //          path = path,
      //          autoClose = true,
      //          ioStrategy = SegmentIO.segmentBlockIO(IOAction.OpenResource),
      //          blockCacheFileId = BlockCacheFileIDGenerator.nextID,
      //          bytes = segmentBytes
      //        )
      //
      //      //close immediately to force flush the bytes to disk. Having mmapWrites == true and mmapReads == false,
      //      //is probably not the most efficient and should not be used.
      //      file.close()
      //      DBFile.channelRead(
      //        path = file.path,
      //        ioStrategy = SegmentIO.segmentBlockIO(IOAction.OpenResource),
      //        blockCacheFileId = BlockCacheFileIDGenerator.nextID,
      //        autoClose = true
      //      )
      //    }
    }
}
