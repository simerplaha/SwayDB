/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.map

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Map.ErrorHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.IOEffect._
import swaydb.core.io.file.{DBFile, IOEffect}
import swaydb.core.map.serializer.{MapCodec, MapEntryReader, MapEntryWriter}
import swaydb.core.queue.{FileSweeper, MemorySweeper}
import swaydb.core.util.{Extension, SkipList}
import swaydb.data.config.IOStrategy
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.reflect.ClassTag

private[map] object PersistentMap extends LazyLogging {

  private[map] def apply[K, V: ClassTag](folder: Path,
                                         mmap: Boolean,
                                         flushOnOverflow: Boolean,
                                         fileSize: Long,
                                         initialWriteCount: Long,
                                         dropCorruptedTailEntries: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                            timeOrder: TimeOrder[Slice[Byte]],
                                                                            functionStore: FunctionStore,
                                                                            fileSweeper: FileSweeper,
                                                                            memorySweeper: MemorySweeper,
                                                                            reader: MapEntryReader[MapEntry[K, V]],
                                                                            writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                            skipListMerger: SkipListMerger[K, V]): IO[swaydb.Error.Map, RecoveryResult[PersistentMap[K, V]]] = {
    IOEffect.createDirectoryIfAbsent(folder)
    val skipList: SkipList.Concurrent[K, V] = SkipList.concurrent[K, V]()(keyOrder)

    recover(folder, mmap, fileSize, skipList, dropCorruptedTailEntries) map {
      case (fileRecoveryResult, hasRange) =>
        RecoveryResult(
          item = new PersistentMap[K, V](
            path = folder,
            mmap = mmap,
            fileSize = fileSize,
            flushOnOverflow = flushOnOverflow,
            initialWriteCount = initialWriteCount,
            currentFile = fileRecoveryResult.item,
            hasRangeInitial = hasRange
          )(skipList),
          result = fileRecoveryResult.result
        )
    }
  }

  private[map] def apply[K, V: ClassTag](folder: Path,
                                         mmap: Boolean,
                                         flushOnOverflow: Boolean,
                                         fileSize: Long,
                                         initialWriteCount: Long)(implicit keyOrder: KeyOrder[K],
                                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                                  fileSweeper: FileSweeper,
                                                                  memorySweeper: MemorySweeper,
                                                                  functionStore: FunctionStore,
                                                                  reader: MapEntryReader[MapEntry[K, V]],
                                                                  writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                  skipListMerger: SkipListMerger[K, V]): IO[swaydb.Error.Map, PersistentMap[K, V]] = {
    IOEffect.createDirectoryIfAbsent(folder)
    val skipList: SkipList.Concurrent[K, V] = SkipList.concurrent[K, V]()(keyOrder)

    firstFile(folder, mmap, fileSize) map {
      file =>
        new PersistentMap[K, V](
          path = folder,
          mmap = mmap,
          fileSize = fileSize,
          flushOnOverflow = flushOnOverflow,
          initialWriteCount = initialWriteCount,
          currentFile = file,
          hasRangeInitial = false
        )(skipList)
    }
  }

  private[map] def firstFile(folder: Path,
                             memoryMapped: Boolean,
                             fileSize: Long)(implicit fileSweeper: FileSweeper,
                                             memorySweeper: MemorySweeper): IO[swaydb.Error.Map, DBFile] =
    if (memoryMapped)
      DBFile.mmapInit(folder.resolve(0.toLogFileId), IOStrategy.SynchronisedIO(true), fileSize, autoClose = false)(fileSweeper, memorySweeper, None)
    else
      DBFile.channelWrite(folder.resolve(0.toLogFileId), IOStrategy.SynchronisedIO(true), autoClose = false)(fileSweeper, memorySweeper, None)

  private[map] def recover[K, V](folder: Path,
                                 mmap: Boolean,
                                 fileSize: Long,
                                 skipList: SkipList.Concurrent[K, V],
                                 dropCorruptedTailEntries: Boolean)(implicit writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                    mapReader: MapEntryReader[MapEntry[K, V]],
                                                                    skipListMerger: SkipListMerger[K, V],
                                                                    keyOrder: KeyOrder[K],
                                                                    fileSweeper: FileSweeper,
                                                                    memorySweeper: MemorySweeper,
                                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                                    functionStore: FunctionStore): IO[swaydb.Error.Map, (RecoveryResult[DBFile], Boolean)] = {
    //read all existing logs and populate skipList
    var hasRange: Boolean = false
    folder.files(Extension.Log) mapIO {
      path =>
        logger.info("{}: Recovering with dropCorruptedTailEntries = {}.", path, dropCorruptedTailEntries)
        DBFile.channelRead(path, IOStrategy.SynchronisedIO(true), autoClose = false)(fileSweeper, memorySweeper, None) flatMap {
          file =>
            file.readAll flatMap {
              bytes =>
                logger.info("{}: Reading file.", path)
                MapCodec.read[K, V](bytes, dropCorruptedTailEntries) map {
                  recovery =>
                    logger.info("{}: Recovered! Populating in-memory map with recovered key-values.", path)
                    val entriesRecovered =
                      recovery.item.foldLeft(0) {
                        case (size, entry) =>
                          //when populating skipList do the same checks a PersistentMap does when writing key-values to the skipList.
                          //Use the merger to write key-values to skipList if the there a range, update or remove(with deadline).
                          //else simply write the key-values to the skipList. This logic should be abstracted out to a common function.
                          //See MapSpec for tests.
                          if (entry.hasRange) {
                            skipListMerger.insert(entry, skipList)
                            hasRange = true
                          } else if (hasRange || entry.hasUpdate || entry.hasRemoveDeadline) {
                            skipListMerger.insert(entry, skipList)
                          } else {
                            entry applyTo skipList
                          }
                          size + entry.entriesCount
                      }
                    logger.info(s"{}: Recovered {} ${if (entriesRecovered > 1) "entries" else "entry"}.", path, entriesRecovered)
                    RecoveryResult(file, recovery.result)
                }
            }
        }
    } flatMap {
      recoveredFiles =>
        nextFile(
          oldFiles = recoveredFiles.map(_.item),
          mmap = mmap,
          fileSize = fileSize,
          skipList = skipList
        ) getOrElse {
          firstFile(
            folder = folder,
            memoryMapped = mmap,
            fileSize = fileSize
          )
        } map {
          file =>
            (
              //if there was a failure recovering any one of the files, return the recovery with the failure result.
              RecoveryResult(
                item = file,
                result = recoveredFiles.find(_.result.isFailure).map(_.result) getOrElse IO.unit
              ),
              hasRange
            )
        }
    }
  }

  /**
   * Creates nextFile by persisting the entries in skipList to the new file. This function does not
   * re-read oldFiles to apply the existing entries to skipList, skipList should already be populated with new entries.
   * This is to ensure that before deleting any of the old entries, a new file is successful created.
   *
   * oldFiles value deleted after the recovery is successful. In case of a failure an error message is logged.
   */
  private[map] def nextFile[K, V](oldFiles: Iterable[DBFile],
                                  mmap: Boolean,
                                  fileSize: Long,
                                  skipList: SkipList.Concurrent[K, V])(implicit reader: MapEntryReader[MapEntry[K, V]],
                                                                       writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                       fileSweeper: FileSweeper,
                                                                       memorySweeper: MemorySweeper): Option[IO[swaydb.Error.Map, DBFile]] =
    oldFiles.lastOption map {
      lastFile =>
        nextFile(lastFile, mmap, fileSize, skipList) flatMap {
          nextFile =>
            //Next file successfully created. delete all old files without the last which gets deleted by nextFile.
            oldFiles.dropRight(1).foreachIO(_.delete()) match {
              case Some(failure) =>
                logger.error(
                  "Recovery successful but failed to delete old log file. Delete this file manually and every other file except '{}' and reboot.",
                  nextFile.path,
                  failure.exception
                )
                IO.Failure(failure.error)

              case None =>
                logger.info(s"Recovery successful")
                IO.Success(nextFile)
            }
        }
    }

  private[map] def nextFile[K, V](currentFile: DBFile,
                                  mmap: Boolean,
                                  size: Long,
                                  skipList: SkipList.Concurrent[K, V])(implicit writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                       mapReader: MapEntryReader[MapEntry[K, V]],
                                                                       fileSweeper: FileSweeper,
                                                                       memorySweeper: MemorySweeper): IO[swaydb.Error.Map, DBFile] =
    currentFile.path.incrementFileId flatMap {
      nextPath =>
        val bytes = MapCodec.write(skipList)
        val newFile =
          if (mmap)
            DBFile.mmapInit(path = nextPath, IOStrategy.SynchronisedIO(true), bufferSize = bytes.size + size, autoClose = false)(fileSweeper, memorySweeper, None)
          else
            DBFile.channelWrite(nextPath, IOStrategy.SynchronisedIO(true), autoClose = false)(fileSweeper, memorySweeper, None)

        newFile flatMap {
          newFile =>
            newFile.append(bytes) flatMap {
              _ =>
                currentFile.delete() flatMap {
                  _ =>
                    IO.Success(newFile)
                }
            }
        }
    }
}

private[map] case class PersistentMap[K, V: ClassTag](path: Path,
                                                      mmap: Boolean,
                                                      fileSize: Long,
                                                      flushOnOverflow: Boolean,
                                                      initialWriteCount: Long,
                                                      private var currentFile: DBFile,
                                                      private val hasRangeInitial: Boolean)(val skipList: SkipList.Concurrent[K, V])(implicit keyOrder: KeyOrder[K],
                                                                                                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                     fileSweeper: FileSweeper,
                                                                                                                                     memorySweeper: MemorySweeper,
                                                                                                                                     functionStore: FunctionStore,
                                                                                                                                     reader: MapEntryReader[MapEntry[K, V]],
                                                                                                                                     writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                                                                     skipListMerger: SkipListMerger[K, V]) extends Map[K, V] with LazyLogging {

  // actualSize of the file can be different to fileSize when the entry's size is > fileSize.
  // In this case a file is created just to fit those bytes (for that one entry).
  // For eg: if fileSize is 4.mb and the entry size is 5.mb, a new file is created with 5.mb for that one entry.
  // all the subsequent entries value added to 4.mb files, if it fits, or else the size is extended again.
  private var actualFileSize: Long = fileSize
  // does not account of flushed entries.
  private var bytesWritten: Long = 0

  //_hasRange is not a case class input parameters because 2.11 throws compilation error 'values cannot be volatile'
  @volatile private var _hasRange: Boolean = hasRangeInitial

  @volatile private var _writeCount: Long = initialWriteCount

  override def hasRange: Boolean = _hasRange

  private val stateIDLock = new Object()

  def currentFilePath =
    currentFile.path

  def writeCountStateId: Long =
    stateIDLock.synchronized {
      _writeCount
    }

  def incrementWriteCountStateId: Long =
    stateIDLock.synchronized {
      _writeCount += 1
      _writeCount
    }

  override def write(mapEntry: MapEntry[K, V]): IO[swaydb.Error.Map, Boolean] =
    stateIDLock.synchronized {
      persist(mapEntry) onSuccessSideEffect {
        _ =>
          _writeCount += 1
      }
    }

  /**
   * Before writing the Entry, check to ensure if the current [[MapEntry]] requires a merge write or direct write.
   *
   * Merge write should be used when
   * - The entry contains a [[Memory.Range]] key-value.
   * - The entry contains a [[Memory.Update]] Update key-value.
   * - The entry contains a [[Memory.Remove]] with deadline key-value. Removes without deadlines do not require merging.
   *
   * Note: These check are not required for Appendix writes because Appendix entries current do not use
   * Range, Update or key-values with deadline.
   */
  @tailrec
  private def persist(entry: MapEntry[K, V]): IO[swaydb.Error.Map, Boolean] =
    if ((bytesWritten + entry.totalByteSize) <= actualFileSize)
      currentFile.append(MapCodec.write(entry)) map {
        _ =>
          //if this main contains range then use skipListMerge.
          if (entry.hasRange) {
            _hasRange = true //set hasRange to true before inserting so that reads start looking for floor key-values as the inserts are occurring.
            skipListMerger.insert(entry, skipList)
          } else if (entry.hasUpdate || entry.hasRemoveDeadline || _hasRange) {
            skipListMerger.insert(entry, skipList)
          } else {
            entry applyTo skipList
          }
          bytesWritten += entry.totalByteSize
          //          println("bytesWritten: " + bytesWritten)
          true
      }
    //flushOnOverflow is executed if the current file is empty, even if flushOnOverflow = false.
    else if (!flushOnOverflow && bytesWritten != 0)
      IO.`false`
    else {
      val nextFilesSize = entry.totalByteSize.toLong max fileSize
      PersistentMap.nextFile(currentFile, mmap, nextFilesSize, skipList) match {
        case IO.Success(newFile) =>
          currentFile = newFile
          actualFileSize = nextFilesSize
          bytesWritten = 0
          persist(entry)

        case IO.Failure(error) =>
          logger.error("{}: Failed to replace with new file", currentFile.path, error.exception)
          IO.Failure(error)
      }
    }

  override def close(): IO[swaydb.Error.Map, Unit] =
    currentFile.close

  override def exists: Boolean =
    currentFile.existsOnDisk

  override def delete: IO[swaydb.Error.Map, Unit] =
    currentFile.delete() flatMap {
      _ =>
        IOEffect.delete(path) map {
          _ =>
            skipList.clear()
        }
    }

  override def pathOption: Option[Path] =
    Some(path)

  override def fileId: IO[swaydb.Error.Map, Long] =
    path.fileId.map(_._1)
}
