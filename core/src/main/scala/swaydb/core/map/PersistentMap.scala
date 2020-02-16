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
 */

package swaydb.core.map

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Map.ExceptionHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.actor.FileSweeper
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.Effect._
import swaydb.core.io.file.{DBFile, Effect}
import swaydb.core.map.serializer.{MapCodec, MapEntryReader, MapEntryWriter}
import swaydb.core.util.Extension
import swaydb.core.util.skiplist.{SkipListConcurrent, SkipList}
import swaydb.data.config.IOStrategy
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec

private[map] object PersistentMap extends LazyLogging {

  private[map] def apply[OK, OV, K <: OK, V <: OV](folder: Path,
                                                   mmap: Boolean,
                                                   flushOnOverflow: Boolean,
                                                   fileSize: Long,
                                                   dropCorruptedTailEntries: Boolean,
                                                   nullKey: OK,
                                                   nullValue: OV)(implicit keyOrder: KeyOrder[K],
                                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                                  functionStore: FunctionStore,
                                                                  fileSweeper: FileSweeper,
                                                                  reader: MapEntryReader[MapEntry[K, V]],
                                                                  writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                  skipListMerger: SkipListMerger[OK, OV, K, V]): RecoveryResult[PersistentMap[OK, OV, K, V]] = {
    Effect.createDirectoryIfAbsent(folder)
    val skipList: SkipListConcurrent[OK, OV, K, V] = SkipList.concurrent[OK, OV, K, V](nullKey, nullValue)(keyOrder)
    val (fileRecoveryResult, hasRange) = recover(folder, mmap, fileSize, skipList, dropCorruptedTailEntries)

    RecoveryResult(
      item = new PersistentMap[OK, OV, K, V](
        path = folder,
        mmap = mmap,
        fileSize = fileSize,
        flushOnOverflow = flushOnOverflow,
        skipList = skipList,
        currentFile = fileRecoveryResult.item,
        hasRangeInitial = hasRange
      ),
      result = fileRecoveryResult.result
    )
  }

  private[map] def apply[OK, OV, K <: OK, V <: OV](folder: Path,
                                                   mmap: Boolean,
                                                   flushOnOverflow: Boolean,
                                                   fileSize: Long,
                                                   nullKey: OK,
                                                   nullValue: OV)(implicit keyOrder: KeyOrder[K],
                                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                                  fileSweeper: FileSweeper,
                                                                  functionStore: FunctionStore,
                                                                  writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                  skipListMerger: SkipListMerger[OK, OV, K, V]): PersistentMap[OK, OV, K, V] = {
    Effect.createDirectoryIfAbsent(folder)
    val skipList: SkipListConcurrent[OK, OV, K, V] = SkipList.concurrent[OK, OV, K, V](nullKey, nullValue)(keyOrder)
    val file = firstFile(folder, mmap, fileSize)
    new PersistentMap[OK, OV, K, V](
      path = folder,
      mmap = mmap,
      fileSize = fileSize,
      flushOnOverflow = flushOnOverflow,
      currentFile = file,
      skipList = skipList,
      hasRangeInitial = false
    )
  }

  private[map] def firstFile(folder: Path,
                             memoryMapped: Boolean,
                             fileSize: Long)(implicit fileSweeper: FileSweeper): DBFile =
    if (memoryMapped)
      DBFile.mmapInit(folder.resolve(0.toLogFileId), IOStrategy.SynchronisedIO(true), fileSize, autoClose = false, blockCacheFileId = 0)(fileSweeper, None)
    else
      DBFile.channelWrite(folder.resolve(0.toLogFileId), IOStrategy.SynchronisedIO(true), autoClose = false, blockCacheFileId = 0)(fileSweeper, None)

  private[map] def recover[OK, OV, K <: OK, V <: OV](folder: Path,
                                                     mmap: Boolean,
                                                     fileSize: Long,
                                                     skipList: SkipListConcurrent[OK, OV, K, V],
                                                     dropCorruptedTailEntries: Boolean)(implicit writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                        mapReader: MapEntryReader[MapEntry[K, V]],
                                                                                        skipListMerger: SkipListMerger[OK, OV, K, V],
                                                                                        keyOrder: KeyOrder[K],
                                                                                        fileSweeper: FileSweeper,
                                                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                                                        functionStore: FunctionStore): (RecoveryResult[DBFile], Boolean) = {
    //read all existing logs and populate skipList
    var hasRange: Boolean = false

    val recoveredFiles =
      folder.files(Extension.Log) mapRecover {
        path =>
          logger.info("{}: Recovering with dropCorruptedTailEntries = {}.", path, dropCorruptedTailEntries)
          val file = DBFile.channelRead(path, IOStrategy.SynchronisedIO(true), autoClose = false, blockCacheFileId = 0)(fileSweeper, None)
          val bytes = file.readAll
          logger.info("{}: Reading file.", path)
          val recovery = MapCodec.read[K, V](bytes, dropCorruptedTailEntries).get
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

    val file =
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
      }

    (
      //if there was a failure recovering any one of the files, return the recovery with the failure result.
      RecoveryResult(
        item = file,
        result = recoveredFiles.find(_.result.isLeft).map(_.result) getOrElse IO.unit
      ),
      hasRange
    )
  }

  /**
   * Creates nextFile by persisting the entries in skipList to the new file. This function does not
   * re-read oldFiles to apply the existing entries to skipList, skipList should already be populated with new entries.
   * This is to ensure that before deleting any of the old entries, a new file is successful created.
   *
   * oldFiles value deleted after the recovery is successful. In case of a failure an error message is logged.
   */
  private[map] def nextFile[OK, OV, K <: OK, V <: OV](oldFiles: Iterable[DBFile],
                                                      mmap: Boolean,
                                                      fileSize: Long,
                                                      skipList: SkipListConcurrent[OK, OV, K, V])(implicit writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                                  fileSweeper: FileSweeper): Option[DBFile] =
    oldFiles.lastOption map {
      lastFile =>
        val file = nextFile(lastFile, mmap, fileSize, skipList)
        //Next file successfully created. delete all old files without the last which gets deleted by nextFile.
        try {
          oldFiles.dropRight(1).foreach(_.delete())
          logger.info(s"Recovery successful")
          file
        } catch {
          case throwable: Throwable =>
            logger.error(
              "Recovery successful but failed to delete old log file. Delete this file manually and every other file except '{}' and reboot.",
              file.path,
              throwable
            )
            throw throwable
        }
    }

  private[map] def nextFile[OK, OV, K <: OK, V <: OV](currentFile: DBFile,
                                                      mmap: Boolean,
                                                      size: Long,
                                                      skipList: SkipListConcurrent[OK, OV, K, V])(implicit writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                                  fileSweeper: FileSweeper): DBFile = {

    val nextPath = currentFile.path.incrementFileId
    val bytes = MapCodec.write(skipList)
    val newFile =
      if (mmap)
        DBFile.mmapInit(path = nextPath, IOStrategy.SynchronisedIO(true), bufferSize = bytes.size + size, autoClose = false, blockCacheFileId = 0)(fileSweeper, None)
      else
        DBFile.channelWrite(nextPath, IOStrategy.SynchronisedIO(true), autoClose = false, blockCacheFileId = 0)(fileSweeper, None)

    newFile.append(bytes)
    currentFile.delete()
    newFile
  }
}

protected case class PersistentMap[OK, OV, K <: OK, V <: OV](path: Path,
                                                             mmap: Boolean,
                                                             fileSize: Long,
                                                             flushOnOverflow: Boolean,
                                                             skipList: SkipListConcurrent[OK, OV, K, V],
                                                             private var currentFile: DBFile,
                                                             private val hasRangeInitial: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                                   fileSweeper: FileSweeper,
                                                                                                   functionStore: FunctionStore,
                                                                                                   writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                                   skipListMerger: SkipListMerger[OK, OV, K, V]) extends Map[OK, OV, K, V] with LazyLogging {

  // actualSize of the file can be different to fileSize when the entry's size is > fileSize.
  // In this case a file is created just to fit those bytes (for that one entry).
  // For eg: if fileSize is 4.mb and the entry size is 5.mb, a new file is created with 5.mb for that one entry.
  // all the subsequent entries value added to 4.mb files, if it fits, or else the size is extended again.
  private var actualFileSize: Long = fileSize
  // does not account of flushed entries.
  private var bytesWritten: Long = 0
  var skipListKeyValuesMaxCount: Int = 0

  //_hasRange is not a case class input parameters because 2.11 throws compilation error 'values cannot be volatile'
  @volatile private var _hasRange: Boolean = hasRangeInitial

  override def hasRange: Boolean = _hasRange

  def currentFilePath =
    currentFile.path

  override def writeSync(mapEntry: MapEntry[K, V]): Boolean =
    synchronized(writeNoSync(mapEntry))

  /**
   * Before writing the Entry, check to ensure if the current [[MapEntry]] requires a merge write or direct write.
   *
   * Merge write should be used when
   * - The entry contains a [[swaydb.core.data.Memory.Range]] key-value.
   * - The entry contains a [[swaydb.core.data.Memory.Update]] Update key-value.
   * - The entry contains a [[swaydb.core.data.Memory.Remove]] with deadline key-value. Removes without deadlines do not require merging.
   *
   * Note: These check are not required for Appendix writes because Appendix entries current do not use
   * Range, Update or key-values with deadline.
   */
  @tailrec
  final def writeNoSync(entry: MapEntry[K, V]): Boolean = {
    val entryTotalByteSize = entry.totalByteSize
    if ((bytesWritten + entryTotalByteSize) <= actualFileSize) {
      currentFile.append(MapCodec.write(entry))
      //if this main contains range then use skipListMerge.
      if (entry.hasRange) {
        _hasRange = true //set hasRange to true before inserting so that reads start looking for floor key-values as the inserts are occurring.
        skipListMerger.insert(entry, skipList)
      } else if (entry.hasUpdate || entry.hasRemoveDeadline || _hasRange) {
        skipListMerger.insert(entry, skipList)
      } else {
        entry applyTo skipList
      }
      skipListKeyValuesMaxCount += entry.entriesCount
      bytesWritten += entryTotalByteSize
      //          println("bytesWritten: " + bytesWritten)
      true
    }
    //flushOnOverflow is executed if the current file is empty, even if flushOnOverflow = false.
    else if (!flushOnOverflow && bytesWritten != 0) {
      false
    } else {
      val nextFilesSize = entryTotalByteSize.toLong max fileSize
      try {
        val newFile = PersistentMap.nextFile(currentFile, mmap, nextFilesSize, skipList)
        currentFile = newFile
        actualFileSize = nextFilesSize
        bytesWritten = 0
      } catch {
        case exception: Exception =>
          logger.error("{}: Failed to replace with new file", currentFile.path, exception)
          throw new Exception("Fatal exception", exception)
      }
      writeNoSync(entry)
    }
  }

  override def close(): Unit =
    currentFile.close()

  override def exists: Boolean =
    currentFile.existsOnDisk

  override def delete: Unit = {
    currentFile.delete()
    Effect.delete(path)
    skipList.clear()
  }

  override def pathOption: Option[Path] =
    Some(path)

  override def fileId: Long =
    path.fileId._1
}
