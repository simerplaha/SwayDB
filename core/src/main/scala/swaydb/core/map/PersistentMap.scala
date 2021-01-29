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

package swaydb.core.map

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Map.ExceptionHandler
import swaydb.IO
import swaydb.core.io.file.Effect._
import swaydb.core.io.file.{DBFile, Effect, ForceSaveApplier}
import swaydb.core.map.serializer.{MapCodec, MapEntryReader, MapEntryWriter}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{ByteBufferSweeper, FileSweeper}
import swaydb.core.util.Extension
import swaydb.data.config.{IOStrategy, MMAP}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.slice.SliceIOImplicits._

import java.nio.file.Path
import scala.annotation.tailrec

private[map] object PersistentMap extends LazyLogging {

  private[map] def apply[K, V, C <: MapCache[K, V]](folder: Path,
                                                    mmap: MMAP.Map,
                                                    flushOnOverflow: Boolean,
                                                    fileSize: Long,
                                                    dropCorruptedTailEntries: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                                       fileSweeper: FileSweeper,
                                                                                       bufferCleaner: ByteBufferSweeperActor,
                                                                                       reader: MapEntryReader[MapEntry[K, V]],
                                                                                       writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                       cacheBuilder: MapCacheBuilder[C],
                                                                                       forceSaveApplier: ForceSaveApplier): RecoveryResult[PersistentMap[K, V, C]] = {
    Effect.createDirectoryIfAbsent(folder)

    val cache = cacheBuilder.create()

    val fileRecoveryResult =
      recover[K, V, C](
        folder = folder,
        mmap = mmap,
        fileSize = fileSize,
        cache = cache,
        dropCorruptedTailEntries = dropCorruptedTailEntries
      )

    val map =
      new PersistentMap[K, V, C](
        path = folder,
        mmap = mmap,
        fileSize = fileSize,
        flushOnOverflow = flushOnOverflow,
        cache = cache,
        currentFile = fileRecoveryResult.item
      )

    RecoveryResult(
      item = map,
      result = fileRecoveryResult.result
    )
  }

  private[map] def apply[K, V, C <: MapCache[K, V]](folder: Path,
                                                    mmap: MMAP.Map,
                                                    flushOnOverflow: Boolean,
                                                    fileSize: Long)(implicit keyOrder: KeyOrder[K],
                                                                    fileSweeper: FileSweeper,
                                                                    bufferCleaner: ByteBufferSweeperActor,
                                                                    cacheBuilder: MapCacheBuilder[C],
                                                                    writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                    forceSaveApplier: ForceSaveApplier): PersistentMap[K, V, C] = {
    Effect.createDirectoryIfAbsent(folder)

    val file =
      firstFile(
        folder = folder,
        memoryMapped = mmap,
        fileSize = fileSize
      )

    new PersistentMap[K, V, C](
      path = folder,
      mmap = mmap,
      fileSize = fileSize,
      flushOnOverflow = flushOnOverflow,
      currentFile = file,
      cache = cacheBuilder.create()
    )
  }

  private[map] def firstFile(folder: Path,
                             memoryMapped: MMAP.Map,
                             fileSize: Long)(implicit fileSweeper: FileSweeper,
                                             bufferCleaner: ByteBufferSweeperActor,
                                             forceSaveApplier: ForceSaveApplier): DBFile =
    memoryMapped match {
      case MMAP.On(deleteAfterClean, forceSave) =>
        DBFile.mmapInit(
          path = folder.resolve(0.toLogFileId),
          fileOpenIOStrategy = IOStrategy.SynchronisedIO(true),
          bufferSize = fileSize,
          autoClose = false,
          deleteAfterClean = deleteAfterClean,
          forceSave = forceSave
        )

      case MMAP.Off(forceSave) =>
        DBFile.channelWrite(
          path = folder.resolve(0.toLogFileId),
          fileOpenIOStrategy = IOStrategy.SynchronisedIO(true),
          autoClose = false,
          forceSave = forceSave
        )
    }


  private[map] def recover[K, V, C <: MapCache[K, V]](folder: Path,
                                                      mmap: MMAP.Map,
                                                      fileSize: Long,
                                                      cache: C,
                                                      dropCorruptedTailEntries: Boolean)(implicit writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                         mapReader: MapEntryReader[MapEntry[K, V]],
                                                                                         fileSweeper: FileSweeper,
                                                                                         bufferCleaner: ByteBufferSweeperActor,
                                                                                         forceSaveApplier: ForceSaveApplier): RecoveryResult[DBFile] = {

    val files = folder.files(Extension.Log)

    val recoveredFiles =
      files mapRecover {
        path =>
          logger.info(s"$path: Recovering key-values with dropCorruptedTailEntries set as $dropCorruptedTailEntries.")
          val file = DBFile.channelRead(path, IOStrategy.SynchronisedIO(true), autoClose = false)
          val bytes = file.readAll
          val recovery = MapCodec.read[K, V](bytes, dropCorruptedTailEntries).get

          val entriesCount = recovery.item.map(_.entriesCount).getOrElse(0)
          val entriesOrEntry = if (entriesCount == 0 || entriesCount > 1) "entries" else "entry"

          logger.info(s"$path: Indexing $entriesCount $entriesOrEntry.")

          //when populating skipList do the same checks a PersistentMap does when writing key-values to the skipList.
          //Use the merger to write key-values to skipList if the there a range, update or remove(with deadline).{if (entriesCount == 0 || entriesCount > 1) "entries" else "entry"
          //else simply write the key-values to the skipList. This logic should be abstracted out to a common function.
          //See MapSpec for tests.
          recovery.item foreach cache.writeNonAtomic

          logger.debug(s"$path: Indexed!")
          RecoveryResult(file, recovery.result)
      }

    val file =
      nextFile[K, V, C](
        oldFiles = recoveredFiles.map(_.item),
        mmap = mmap,
        fileSize = fileSize,
        cache = cache
      ) getOrElse {
        firstFile(
          folder = folder,
          memoryMapped = mmap,
          fileSize = fileSize
        )
      }

    //if there was a failure recovering any one of the files, return the recovery with the failure result.
    RecoveryResult(
      item = file,
      result = recoveredFiles.find(_.result.isLeft).map(_.result) getOrElse IO.unit
    )
  }

  /**
   * Creates nextFile by persisting the entries in skipList to the new file. This function does not
   * re-read oldFiles to apply the existing entries to skipList, skipList should already be populated with new entries.
   * This is to ensure that before deleting any of the old entries, a new file is successful created.
   *
   * oldFiles value deleted after the recovery is successful. In case of a failure an error message is logged.
   */
  private[map] def nextFile[K, V, C <: MapCache[K, V]](oldFiles: Slice[DBFile],
                                                       mmap: MMAP.Map,
                                                       fileSize: Long,
                                                       cache: C)(implicit writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                 fileSweeper: FileSweeper,
                                                                 bufferCleaner: ByteBufferSweeperActor,
                                                                 forceSaveApplier: ForceSaveApplier): Option[DBFile] =
    oldFiles.lastOption map {
      lastFile =>
        val file =
          nextFile[K, V, C](
            currentFile = lastFile,
            mmap = mmap,
            size = fileSize,
            cache = cache
          )
        //Next file successfully created. delete all old files without the last which gets deleted by nextFile.
        try {
          oldFiles.dropRight(1).foreach(_.delete())
          logger.debug(s"Recovery successful")
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

  private[map] def nextFile[K, V, C <: MapCache[K, V]](currentFile: DBFile,
                                                       mmap: MMAP.Map,
                                                       size: Long,
                                                       cache: C)(implicit writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                 fileSweeper: FileSweeper,
                                                                 bufferCleaner: ByteBufferSweeperActor,
                                                                 forceSaveApplier: ForceSaveApplier): DBFile = {

    val nextPath = currentFile.path.incrementFileId
    val bytes = MapCodec.write(cache.iterator)

    val newFile =
      mmap match {
        case MMAP.On(deleteAfterClean, forceSave) =>
          DBFile.mmapInit(
            path = nextPath,
            fileOpenIOStrategy = IOStrategy.SynchronisedIO(true),
            bufferSize = bytes.size + size,
            autoClose = false,
            deleteAfterClean = deleteAfterClean,
            forceSave = forceSave
          )

        case MMAP.Off(forceSave) =>
          DBFile.channelWrite(
            path = nextPath,
            fileOpenIOStrategy = IOStrategy.SynchronisedIO(true),
            autoClose = false,
            forceSave = forceSave
          )
      }

    newFile.append(bytes)
    currentFile.delete()
    newFile
  }
}

protected case class PersistentMap[K, V, C <: MapCache[K, V]](path: Path,
                                                              mmap: MMAP.Map,
                                                              fileSize: Long,
                                                              flushOnOverflow: Boolean,
                                                              cache: C,
                                                              private var currentFile: DBFile)(implicit val fileSweeper: FileSweeper,
                                                                                               val bufferCleaner: ByteBufferSweeperActor,
                                                                                               val writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                                               val forceSaveApplier: ForceSaveApplier) extends Map[K, V, C] with LazyLogging {

  // actualSize of the file can be different to fileSize when the entry's size is > fileSize.
  // In this case a file is created just to fit those bytes (for that one entry).
  // For eg: if fileSize is 4.mb and the entry size is 5.mb, a new file is created with 5.mb for that one entry.
  // all the subsequent entries value added to 4.mb files, if it fits, or else the size is extended again.
  private var actualFileSize: Long = fileSize
  // does not account of flushed entries.
  private var bytesWritten: Long = 0
  //minimum of writes we should see before logging out warning messages that the fileSize is too small.
  private val minimumNumberOfWritesAfterFlush = 10
  //maintains allowed number of writes that can occurred after the last flush before warning.
  private var allowedPostFlushEntriesBeforeWarn: Long = 0

  override val uniqueFileNumber: Long =
    Map.uniqueFileNumberGenerator.next

  def typeName = productPrefix

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
      cache.writeAtomic(entry)
      bytesWritten += entryTotalByteSize
      allowedPostFlushEntriesBeforeWarn -= 1 //decrement the number on successful write
      true
    } else if (!flushOnOverflow && bytesWritten != 0) {
      //flushOnOverflow is executed if the current file is empty, even if flushOnOverflow = false.
      false
    } else {
      val nextFilesSize = entryTotalByteSize.toLong max fileSize

      try {
        val newFile =
          PersistentMap.nextFile[K, V, C](
            currentFile = currentFile,
            mmap = mmap,
            size = nextFilesSize,
            cache = cache
          )

        /**
         * If the fileSize is too small like 1.byte it will result in too many flushes. Log a warn message to increase
         * file size.
         */
        if (allowedPostFlushEntriesBeforeWarn <= 0) //if it was in negative then restart the count
          allowedPostFlushEntriesBeforeWarn = minimumNumberOfWritesAfterFlush
        else if (allowedPostFlushEntriesBeforeWarn > 0) //If the count did not get negative then warn that the fileSize is too small.
          logger.warn(s"$typeName's file size of $fileSize.bytes is too small and would result in too many flushes. Please increase the default fileSize to at least ${newFile.fileSize}.bytes. Folder: $path.")

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

  override def delete: Unit =
    if (mmap.deleteAfterClean) {
      //if it's require deleteAfterClean then do not invoke delete directly
      //instead invoke close (which will also call ByteBufferCleaner for closing)
      // and then submit delete to ByteBufferCleaner actor.
      currentFile.close()
      bufferCleaner.actor send ByteBufferSweeper.Command.DeleteFolder(path, currentFile.path)
    } else {
      //else delete immediately.
      currentFile.delete()
      Effect.delete(path)
    }

  override def pathOption: Option[Path] =
    Some(path)

}
