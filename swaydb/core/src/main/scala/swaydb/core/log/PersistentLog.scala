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

package swaydb.core.log

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Log.ExceptionHandler
import swaydb.IO
import swaydb.config.MMAP
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.file.sweeper.bytebuffer.ByteBufferCommand
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.{CoreFile, ForceSaveApplier}
import swaydb.core.log.serialiser.{LogEntryReader, LogEntryParser, LogEntryWriter}
import swaydb.effect.Effect._
import swaydb.effect.{Effect, IOStrategy}
import swaydb.slice.Slice
import swaydb.SliceIOImplicits._
import swaydb.slice.order.KeyOrder
import swaydb.utils.Extension

import java.nio.file.Path
import scala.annotation.tailrec

private[core] object PersistentLog extends LazyLogging {

  def apply[K, V, C <: LogCache[K, V]](folder: Path,
                                       mmap: MMAP.Log,
                                       flushOnOverflow: Boolean,
                                       fileSize: Int,
                                       dropCorruptedTailEntries: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                          fileSweeper: FileSweeper,
                                                                          bufferCleaner: ByteBufferSweeperActor,
                                                                          reader: LogEntryReader[LogEntry[K, V]],
                                                                          writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                                          cacheBuilder: LogCacheBuilder[C],
                                                                          forceSaveApplier: ForceSaveApplier): LogRecoveryResult[PersistentLog[K, V, C]] = {
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

    val log =
      new PersistentLog[K, V, C](
        path = folder,
        mmap = mmap,
        fileSize = fileSize,
        flushOnOverflow = flushOnOverflow,
        cache = cache,
        currentFile = fileRecoveryResult.item
      )

    LogRecoveryResult(
      item = log,
      result = fileRecoveryResult.result
    )
  }

  def apply[K, V, C <: LogCache[K, V]](folder: Path,
                                       mmap: MMAP.Log,
                                       flushOnOverflow: Boolean,
                                       fileSize: Int)(implicit keyOrder: KeyOrder[K],
                                                      fileSweeper: FileSweeper,
                                                      bufferCleaner: ByteBufferSweeperActor,
                                                      cacheBuilder: LogCacheBuilder[C],
                                                      writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                      forceSaveApplier: ForceSaveApplier): PersistentLog[K, V, C] = {
    Effect.createDirectoryIfAbsent(folder)

    val file =
      firstFile(
        folder = folder,
        memoryMapped = mmap,
        fileSize = fileSize
      )

    new PersistentLog[K, V, C](
      path = folder,
      mmap = mmap,
      fileSize = fileSize,
      flushOnOverflow = flushOnOverflow,
      currentFile = file,
      cache = cacheBuilder.create()
    )
  }

  def firstFile(folder: Path,
                memoryMapped: MMAP.Log,
                fileSize: Int)(implicit fileSweeper: FileSweeper,
                               bufferCleaner: ByteBufferSweeperActor,
                               forceSaveApplier: ForceSaveApplier): CoreFile =
    memoryMapped match {
      case MMAP.On(deleteAfterClean, forceSave) =>
        CoreFile.mmapEmptyWriteableReadable(
          path = folder.resolve(0.toLogFileId),
          fileOpenIOStrategy = IOStrategy.SynchronisedIO(true),
          bufferSize = fileSize,
          autoClose = false,
          deleteAfterClean = deleteAfterClean,
          forceSave = forceSave
        )

      case MMAP.Off(forceSave) =>
        CoreFile.standardWritable(
          path = folder.resolve(0.toLogFileId),
          fileOpenIOStrategy = IOStrategy.SynchronisedIO(true),
          autoClose = false,
          forceSave = forceSave
        )
    }


  def recover[K, V, C <: LogCache[K, V]](folder: Path,
                                         mmap: MMAP.Log,
                                         fileSize: Int,
                                         cache: C,
                                         dropCorruptedTailEntries: Boolean)(implicit writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                                            logReader: LogEntryReader[LogEntry[K, V]],
                                                                            fileSweeper: FileSweeper,
                                                                            bufferCleaner: ByteBufferSweeperActor,
                                                                            forceSaveApplier: ForceSaveApplier): LogRecoveryResult[CoreFile] = {

    val files = folder.files(Extension.Log)

    val recoveredFiles =
      files mapRecover {
        path =>
          logger.info(s"$path: Recovering key-values with dropCorruptedTailEntries set as $dropCorruptedTailEntries.")
          val file = CoreFile.standardReadable(path, IOStrategy.SynchronisedIO(true), autoClose = false)
          val bytes = file.readAll()
          val recovery = LogEntryParser.read[K, V](bytes, dropCorruptedTailEntries).get

          val entriesCount = recovery.item.map(_.entriesCount).getOrElse(0)
          val entriesOrEntry = if (entriesCount == 0 || entriesCount > 1) "entries" else "entry"

          logger.info(s"$path: Indexing $entriesCount $entriesOrEntry.")

          //when populating skipList do the same checks a PersistentMap does when writing key-values to the skipList.
          //Use the merger to write key-values to skipList if the there a range, update or remove(with deadline).{if (entriesCount == 0 || entriesCount > 1) "entries" else "entry"
          //else simply write the key-values to the skipList. This logic should be abstracted out to a common function.
          //See LogSpec for tests.
          recovery.item foreach cache.writeNonAtomic

          logger.debug(s"$path: Indexed!")
          LogRecoveryResult(file, recovery.result)
      }

    val file =
      nextFile[K, V, C](
        oldFiles = recoveredFiles.mapToSlice(_.item),
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
    LogRecoveryResult(
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
  def nextFile[K, V, C <: LogCache[K, V]](oldFiles: Slice[CoreFile],
                                          mmap: MMAP.Log,
                                          fileSize: Int,
                                          cache: C)(implicit writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                    fileSweeper: FileSweeper,
                                                    bufferCleaner: ByteBufferSweeperActor,
                                                    forceSaveApplier: ForceSaveApplier): Option[CoreFile] =
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

  def nextFile[K, V, C <: LogCache[K, V]](currentFile: CoreFile,
                                          mmap: MMAP.Log,
                                          size: Int,
                                          cache: C)(implicit writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                    fileSweeper: FileSweeper,
                                                    bufferCleaner: ByteBufferSweeperActor,
                                                    forceSaveApplier: ForceSaveApplier): CoreFile = {

    val nextPath = currentFile.path.incrementFileId
    val bytes = LogEntryParser.write(cache.iterator)

    val newFile =
      mmap match {
        case MMAP.On(deleteAfterClean, forceSave) =>
          CoreFile.mmapEmptyWriteableReadable(
            path = nextPath,
            fileOpenIOStrategy = IOStrategy.SynchronisedIO(true),
            bufferSize = bytes.size + size,
            autoClose = false,
            deleteAfterClean = deleteAfterClean,
            forceSave = forceSave
          )

        case MMAP.Off(forceSave) =>
          CoreFile.standardWritable(
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

protected case class PersistentLog[K, V, C <: LogCache[K, V]] private(path: Path,
                                                                      mmap: MMAP.Log,
                                                                      fileSize: Int,
                                                                      flushOnOverflow: Boolean,
                                                                      cache: C,
                                                                      private var currentFile: CoreFile)(implicit val fileSweeper: FileSweeper,
                                                                                                         val bufferCleaner: ByteBufferSweeperActor,
                                                                                                         val writer: LogEntryWriter[LogEntry.Put[K, V]],
                                                                                                         val forceSaveApplier: ForceSaveApplier) extends Log[K, V, C] with LazyLogging {

  // actualSize of the file can be different to fileSize when the entry's size is > fileSize.
  // In this case a file is created just to fit those bytes (for that one entry).
  // For eg: if fileSize is 4.mb and the entry size is 5.mb, a new file is created with 5.mb for that one entry.
  // all the subsequent entries value added to 4.mb files, if it fits, or else the size is extended again.
  private var actualFileSize: Int = fileSize
  // does not account of flushed entries.
  private var bytesWritten: Long = 0
  //minimum of writes we should see before logging out warning messages that the fileSize is too small.
  private val minimumNumberOfWritesAfterFlush = 10
  //maintains allowed number of writes that can occurred after the last flush before warning.
  private var allowedPostFlushEntriesBeforeWarn: Long = 0

  override val uniqueFileNumber: Long =
    Log.uniqueFileNumberGenerator.nextId()

  def currentFilePath: Path =
    currentFile.path

  override def writeSync(logEntry: LogEntry[K, V]): Boolean =
    synchronized(writeNoSync(logEntry))

  /**
   * Before writing the Entry, check to ensure if the current [[LogEntry]] requires a merge write or direct write.
   *
   * Merge write should be used when
   *  - The entry contains a [[swaydb.core.segment.data.Memory.Range]] key-value.
   *  - The entry contains a [[swaydb.core.segment.data.Memory.Update]] Update key-value.
   *  - The entry contains a [[swaydb.core.segment.data.Memory.Remove]] with deadline key-value. Removes without deadlines do not require merging.
   *
   * Note: These check are not required for Appendix writes because Appendix entries current do not use
   * Range, Update or key-values with deadline.
   */
  @tailrec
  final def writeNoSync(entry: LogEntry[K, V]): Boolean = {
    val entryTotalByteSize = entry.totalByteSize
    if ((bytesWritten + entryTotalByteSize) <= actualFileSize) {
      currentFile.append(LogEntryParser.write(entry))
      //if this main contains range then use skipListMerge.
      cache.writeAtomic(entry)
      bytesWritten += entryTotalByteSize
      allowedPostFlushEntriesBeforeWarn -= 1 //decrement the number on successful write
      true
    } else if (!flushOnOverflow && bytesWritten != 0) {
      //flushOnOverflow is executed if the current file is empty, even if flushOnOverflow = false.
      false
    } else {
      val nextFilesSize = entryTotalByteSize max fileSize

      try {
        val newFile =
          PersistentLog.nextFile[K, V, C](
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
          logger.warn(s"${this.productPrefix}'s file size of $fileSize.bytes is too small and would result in too many flushes. Please increase the default fileSize to at least ${newFile.fileSize()}.bytes. Folder: $path.")

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

  override def exists(): Boolean =
    currentFile.existsOnDisk()

  override def delete(): Unit =
    if (mmap.deleteAfterClean) {
      //if it's require deleteAfterClean then do not invoke delete directly
      //instead invoke close (which will also call ByteBufferCleaner for closing)
      // and then submit delete to ByteBufferCleaner actor.
      currentFile.close()
      bufferCleaner.actor() send ByteBufferCommand.DeleteFolder(path, currentFile.path)
    } else {
      //else delete immediately.
      currentFile.delete()
      Effect.delete(path)
    }

  override def pathOption: Option[Path] =
    Some(path)

}
