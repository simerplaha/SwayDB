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

package swaydb.core.io.file

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.IO.ExceptionHandler
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{ByteBufferSweeper, FileSweeper, FileSweeperItem}
import swaydb.data.cache.Cache
import swaydb.data.config.ForceSave
import swaydb.data.slice.Slice
import swaydb.effect.{Effect, IOStrategy, Reserve}
import swaydb.{Error, IO}

import java.nio.file.Path
import scala.util.hashing.MurmurHash3

object DBFile extends LazyLogging {

  def fileCache(filePath: Path,
                memoryMapped: Boolean,
                deleteAfterClean: Boolean,
                fileOpenIOStrategy: IOStrategy.ThreadSafe,
                file: Option[DBFileType],
                autoClose: Boolean)(implicit fileSweeper: FileSweeper,
                                    bufferCleaner: ByteBufferSweeperActor,
                                    forceSaveApplier: ForceSaveApplier) = {

    //We need to create a single FileSweeperItem that can be
    //re-submitted to fileSweeper every time this file requires a close Actor request.
    //Creating new FileSweeper item for each close request would result in cleaner
    //code without null but would be expensive considering the number of objects created.
    var self: Cache[Error.IO, Unit, DBFileType] = null

    val closer: FileSweeperItem =
      new FileSweeperItem {
        override def path: Path = filePath

        override def delete(): Unit = {
          val message = s"Delete invoked on only closeable ${classOf[FileSweeperItem].getSimpleName}. Path: $path. autoClose = $autoClose. deleteAfterClean = $deleteAfterClean."
          val exception = new Exception(message)
          logger.error(message, exception)
          exception.printStackTrace()
        }

        override def close(): Unit =
          self.clearApply {
            case Some(file) =>
              IO(file.close())

            case None =>
              IO.unit
          }

        override def isOpen: Boolean =
          self.getIO().exists(_.exists(_.isOpen))
      }

    val cache =
      Cache.io[swaydb.Error.IO, Error.OpeningFile, Unit, DBFileType](
        //force the cache to be cacheOnAccess regardless of what's configured.
        //This is also needed because only a single thread can close or delete a
        //file using clearApply and stored cached is required otherwise this will lead
        //to many open FileChannels without reference which results in "too many files open" exception.
        strategy = fileOpenIOStrategy.forceCacheOnAccess,
        reserveError = Error.OpeningFile(filePath, Reserve.free(name = s"DBFile: $filePath. MemoryMapped: $memoryMapped")),
        initial = file
      ) {
        (_, _) =>
          logger.debug(s"{}: Opening closed file.", filePath)

          IO {
            val file =
              if (memoryMapped)
                MMAPFile.read(
                  path = filePath,
                  deleteAfterClean = deleteAfterClean
                )
              else
                StandardFile.read(path = filePath)

            if (autoClose)
              fileSweeper send FileSweeper.Command.CloseFileItem(closer)

            file
          }
      }

    self = cache

    if (autoClose && file.isDefined) fileSweeper send FileSweeper.Command.CloseFileItem(closer)
    cache
  }

  def standardWrite(path: Path,
                    fileOpenIOStrategy: IOStrategy.ThreadSafe,
                    autoClose: Boolean,
                    forceSave: ForceSave.StandardFiles)(implicit fileSweeper: FileSweeper,
                                                        bufferCleaner: ByteBufferSweeperActor,
                                                        forceSaveApplier: ForceSaveApplier): DBFile = {
    val file = StandardFile.write(path, forceSave)
    new DBFile(
      path = path,
      memoryMapped = false,
      autoClose = autoClose,
      deleteAfterClean = false,
      forceSaveConfig = forceSave,
      fileCache =
        fileCache(
          filePath = path,
          memoryMapped = false,
          deleteAfterClean = false,
          file = Some(file),
          fileOpenIOStrategy = fileOpenIOStrategy,
          autoClose = autoClose
        )
    )
  }

  def standardRead(path: Path,
                   fileOpenIOStrategy: IOStrategy.ThreadSafe,
                   autoClose: Boolean,
                   checkExists: Boolean = true)(implicit fileSweeper: FileSweeper,
                                                bufferCleaner: ByteBufferSweeperActor,
                                                forceSaveApplier: ForceSaveApplier): DBFile =
    if (checkExists && Effect.notExists(path))
      throw swaydb.Exception.NoSuchFile(path)
    else
      new DBFile(
        path = path,
        memoryMapped = false,
        autoClose = autoClose,
        deleteAfterClean = false,
        forceSaveConfig = ForceSave.Off,
        fileCache =
          fileCache(
            filePath = path,
            memoryMapped = false,
            deleteAfterClean = false,
            fileOpenIOStrategy = fileOpenIOStrategy,
            file = None,
            autoClose = autoClose
          )
      )

  def mmapWriteAndRead(path: Path,
                       fileOpenIOStrategy: IOStrategy.ThreadSafe,
                       autoClose: Boolean,
                       deleteAfterClean: Boolean,
                       forceSave: ForceSave.MMAPFiles,
                       bytes: Iterable[Slice[Byte]])(implicit fileSweeper: FileSweeper,
                                                     bufferCleaner: ByteBufferSweeperActor,
                                                     forceSaveApplier: ForceSaveApplier): DBFile = {
    val totalWritten =
      bytes.foldLeft(0) { //do not write bytes if the Slice has empty bytes.
        case (written, bytes) =>
          if (!bytes.isFull)
            throw swaydb.Exception.FailedToWriteAllBytes(0, bytes.size, bytes.size)
          else
            written + bytes.size
      }

    val file =
      mmapInit(
        path = path,
        fileOpenIOStrategy = fileOpenIOStrategy,
        bufferSize = totalWritten,
        autoClose = autoClose,
        forceSave = forceSave,
        deleteAfterClean = deleteAfterClean
      )

    file.append(bytes)
    file
  }

  def mmapWriteAndReadTransfer(path: Path,
                               fileOpenIOStrategy: IOStrategy.ThreadSafe,
                               autoClose: Boolean,
                               deleteAfterClean: Boolean,
                               forceSave: ForceSave.MMAPFiles,
                               bufferSize: Int,
                               transfer: DBFile => Unit)(implicit fileSweeper: FileSweeper,
                                                         bufferCleaner: ByteBufferSweeperActor,
                                                         forceSaveApplier: ForceSaveApplier): DBFile = {
    val file =
      mmapInit(
        path = path,
        fileOpenIOStrategy = fileOpenIOStrategy,
        bufferSize = bufferSize,
        autoClose = autoClose,
        forceSave = forceSave,
        deleteAfterClean = deleteAfterClean
      )

    try
      transfer(file)
    catch {
      case throwable: Throwable =>
        logger.error(s"Failed to write MMAP file with applier. Closing file: $path", throwable)
        file.close()
        throw throwable
    }

    file
  }

  def mmapWriteAndRead(path: Path,
                       fileOpenIOStrategy: IOStrategy.ThreadSafe,
                       autoClose: Boolean,
                       deleteAfterClean: Boolean,
                       forceSave: ForceSave.MMAPFiles,
                       bytes: Slice[Byte])(implicit fileSweeper: FileSweeper,
                                           bufferCleaner: ByteBufferSweeperActor,
                                           forceSaveApplier: ForceSaveApplier): DBFile =
  //do not write bytes if the Slice has empty bytes.
    if (!bytes.isFull) {
      throw swaydb.Exception.FailedToWriteAllBytes(0, bytes.size, bytes.size)
    } else {
      val file =
        mmapInit(
          path = path,
          fileOpenIOStrategy = fileOpenIOStrategy,
          bufferSize = bytes.size,
          autoClose = autoClose,
          forceSave = forceSave,
          deleteAfterClean = deleteAfterClean
        )

      file.append(bytes)
      file
    }

  def mmapRead(path: Path,
               fileOpenIOStrategy: IOStrategy.ThreadSafe,
               autoClose: Boolean,
               deleteAfterClean: Boolean,
               checkExists: Boolean = true)(implicit fileSweeper: FileSweeper,
                                            bufferCleaner: ByteBufferSweeperActor,
                                            forceSaveApplier: ForceSaveApplier): DBFile =
    if (checkExists && Effect.notExists(path)) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      new DBFile(
        path = path,
        memoryMapped = true,
        autoClose = autoClose,
        deleteAfterClean = deleteAfterClean,
        forceSaveConfig = ForceSave.Off,
        fileCache =
          fileCache(
            filePath = path,
            memoryMapped = true,
            deleteAfterClean = deleteAfterClean,
            fileOpenIOStrategy = fileOpenIOStrategy,
            file = None,
            autoClose = autoClose
          )
      )
    }

  def mmapInit(path: Path,
               fileOpenIOStrategy: IOStrategy.ThreadSafe,
               bufferSize: Int,
               autoClose: Boolean,
               deleteAfterClean: Boolean,
               forceSave: ForceSave.MMAPFiles)(implicit fileSweeper: FileSweeper,
                                               bufferCleaner: ByteBufferSweeperActor,
                                               forceSaveApplier: ForceSaveApplier): DBFile = {
    val file =
      MMAPFile.write(
        path = path,
        bufferSize = bufferSize,
        deleteAfterClean = deleteAfterClean,
        forceSave = forceSave
      )

    new DBFile(
      path = path,
      memoryMapped = true,
      autoClose = autoClose,
      deleteAfterClean = deleteAfterClean,
      forceSaveConfig = forceSave,
      fileCache =
        fileCache(
          filePath = path,
          memoryMapped = true,
          deleteAfterClean = deleteAfterClean,
          fileOpenIOStrategy = fileOpenIOStrategy,
          file = Some(file),
          autoClose = autoClose
        )
    )
  }
}
/**
 * Wrapper class for different file types of [[DBFileType]].
 *
 * Responsible for lazy loading files for reads and opening closed files in a thread safe manner.
 */
class DBFile(val path: Path,
             val memoryMapped: Boolean,
             val autoClose: Boolean,
             val deleteAfterClean: Boolean,
             val forceSaveConfig: ForceSave,
             fileCache: Cache[swaydb.Error.IO, Unit, DBFileType])(implicit bufferCleaner: ByteBufferSweeperActor,
                                                                  forceSaveApplied: ForceSaveApplier) extends LazyLogging {

  def existsOnDisk =
    Effect.exists(path)

  @inline def file: DBFileType =
    fileCache.value(()).get

  def delete(): Unit =
    fileCache.clearApply {
      case Some(file) =>
        IO {
          file.close()
          //try delegating the delete to the file itself.
          //If the file is already closed, then delete it from disk.
          //memory files are never closed so the first statement will always be executed for memory files.
          if (deleteAfterClean)
            bufferCleaner.actor send ByteBufferSweeper.Command.DeleteFile(path)
          else
            file.delete()
        }

      case None =>
        IO {
          if (deleteAfterClean)
            bufferCleaner.actor send ByteBufferSweeper.Command.DeleteFile(path)
          else
            Effect.deleteIfExists(path)
        }
    } get

  def close(): Unit =
    fileCache.clearApply {
      case Some(file) =>
        IO(file.close())

      case None =>
        IO.unit
    } get

  //if it's an in memory files return failure as Memory files cannot be copied.
  def copyTo(toPath: Path): Path = {
    forceSaveApplied.beforeCopy(this, toPath, forceSaveConfig)

    val copiedPath = Effect.copy(path, toPath)
    logger.trace("{}: Copied: to {}", copiedPath, toPath)
    copiedPath
  }

  def append(slice: Slice[Byte]) =
    fileCache.value(()).get.append(slice)

  def append(slice: Iterable[Slice[Byte]]) =
    fileCache.value(()).get.append(slice)

  def read(position: Int,
           size: Int): Slice[Byte] =
    if (size == 0)
      Slice.emptyBytes
    else
      fileCache.value(()).get.read(position = position, size = size)

  def transfer(position: Int, count: Int, transferTo: DBFile): Int =
    file.transfer(
      position = position,
      count = count,
      transferTo = transferTo.file
    )

  def get(position: Int): Byte =
    fileCache.value(()).get.get(position)

  def getSkipCache(position: Int): Byte =
    fileCache.value(()).get.get(position)

  def readAll: Slice[Byte] =
    fileCache.value(()).get.readAll

  def fileSize: Int =
    fileCache.value(()).get.size

  //memory files are never closed, if it's memory file return true.
  def isOpen: Boolean =
    fileCache.getIO().exists(_.exists(_.isOpen))

  def isFileDefined: Boolean =
    fileCache.getIO().isDefined

  def isMemoryMapped: Boolean =
    memoryMapped

  def isLoaded: Boolean =
    fileCache.value(()).get.isLoaded

  def isFull: Boolean =
    fileCache.value(()).get.isFull

  def forceSave(): Unit =
    fileCache.value(()).get.forceSave()

  override def equals(that: Any): Boolean =
    that match {
      case other: DBFile =>
        this.path == other.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    MurmurHash3.stringHash(path.toString)
}
