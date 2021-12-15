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

package swaydb.core.file

import com.typesafe.scalalogging.LazyLogging
import swaydb.{Error, IO}
import swaydb.Error.IO.ExceptionHandler
import swaydb.config.ForceSave
import swaydb.core.cache.Cache
import swaydb.core.file.sweeper.{FileSweeper, FileSweeperCommand, FileSweeperItem}
import swaydb.core.file.sweeper.bytebuffer.ByteBufferCommand
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.effect.{Effect, IOStrategy, Reserve}
import swaydb.slice.{Slice, SliceRO}

import java.nio.file.Path

private[core] object CoreFile extends LazyLogging {

  private def fileCache(filePath: Path,
                        memoryMapped: Boolean,
                        deleteAfterClean: Boolean,
                        fileOpenIOStrategy: IOStrategy.ThreadSafe,
                        file: Option[CoreFileType],
                        autoClose: Boolean)(implicit fileSweeper: FileSweeper,
                                            bufferCleaner: ByteBufferSweeperActor,
                                            forceSaveApplier: ForceSaveApplier): Cache[Error.IO, Unit, CoreFileType] = {

    //We need to create a single FileSweeperItem that can be
    //re-submitted to fileSweeper every time this file requires a close Actor request.
    //Creating new FileSweeper item for each close request would result in cleaner
    //code without null but would be expensive considering the number of objects created.
    var self: Cache[Error.IO, Unit, CoreFileType] = null

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
          self.state().exists(_.exists(_.isOpen))
      }

    val cache =
      Cache[swaydb.Error.IO, Error.OpeningFile, Unit, CoreFileType](
        //force the cache to be cacheOnAccess regardless of what's configured.
        //This is also needed because only a single thread can close or delete a
        //file using clearApply and stored cached is required otherwise this will lead
        //to many open FileChannels without reference which results in "too many files open" exception.
        strategy = fileOpenIOStrategy.forceCacheOnAccess,
        reserveError = Error.OpeningFile(filePath, Reserve.free(name = s"CoreFile: $filePath. MemoryMapped: $memoryMapped")),
        initial = file
      ) {
        (_, _) =>
          logger.debug(s"{}: Opening closed file.", filePath)

          IO {
            val file =
              if (memoryMapped)
                MMAPFile.readable(
                  path = filePath,
                  deleteAfterClean = deleteAfterClean
                )
              else
                StandardFile.readable(path = filePath)

            if (autoClose)
              fileSweeper send FileSweeperCommand.CloseFileItem(closer)

            file
          }
      }

    self = cache

    if (autoClose && file.isDefined) fileSweeper send FileSweeperCommand.CloseFileItem(closer)
    cache
  }

  def standardWritable(path: Path,
                       fileOpenIOStrategy: IOStrategy.ThreadSafe,
                       autoClose: Boolean,
                       forceSave: ForceSave.StandardFiles)(implicit fileSweeper: FileSweeper,
                                                           bufferCleaner: ByteBufferSweeperActor,
                                                           forceSaveApplier: ForceSaveApplier): CoreFile =
    new CoreFile(
      path = path,
      memoryMapped = false,
      autoClose = autoClose,
      deleteAfterClean = false,
      forceSaveConfig = forceSave,
      cache =
        fileCache(
          filePath = path,
          memoryMapped = false,
          deleteAfterClean = false,
          file = Some(StandardFile.writeable(path, forceSave)),
          fileOpenIOStrategy = fileOpenIOStrategy,
          autoClose = autoClose
        )
    )

  def standardReadable(path: Path,
                       fileOpenIOStrategy: IOStrategy.ThreadSafe,
                       autoClose: Boolean,
                       checkExists: Boolean = true)(implicit fileSweeper: FileSweeper,
                                                    bufferCleaner: ByteBufferSweeperActor,
                                                    forceSaveApplier: ForceSaveApplier): CoreFile =
    if (checkExists && Effect.notExists(path))
      throw swaydb.Exception.NoSuchFile(path)
    else
      new CoreFile(
        path = path,
        memoryMapped = false,
        autoClose = autoClose,
        deleteAfterClean = false,
        forceSaveConfig = ForceSave.Off,
        cache =
          fileCache(
            filePath = path,
            memoryMapped = false,
            deleteAfterClean = false,
            fileOpenIOStrategy = fileOpenIOStrategy,
            file = None,
            autoClose = autoClose
          )
      )

  def mmapWriteableReadable(path: Path,
                            fileOpenIOStrategy: IOStrategy.ThreadSafe,
                            autoClose: Boolean,
                            deleteAfterClean: Boolean,
                            forceSave: ForceSave.MMAPFiles,
                            bytes: Array[Slice[Byte]])(implicit fileSweeper: FileSweeper,
                                                       bufferCleaner: ByteBufferSweeperActor,
                                                       forceSaveApplier: ForceSaveApplier): CoreFile = {
    val totalWritten =
      bytes.foldLeft(0) { //do not write bytes if the Slice has empty bytes.
        case (written, bytes) =>
          if (!bytes.isFull)
            throw swaydb.Exception.FailedToWriteAllBytes(0, bytes.allocatedSize, bytes.size)
          else
            written + bytes.size
      }

    val file =
      mmapEmptyWriteableReadable(
        path = path,
        fileOpenIOStrategy = fileOpenIOStrategy,
        bufferSize = totalWritten,
        autoClose = autoClose,
        forceSave = forceSave,
        deleteAfterClean = deleteAfterClean
      )

    file.appendBatch(bytes)
    file
  }

  def mmapWriteableReadable(path: Path,
                            fileOpenIOStrategy: IOStrategy.ThreadSafe,
                            autoClose: Boolean,
                            deleteAfterClean: Boolean,
                            forceSave: ForceSave.MMAPFiles,
                            bytes: Slice[Byte])(implicit fileSweeper: FileSweeper,
                                                bufferCleaner: ByteBufferSweeperActor,
                                                forceSaveApplier: ForceSaveApplier): CoreFile =
  //do not write bytes if the Slice has empty bytes.
    if (!bytes.isFull) {
      throw swaydb.Exception.FailedToWriteAllBytes(0, bytes.allocatedSize, bytes.size)
    } else {
      val file =
        mmapEmptyWriteableReadable(
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

  def mmapReadable(path: Path,
                   fileOpenIOStrategy: IOStrategy.ThreadSafe,
                   autoClose: Boolean,
                   deleteAfterClean: Boolean,
                   checkExists: Boolean = true)(implicit fileSweeper: FileSweeper,
                                                bufferCleaner: ByteBufferSweeperActor,
                                                forceSaveApplier: ForceSaveApplier): CoreFile =
    if (checkExists && Effect.notExists(path)) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      new CoreFile(
        path = path,
        memoryMapped = true,
        autoClose = autoClose,
        deleteAfterClean = deleteAfterClean,
        forceSaveConfig = ForceSave.Off,
        cache =
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

  def mmapWriteableReadableTransfer(path: Path,
                                    fileOpenIOStrategy: IOStrategy.ThreadSafe,
                                    autoClose: Boolean,
                                    deleteAfterClean: Boolean,
                                    forceSave: ForceSave.MMAPFiles,
                                    bufferSize: Int,
                                    transfer: CoreFile => Unit)(implicit fileSweeper: FileSweeper,
                                                                bufferCleaner: ByteBufferSweeperActor,
                                                                forceSaveApplier: ForceSaveApplier): CoreFile = {
    val file =
      mmapEmptyWriteableReadable(
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

  def mmapEmptyWriteableReadable(path: Path,
                                 fileOpenIOStrategy: IOStrategy.ThreadSafe,
                                 bufferSize: Int,
                                 autoClose: Boolean,
                                 deleteAfterClean: Boolean,
                                 forceSave: ForceSave.MMAPFiles)(implicit fileSweeper: FileSweeper,
                                                                 bufferCleaner: ByteBufferSweeperActor,
                                                                 forceSaveApplier: ForceSaveApplier): CoreFile = {
    val file =
      MMAPFile.writeableReadable(
        path = path,
        bufferSize = bufferSize,
        deleteAfterClean = deleteAfterClean,
        forceSave = forceSave
      )

    new CoreFile(
      path = path,
      memoryMapped = true,
      autoClose = autoClose,
      deleteAfterClean = deleteAfterClean,
      forceSaveConfig = forceSave,
      cache =
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
 * Wrapper class for different file types of [[CoreFileType]].
 *
 * Responsible for lazy loading files for reads and opening closed files in a thread safe manner.
 */
private[core] class CoreFile(val path: Path,
                             val memoryMapped: Boolean,
                             val autoClose: Boolean,
                             val deleteAfterClean: Boolean,
                             val forceSaveConfig: ForceSave,
                             cache: Cache[swaydb.Error.IO, Unit, CoreFileType])(implicit bufferCleaner: ByteBufferSweeperActor,
                                                                                forceSaveApplied: ForceSaveApplier) extends LazyLogging {

  def existsOnDisk(): Boolean =
    Effect.exists(path)

  @inline private def file(): CoreFileType =
    cache.getOrFetch(()).get

  def delete(): Unit =
    cache.clearApply {
      case Some(file) =>
        IO {
          file.close()
          //try delegating the delete to the file itself.
          //If the file is already closed, then delete it from disk.
          //memory files are never closed so the first statement will always be executed for memory files.
          if (deleteAfterClean)
            bufferCleaner.actor() send ByteBufferCommand.DeleteFile(path)
          else
            file.delete()
        }

      case None =>
        IO {
          if (deleteAfterClean)
            bufferCleaner.actor() send ByteBufferCommand.DeleteFile(path)
          else
            Effect.deleteIfExists(path)
        }
    } get

  def close(): Unit =
    cache.clearApply {
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

  def append(slice: Slice[Byte]): Unit =
    file().append(slice)

  def appendBatch(slice: Array[Slice[Byte]]): Unit =
    file().appendBatch(slice)

  def read(position: Int,
           size: Int): Slice[Byte] =
    if (size == 0)
      Slice.emptyBytes
    else
      file().read(position = position, size = size)

  def read(position: Int,
           size: Int,
           blockSize: Int): SliceRO[Byte] =
    if (size == 0)
      Slice.emptyBytes //no need to have this as global val because core never asks for 0 size
    else
      file().read(position = position, size = size, blockSize = blockSize)

  def transfer(position: Int, count: Int, transferTo: CoreFile): Int =
    file().transfer(
      position = position,
      count = count,
      transferTo = transferTo.file()
    )

  def get(position: Int): Byte =
    file().get(position)

  def readAll(): Slice[Byte] =
    file().readAll()

  def fileSize(): Int =
    file().size()

  //memory files are never closed, if it's memory file return true.
  def isOpen: Boolean =
    cache.state().exists(_.exists(_.isOpen))

  def isFileDefined: Boolean =
    cache.state().isDefined

  def isLoaded(): Boolean =
    file().isLoaded()

  def isFull(): Boolean =
    file().isFull()

  def forceSave(): Unit =
    file().forceSave()

  override def equals(that: Any): Boolean =
    that match {
      case other: CoreFile =>
        this.path == other.path

      case _ =>
        false
    }

  override def hashCode(): Int =
  //This could be problematic if there is a case where a hashMap of opened
  //file is required. Currently there is no such use-case.
    path.hashCode()
}
