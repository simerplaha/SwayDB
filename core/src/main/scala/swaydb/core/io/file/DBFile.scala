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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.io.file

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.IO.ExceptionHandler
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper, FileSweeperItem}
import swaydb.data.Reserve
import swaydb.data.cache.Cache
import swaydb.data.config.{ForceSave, IOStrategy}
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.util.hashing.MurmurHash3

object DBFile extends LazyLogging {

  def fileCache(filePath: Path,
                memoryMapped: Boolean,
                deleteAfterClean: Boolean,
                fileOpenIOStrategy: IOStrategy.ThreadSafe,
                file: Option[DBFileType],
                blockCacheFileId: Long,
                autoClose: Boolean)(implicit fileSweeper: FileSweeperActor,
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
                  blockCacheFileId = blockCacheFileId,
                  deleteAfterClean = deleteAfterClean
                )
              else
                ChannelFile.read(
                  path = filePath,
                  blockCacheFileId = blockCacheFileId
                )

            if (autoClose)
              fileSweeper send FileSweeper.Command.Close(closer)

            file
          }
      }

    self = cache

    if (autoClose && file.isDefined) fileSweeper send FileSweeper.Command.Close(closer)
    cache
  }

  def channelWrite(path: Path,
                   fileOpenIOStrategy: IOStrategy.ThreadSafe,
                   blockCacheFileId: Long,
                   autoClose: Boolean,
                   forceSave: ForceSave.ChannelFiles)(implicit fileSweeper: FileSweeperActor,
                                                      blockCache: Option[BlockCache.State],
                                                      bufferCleaner: ByteBufferSweeperActor,
                                                      forceSaveApplier: ForceSaveApplier): DBFile = {
    val file = ChannelFile.write(path, blockCacheFileId, forceSave)
    new DBFile(
      path = path,
      memoryMapped = false,
      autoClose = autoClose,
      deleteAfterClean = false,
      forceSaveConfig = forceSave,
      blockCacheFileId = blockCacheFileId,
      fileCache =
        fileCache(
          filePath = path,
          memoryMapped = false,
          deleteAfterClean = false,
          file = Some(file),
          fileOpenIOStrategy = fileOpenIOStrategy,
          autoClose = autoClose,
          blockCacheFileId = blockCacheFileId
        )
    )
  }

  def channelRead(path: Path,
                  fileOpenIOStrategy: IOStrategy.ThreadSafe,
                  autoClose: Boolean,
                  blockCacheFileId: Long,
                  checkExists: Boolean = true)(implicit fileSweeper: FileSweeperActor,
                                               blockCache: Option[BlockCache.State],
                                               bufferCleaner: ByteBufferSweeperActor,
                                               forceSaveApplier: ForceSaveApplier): DBFile =
    if (checkExists && Effect.notExists(path))
      throw swaydb.Exception.NoSuchFile(path)
    else {
      new DBFile(
        path = path,
        memoryMapped = false,
        autoClose = autoClose,
        deleteAfterClean = false,
        forceSaveConfig = ForceSave.Off,
        blockCacheFileId = blockCacheFileId,
        fileCache =
          fileCache(
            filePath = path,
            memoryMapped = false,
            deleteAfterClean = false,
            fileOpenIOStrategy = fileOpenIOStrategy,
            file = None,
            blockCacheFileId = blockCacheFileId,
            autoClose = autoClose
          )
      )
    }

  def mmapWriteAndRead(path: Path,
                       fileOpenIOStrategy: IOStrategy.ThreadSafe,
                       autoClose: Boolean,
                       deleteAfterClean: Boolean,
                       forceSave: ForceSave.MMAPFiles,
                       blockCacheFileId: Long,
                       bytes: Iterable[Slice[Byte]])(implicit fileSweeper: FileSweeperActor,
                                                     blockCache: Option[BlockCache.State],
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
        blockCacheFileId = blockCacheFileId,
        autoClose = autoClose,
        forceSave = forceSave,
        deleteAfterClean = deleteAfterClean
      )

    file.append(bytes)
    file
  }

  def mmapWriteAndRead(path: Path,
                       fileOpenIOStrategy: IOStrategy.ThreadSafe,
                       autoClose: Boolean,
                       deleteAfterClean: Boolean,
                       forceSave: ForceSave.MMAPFiles,
                       blockCacheFileId: Long,
                       bytes: Slice[Byte])(implicit fileSweeper: FileSweeperActor,
                                           blockCache: Option[BlockCache.State],
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
          blockCacheFileId = blockCacheFileId,
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
               blockCacheFileId: Long,
               checkExists: Boolean = true)(implicit fileSweeper: FileSweeperActor,
                                            blockCache: Option[BlockCache.State],
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
        blockCacheFileId = blockCacheFileId,
        fileCache =
          fileCache(
            filePath = path,
            memoryMapped = true,
            deleteAfterClean = deleteAfterClean,
            fileOpenIOStrategy = fileOpenIOStrategy,
            file = None,
            blockCacheFileId = blockCacheFileId,
            autoClose = autoClose
          )
      )
    }

  def mmapInit(path: Path,
               fileOpenIOStrategy: IOStrategy.ThreadSafe,
               bufferSize: Long,
               blockCacheFileId: Long,
               autoClose: Boolean,
               deleteAfterClean: Boolean,
               forceSave: ForceSave.MMAPFiles)(implicit fileSweeper: FileSweeperActor,
                                               blockCache: Option[BlockCache.State],
                                               bufferCleaner: ByteBufferSweeperActor,
                                               forceSaveApplier: ForceSaveApplier): DBFile = {
    val file =
      MMAPFile.write(
        path = path,
        bufferSize = bufferSize,
        blockCacheFileId = blockCacheFileId,
        deleteAfterClean = deleteAfterClean,
        forceSave = forceSave
      )

    new DBFile(
      path = path,
      memoryMapped = true,
      autoClose = autoClose,
      deleteAfterClean = deleteAfterClean,
      forceSaveConfig = forceSave,
      blockCacheFileId = blockCacheFileId,
      fileCache =
        fileCache(
          filePath = path,
          memoryMapped = true,
          deleteAfterClean = deleteAfterClean,
          fileOpenIOStrategy = fileOpenIOStrategy,
          file = Some(file),
          blockCacheFileId = blockCacheFileId,
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
             val blockCacheFileId: Long,
             fileCache: Cache[swaydb.Error.IO, Unit, DBFileType])(implicit blockCache: Option[BlockCache.State],
                                                                  bufferCleaner: ByteBufferSweeperActor,
                                                                  forceSaveApplied: ForceSaveApplier) extends LazyLogging {

  def existsOnDisk =
    Effect.exists(path)

  def blockSize: Option[Int] =
    blockCache.map(_.blockSize)

  def file: DBFileType =
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

  def readBlock(position: Int): Option[Slice[Byte]] =
    blockCache map {
      blockCache =>
        read(
          position = position,
          size = blockCache.blockSize,
          blockCache = blockCache
        )
    }

  def read(position: Int, size: Int): Slice[Byte] =
    if (size == 0)
      Slice.emptyBytes
    else
      blockCache match {
        case Some(blockCache) =>
          read(
            position = position,
            size = size,
            blockCache = blockCache
          )

        case None =>
          fileCache.value(()).get.read(position, size)
      }

  def read(position: Int,
           size: Int,
           blockCache: BlockCache.State): Slice[Byte] =
    if (size == 0)
      Slice.emptyBytes
    else
      BlockCache.getOrSeek(
        position = position,
        size = size,
        file = fileCache.value(()).get,
        state = blockCache
      )

  def get(position: Int): Byte =
    if (blockCache.isDefined)
      read(position, 1).head
    else
      fileCache.value(()).get.get(position)

  def readAll: Slice[Byte] =
    fileCache.value(()).get.readAll

  def fileSize: Long =
    fileCache.value(()).get.fileSize

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
