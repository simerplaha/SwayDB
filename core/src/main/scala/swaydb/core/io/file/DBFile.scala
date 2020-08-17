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

package swaydb.core.io.file

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.IO.ExceptionHandler
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper, FileSweeperItem}
import swaydb.core.cache.Cache
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.data.Reserve
import swaydb.data.config.IOStrategy
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.util.hashing.MurmurHash3

object DBFile extends LazyLogging {

  def fileCache(filePath: Path,
                memoryMapped: Boolean,
                deleteOnClean: Boolean,
                ioStrategy: IOStrategy.ThreadSafe,
                file: Option[DBFileType],
                blockCacheFileId: Long,
                autoClose: Boolean)(implicit fileSweeper: FileSweeperActor,
                                    bufferCleaner: ByteBufferSweeperActor) = {

    //FIX-ME: need a better solution.
    var self: Cache[Error.IO, Unit, DBFileType] = null

    val closer: FileSweeperItem =
      new FileSweeperItem {
        override def path: Path = filePath

        override def delete(): Unit = {
          val message = s"Delete invoked on only closeable ${classOf[FileSweeperItem].getSimpleName}. Path: $path. autoClose = $autoClose. deleteOnClean = $deleteOnClean."
          val exception = new Exception(message)
          logger.error(message, exception)
          exception.printStackTrace()
        }

        override def close(): Unit =
          self.clearApply(_.foreach(_.close()))

        override def isOpen: Boolean =
          self.getIO().exists(_.exists(_.isOpen))
      }

    val cache =
      Cache.io[swaydb.Error.IO, Error.OpeningFile, Unit, DBFileType](
        //force the cache to be cacheOnAccess regardless of what's configured.
        //This is also needed because only a single thread can close or delete a
        //file using clearApply and stored cached is required otherwise this will lead
        //to many open FileChannels without reference which results in "too many files open" exception.
        strategy = ioStrategy.forceCacheOnAccess,
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
                  deleteOnClean = deleteOnClean
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
                   ioStrategy: IOStrategy.ThreadSafe,
                   blockCacheFileId: Long,
                   autoClose: Boolean)(implicit fileSweeper: FileSweeperActor,
                                       blockCache: Option[BlockCache.State],
                                       bufferCleaner: ByteBufferSweeperActor): DBFile = {
    val file = ChannelFile.write(path, blockCacheFileId)
    new DBFile(
      path = path,
      memoryMapped = false,
      autoClose = autoClose,
      deleteOnClean = false,
      blockCacheFileId = blockCacheFileId,
      fileCache =
        fileCache(
          filePath = path,
          memoryMapped = false,
          deleteOnClean = false,
          file = Some(file),
          ioStrategy = ioStrategy,
          autoClose = autoClose,
          blockCacheFileId = blockCacheFileId
        )
    )
  }

  def channelRead(path: Path,
                  ioStrategy: IOStrategy.ThreadSafe,
                  autoClose: Boolean,
                  blockCacheFileId: Long,
                  checkExists: Boolean = true)(implicit fileSweeper: FileSweeperActor,
                                               blockCache: Option[BlockCache.State],
                                               bufferCleaner: ByteBufferSweeperActor): DBFile =
    if (checkExists && Effect.notExists(path))
      throw swaydb.Exception.NoSuchFile(path)
    else {
      new DBFile(
        path = path,
        memoryMapped = false,
        autoClose = autoClose,
        deleteOnClean = false,
        blockCacheFileId = blockCacheFileId,
        fileCache =
          fileCache(
            filePath = path,
            memoryMapped = false,
            deleteOnClean = false,
            ioStrategy = ioStrategy,
            file = None,
            blockCacheFileId = blockCacheFileId,
            autoClose = autoClose
          )
      )
    }

  def mmapWriteAndRead(path: Path,
                       ioStrategy: IOStrategy.ThreadSafe,
                       autoClose: Boolean,
                       deleteOnClean: Boolean,
                       blockCacheFileId: Long,
                       bytes: Iterable[Slice[Byte]])(implicit fileSweeper: FileSweeperActor,
                                                     blockCache: Option[BlockCache.State],
                                                     bufferCleaner: ByteBufferSweeperActor): DBFile = {
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
        ioStrategy = ioStrategy,
        bufferSize = totalWritten,
        blockCacheFileId = blockCacheFileId,
        autoClose = autoClose,
        deleteOnClean = deleteOnClean
      )

    file.append(bytes)
    file
  }

  def mmapWriteAndRead(path: Path,
                       ioStrategy: IOStrategy.ThreadSafe,
                       autoClose: Boolean,
                       deleteOnClean: Boolean,
                       blockCacheFileId: Long,
                       bytes: Slice[Byte])(implicit fileSweeper: FileSweeperActor,
                                           blockCache: Option[BlockCache.State],
                                           bufferCleaner: ByteBufferSweeperActor): DBFile =
  //do not write bytes if the Slice has empty bytes.
    if (!bytes.isFull) {
      throw swaydb.Exception.FailedToWriteAllBytes(0, bytes.size, bytes.size)
    } else {
      val file =
        mmapInit(
          path = path,
          ioStrategy = ioStrategy,
          bufferSize = bytes.size,
          blockCacheFileId = blockCacheFileId,
          autoClose = autoClose,
          deleteOnClean = deleteOnClean
        )

      file.append(bytes)
      file
    }

  def mmapRead(path: Path,
               ioStrategy: IOStrategy.ThreadSafe,
               autoClose: Boolean,
               deleteOnClean: Boolean,
               blockCacheFileId: Long,
               checkExists: Boolean = true)(implicit fileSweeper: FileSweeperActor,
                                            blockCache: Option[BlockCache.State],
                                            bufferCleaner: ByteBufferSweeperActor): DBFile =
    if (checkExists && Effect.notExists(path)) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      new DBFile(
        path = path,
        memoryMapped = true,
        autoClose = autoClose,
        deleteOnClean = deleteOnClean,
        blockCacheFileId = blockCacheFileId,
        fileCache =
          fileCache(
            filePath = path,
            memoryMapped = true,
            deleteOnClean = deleteOnClean,
            ioStrategy = ioStrategy,
            file = None,
            blockCacheFileId = blockCacheFileId,
            autoClose = autoClose
          )
      )
    }

  def mmapInit(path: Path,
               ioStrategy: IOStrategy.ThreadSafe,
               bufferSize: Long,
               blockCacheFileId: Long,
               autoClose: Boolean,
               deleteOnClean: Boolean)(implicit fileSweeper: FileSweeperActor,
                                       blockCache: Option[BlockCache.State],
                                       bufferCleaner: ByteBufferSweeperActor): DBFile = {
    val file =
      MMAPFile.write(
        path = path,
        bufferSize = bufferSize,
        blockCacheFileId = blockCacheFileId,
        deleteOnClean = deleteOnClean
      )

    new DBFile(
      path = path,
      memoryMapped = true,
      autoClose = autoClose,
      deleteOnClean = deleteOnClean,
      blockCacheFileId = blockCacheFileId,
      fileCache =
        fileCache(
          filePath = path,
          memoryMapped = true,
          deleteOnClean = deleteOnClean,
          ioStrategy = ioStrategy,
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
             val deleteOnClean: Boolean,
             val blockCacheFileId: Long,
             fileCache: Cache[swaydb.Error.IO, Unit, DBFileType])(implicit blockCache: Option[BlockCache.State],
                                                                  bufferCleaner: ByteBufferSweeperActor) extends LazyLogging {

  def existsOnDisk =
    Effect.exists(path)

  def blockSize: Option[Int] =
    blockCache.map(_.blockSize)

  def file: DBFileType =
    fileCache.value(()).get

  def delete(): Unit =
    fileCache.clearApply {
      case Some(file) =>
        file.close()
        //try delegating the delete to the file itself.
        //If the file is already closed, then delete it from disk.
        //memory files are never closed so the first statement will always be executed for memory files.
        if (deleteOnClean)
          bufferCleaner.actor send ByteBufferSweeper.Command.DeleteFile(path)
        else
          file.delete()

      case None =>
        if (deleteOnClean)
          bufferCleaner.actor send ByteBufferSweeper.Command.DeleteFile(path)
        else
          Effect.deleteIfExists(path)
    }

  def close(): Unit =
    fileCache.clearApply(_.foreach(_.close()))

  //if it's an in memory files return failure as Memory files cannot be copied.
  def copyTo(toPath: Path): Path = {
    forceSave()
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
    fileCache.value(()).get.isMemoryMapped

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
