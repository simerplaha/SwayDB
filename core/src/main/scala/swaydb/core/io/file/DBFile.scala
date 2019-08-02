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

package swaydb.core.io.file

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.IO.ErrorHandler
import swaydb.IO._
import swaydb.core.queue.{FileLimiter, FileLimiterItem}
import swaydb.core.util.cache.Cache
import swaydb.data.Reserve
import swaydb.data.config.IOStrategy
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.util.hashing.MurmurHash3

object DBFile extends LazyLogging {

  def fileCache(path: Path,
                memoryMapped: Boolean,
                ioStrategy: IOStrategy,
                file: Option[DBFileType],
                autoClose: Boolean)(implicit limiter: FileLimiter) =
    Cache.blockIO[swaydb.Error.IO, Error.OpeningFile, Unit, DBFileType](
      blockIO = (_: Unit) => ioStrategy,
      reserveError = Error.OpeningFile(path, Reserve(name = s"DBFile: $path. MemoryMapped: $memoryMapped")),
      initial = file
    ) {
      _ =>
        logger.trace(s"{}: Opening closed file.", path)
        val openResult =
          if (memoryMapped)
            MMAPFile.read(path)
          else
            ChannelFile.read(path)

        if (autoClose) openResult.foreach(limiter.close)
        openResult
    }

  def write(path: Path,
            bytes: Slice[Byte]): IO[swaydb.Error.IO, Path] =
    IOEffect.write(path, bytes)

  def write(path: Path,
            bytes: Iterable[Slice[Byte]]): IO[swaydb.Error.IO, Path] =
    IOEffect.write(path, bytes)

  def channelWrite(path: Path,
                   ioStrategy: IOStrategy,
                   autoClose: Boolean)(implicit limiter: FileLimiter): IO[swaydb.Error.IO, DBFile] =
    ChannelFile.write(path) map {
      file =>
        new DBFile(
          path = path,
          memoryMapped = false,
          autoClose = autoClose,
          cache =
            fileCache(
              path = path,
              memoryMapped = false,
              file = Some(file),
              ioStrategy = ioStrategy,
              autoClose = autoClose
            )
        )
    }

  def channelRead(path: Path,
                  ioStrategy: IOStrategy,
                  autoClose: Boolean,
                  checkExists: Boolean = true)(implicit limiter: FileLimiter): IO[swaydb.Error.IO, DBFile] =
    if (checkExists && IOEffect.notExists(path))
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      IO(
        new DBFile(
          path = path,
          memoryMapped = false,
          autoClose = autoClose,
          cache =
            fileCache(
              path = path,
              memoryMapped = false,
              file = None,
              ioStrategy = ioStrategy,
              autoClose = autoClose
            )
        )
      )

  def mmapWriteAndRead(path: Path,
                       ioStrategy: IOStrategy,
                       autoClose: Boolean,
                       bytes: Iterable[Slice[Byte]])(implicit limiter: FileLimiter): IO[swaydb.Error.IO, DBFile] =
  //do not write bytes if the Slice has empty bytes.
    bytes.foldLeftIO(0) {
      case (written, bytes) =>
        if (!bytes.isFull)
          IO.failed(swaydb.Exception.FailedToWriteAllBytes(0, bytes.size, bytes.size))
        else
          IO.Success(written + bytes.size)
    } flatMap {
      totalWritten =>
        mmapInit(
          path = path,
          bufferSize = totalWritten,
          ioStrategy = ioStrategy,
          autoClose = autoClose
        ) flatMap {
          file =>
            file.append(bytes) map {
              _ =>
                file
            }
        }
    }

  def mmapWriteAndRead(path: Path,
                       ioStrategy: IOStrategy,
                       autoClose: Boolean,
                       bytes: Slice[Byte])(implicit limiter: FileLimiter): IO[swaydb.Error.IO, DBFile] =
  //do not write bytes if the Slice has empty bytes.
    if (!bytes.isFull)
      IO.failed(swaydb.Exception.FailedToWriteAllBytes(0, bytes.size, bytes.size))
    else
      mmapInit(
        path = path,
        bufferSize = bytes.size,
        ioStrategy = ioStrategy,
        autoClose = autoClose
      ) flatMap {
        file =>
          file.append(bytes) map {
            _ =>
              file
          }
      }

  def mmapRead(path: Path,
               ioStrategy: IOStrategy,
               autoClose: Boolean,
               checkExists: Boolean = true)(implicit limiter: FileLimiter): IO[swaydb.Error.IO, DBFile] =
    if (checkExists && IOEffect.notExists(path))
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      IO(
        new DBFile(
          path = path,
          memoryMapped = true,
          autoClose = autoClose,
          cache =
            fileCache(
              path = path,
              memoryMapped = true,
              ioStrategy = ioStrategy,
              file = None,
              autoClose = autoClose
            )
        )
      )

  def mmapInit(path: Path,
               ioStrategy: IOStrategy,
               bufferSize: Long,
               autoClose: Boolean)(implicit limiter: FileLimiter): IO[swaydb.Error.IO, DBFile] =
    MMAPFile.write(path, bufferSize) map {
      file =>
        new DBFile(
          path = path,
          memoryMapped = true,
          autoClose = autoClose,
          cache =
            fileCache(
              path = path,
              memoryMapped = true,
              file = Some(file),
              ioStrategy = ioStrategy,
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
             memoryMapped: Boolean,
             autoClose: Boolean,
             cache: Cache[swaydb.Error.IO, Unit, DBFileType])(implicit limiter: FileLimiter) extends FileLimiterItem with LazyLogging {

  private val busy = Reserve[Unit](name = s"${this.getClass.getSimpleName}: $path")
  require(busy.isFree)

  if (autoClose && isOpen) limiter.close(this)

  def existsOnDisk =
    IOEffect.exists(path)

  def file: IO[Error.IO, DBFileType] =
    cache.value()

  def delete(): IO[swaydb.Error.IO, Unit] =
  //close the file
    close flatMap {
      _ =>
        //try delegating the delete to the file itself.
        //If the file is already closed, then delete it from disk.
        //memory files are never closed so the first statement will always be executed for memory files.
        (cache.value().map(_.delete()) getOrElse IOEffect.deleteIfExists(path)) map {
          _ =>
            cache.clear()
        }
    }

  def close: IO[swaydb.Error.IO, Unit] =
    cache.value() map {
      fileType =>
        fileType.close() map {
          _ =>
            cache.clear()
        }
    } getOrElse IO.unit

  //if it's an in memory files return failure as Memory files cannot be copied.
  def copyTo(toPath: Path): IO[swaydb.Error.IO, Path] =
    forceSave() flatMap {
      _ =>
        IOEffect.copy(path, toPath) map {
          path =>
            logger.trace("{}: Copied: to {}", path, toPath)
            path
        }
    }

  def append(slice: Slice[Byte]) =
    cache.value() flatMap (_.append(slice))

  def append(slice: Iterable[Slice[Byte]]) =
    cache.value() flatMap (_.append(slice))

  def read(position: Int, size: Int): IO[swaydb.Error.IO, Slice[Byte]] =
    if (size == 0)
      IO.emptyBytes
    else
      cache.value() flatMap (_.read(position, size))

  def get(position: Int) =
    cache.value() flatMap (_.get(position))

  def readAll =
    cache.value() flatMap (_.readAll)

  def fileSize =
    cache.value() flatMap (_.fileSize)

  //memory files are never closed, if it's memory file return true.
  def isOpen =
    cache.value().exists(_.isOpen)

  def isFileDefined =
    cache.isCached

  def isMemoryMapped =
    cache.value() flatMap (_.isMemoryMapped)

  def isLoaded =
    cache.value() flatMap (_.isLoaded)

  def isFull: IO[swaydb.Error.IO, Boolean] =
    cache.value() flatMap (_.isFull)

  def forceSave(): IO[swaydb.Error.IO, Unit] =
    cache.value().map(_.forceSave()) getOrElse IO.unit

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
