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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.io.file

import com.typesafe.scalalogging.LazyLogging
import java.nio.file.{NoSuchFileException, Path}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.util.hashing.MurmurHash3
import swaydb.core.queue.{FileLimiter, LimiterType}
import swaydb.core.segment.SegmentException
import swaydb.core.segment.SegmentException.CannotCopyInMemoryFiles
import swaydb.data.io.IO
import swaydb.data.slice.Slice

object DBFile {

  def write(bytes: Slice[Byte],
            path: Path): IO[Path] =
    IOOps.write(bytes, path)

  def channelWrite(path: Path, autoClose: Boolean)(implicit ec: ExecutionContext,
                                                   limiter: FileLimiter): IO[DBFile] =
    ChannelFile.write(path) map {
      file =>
        new DBFile(path = path, memoryMapped = false, memory = false, autoClose = autoClose, file = Some(file))
    }

  def channelRead(path: Path, autoClose: Boolean, checkExists: Boolean = true)(implicit ec: ExecutionContext,
                                                                               limiter: FileLimiter): IO[DBFile] =
    if (checkExists && IOOps.notExists(path))
      IO.Failure(new NoSuchFileException(path.toString))
    else
      IO(new DBFile(path = path, memoryMapped = false, memory = false, autoClose = autoClose, file = None))

  def mmapWriteAndRead(bytes: Slice[Byte],
                       path: Path,
                       autoClose: Boolean)(implicit ec: ExecutionContext,
                                           limiter: FileLimiter): IO[DBFile] =
  //do not write bytes if the Slice has empty bytes.
    if (!bytes.isFull)
      IO.Failure(SegmentException.FailedToWriteAllBytes(0, bytes.written, bytes.size))
    else
      mmapInit(path, bytes.written, autoClose = autoClose) flatMap {
        file =>
          file.append(bytes) map {
            _ =>
              file
          }
      }

  def mmapRead(path: Path, autoClose: Boolean, checkExists: Boolean = true)(implicit ec: ExecutionContext,
                                                                            limiter: FileLimiter): IO[DBFile] =
    if (checkExists && IOOps.notExists(path))
      IO.Failure(new NoSuchFileException(path.toString))
    else
      IO(new DBFile(path = path, memoryMapped = true, memory = false, autoClose = autoClose, file = None))

  def mmapInit(path: Path,
               bufferSize: Long,
               autoClose: Boolean)(implicit ec: ExecutionContext,
                                   limiter: FileLimiter): IO[DBFile] =
    MMAPFile.write(path, bufferSize) map {
      file =>
        new DBFile(path = path, memoryMapped = true, memory = false, autoClose = autoClose, file = Some(file))
    }

  def memory(path: Path,
             bytes: Slice[Byte],
             autoClose: Boolean)(implicit ec: ExecutionContext,
                                 limiter: FileLimiter): IO[DBFile] =
    IO {
      new DBFile(path = path, memoryMapped = false, memory = true, autoClose = autoClose, file = Some(MemoryFile(path, bytes)))
    }
}
/**
  * Wrapper class for different file types of [[DBFileType]].
  *
  * Responsible for lazy loading files for reads and opening closed files in a thread safe manner.
  */
class DBFile(val path: Path,
             memoryMapped: Boolean,
             val memory: Boolean,
             autoClose: Boolean,
             @volatile var file: Option[DBFileType])(implicit ec: ExecutionContext,
                                                     limiter: FileLimiter) extends LimiterType with LazyLogging {

  private val open = new AtomicBoolean(file.exists(_.isOpen))

  if (autoClose && open.get) limiter.close(this)

  def existsOnDisk =
    IOOps.exists(path)

  def existsInMemory =
    file.isDefined

  def delete(): IO[Unit] =
  //close the file
    close flatMap {
      _ =>
        //try delegating the delete to the file itself.
        //If the file is already closed, then delete it from disk.
        //memory files are never closed so the first statement will always be executed for memory files.
        (file.map(_.delete()) getOrElse IOOps.deleteIfExists(path)) map {
          _ =>
            file = None
        }
    }

  def close: IO[Unit] =
    file map {
      fileType =>
        fileType.close() map {
          _ =>
            open.set(false)
            //cannot lose reference to in-memory file on close. Only on delete, this in-memory file reference can be discarded.
            if (!memory) file = None
        }
    } getOrElse IO.successUnit

  //if it's an in memory files return failure as Memory files cannot be copied.
  def copyTo(toPath: Path): IO[Path] =
    if (file.map(_.memory).getOrElse(false))
      IO.Failure(CannotCopyInMemoryFiles(path))
    else {
      forceSave() flatMap {
        _ =>
          IOOps.copy(path, toPath) map {
            path =>
              logger.trace("{}: Copied: to {}", path, toPath)
              path
          }
      }
    }

  private def tryOpen(): IO[DBFileType] =
    if (open.compareAndSet(false, true)) {
      logger.trace(s"{}: Opening closed file.", path)
      val openResult =
        if (memory)
          file.map(IO.Success(_)) getOrElse {
            open.set(false)
            IO.Failure(new NoSuchFileException(path.toString))
          }
        else if (memoryMapped)
          MMAPFile.read(path)
        else
          ChannelFile.read(path)

      openResult match {
        case success @ IO.Success(fileOpened) =>
          file.foreach(_.close())
          file = Some(fileOpened)
          if (autoClose)
            limiter.close(this)
          success

        case failed @ IO.Failure(_) =>
          open.set(false)
          failed
      }
    } else {
      file match {
        case Some(fileOpened) =>
          IO.Success(fileOpened)

        case None =>
          IO.Failure(SegmentException.BusyOpeningFile(path))
      }
    }

  private def openFile(): IO[DBFileType] =
    file match {
      case Some(openedFile) =>
        IO.Success(openedFile)

      case None =>
        tryOpen()
    }

  def append(slice: Slice[Byte]) =
    openFile() flatMap (_.append(slice))

  def read(position: Int, size: Int) =
    openFile() flatMap (_.read(position, size))

  def get(position: Int) =
    openFile() flatMap (_.get(position))

  def readAll =
    openFile() flatMap (_.readAll)

  def fileSize =
    openFile() flatMap (_.fileSize)

  //memory files are never closed, if it's memory file return true.
  def isOpen =
    open.get()

  def isFileDefined =
    file.isDefined

  def isMemoryMapped =
    openFile() flatMap (_.isMemoryMapped)

  def isLoaded =
    openFile() flatMap (_.isLoaded)

  def isFull: IO[Boolean] =
    openFile() flatMap (_.isFull)

  def forceSave(): IO[Unit] =
    file.map(_.forceSave()) getOrElse IO.successUnit

  def persistent: Boolean =
    !memory

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
