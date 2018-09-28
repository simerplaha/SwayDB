/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import java.nio.file.{NoSuchFileException, Path}
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.segment.SegmentException
import swaydb.core.segment.SegmentException.CannotCopyInMemoryFiles
import swaydb.core.util.TryUtil
import swaydb.data.slice.Slice

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object DBFile {

  def write(bytes: Slice[Byte],
            path: Path): Try[Path] =
    IO.write(bytes, path)

  def channelWrite(path: Path, onOpen: DBFile => Unit = _ => ())(implicit ec: ExecutionContext): Try[DBFile] =
    ChannelFile.write(path) map {
      file =>
        new DBFile(path = path, memoryMapped = false, onOpen = onOpen, memory = false, file = Some(file))
    }

  def channelRead(path: Path, onOpen: DBFile => Unit = _ => (), checkExists: Boolean = true)(implicit ec: ExecutionContext): Try[DBFile] =
    if (checkExists && IO.notExists(path))
      Failure(new NoSuchFileException(path.toString))
    else
      Try(new DBFile(path = path, memoryMapped = false, onOpen = onOpen, memory = false, file = None))

  def mmapWriteAndRead(bytes: Slice[Byte],
                       path: Path,
                       onOpen: DBFile => Unit = _ => ())(implicit ec: ExecutionContext): Try[DBFile] =
  //do not write bytes if the Slice has empty bytes.
    if (!bytes.isFull)
      Failure(SegmentException.FailedToWriteAllBytes(0, bytes.written, bytes.size))
    else
      mmapInit(path, bytes.written, onOpen) flatMap {
        file =>
          file.append(bytes) map {
            _ =>
              file
          }
      }

  def mmapRead(path: Path, onOpen: DBFile => Unit = _ => (), checkExists: Boolean = true)(implicit ec: ExecutionContext): Try[DBFile] =
    if (checkExists && IO.notExists(path))
      Failure(new NoSuchFileException(path.toString))
    else
      Try(new DBFile(path = path, memoryMapped = true, onOpen, memory = false, file = None))

  def mmapInit(path: Path,
               bufferSize: Long,
               onOpen: DBFile => Unit = _ => ())(implicit ec: ExecutionContext): Try[DBFile] =
    MMAPFile.write(path, bufferSize) map {
      file =>
        new DBFile(path = path, memoryMapped = true, onOpen = onOpen, memory = false, file = Some(file))
    }

  def memory(path: Path, bytes: Slice[Byte])(implicit ec: ExecutionContext): Try[DBFile] =
    Try {
      new DBFile(path = path, memoryMapped = false, onOpen = _ => Unit, memory = true, file = Some(MemoryFile(path, bytes)))
    }
}
/**
  * Wrapper class for different file types of [[DBFileType]].
  *
  * Responsible for lazy loading files for reads and opening closed files in a thread safe manner.
  */
class DBFile(val path: Path,
             memoryMapped: Boolean,
             onOpen: DBFile => Unit,
             val memory: Boolean,
             @volatile var file: Option[DBFileType])(implicit ec: ExecutionContext) extends LazyLogging with DBFileType {

  private val open = new AtomicBoolean(file.exists(_.isOpen))

  if (open.get) onOpen(this)

  def existsOnDisk =
    IO.exists(path)

  def existsInMemory =
    file.isDefined

  def delete(): Try[Unit] =
  //close the file
    close flatMap {
      _ =>
        //try delegating the delete to the file itself.
        //If the file is already closed, then delete it from disk.
        //memory files are never closed so the first statement will always be executed for memory files.
        (file.map(_.delete()) getOrElse IO.deleteIfExists(path)) map {
          _ =>
            file = None
        }
    }

  def close: Try[Unit] =
    file map {
      fileType =>
        fileType.close() map {
          _ =>
            open.set(false)
            //cannot lose reference to in-memory file on close. Only on delete, this in-memory file reference can be discarded.
            if (!memory) file = None
        }
    } getOrElse TryUtil.successUnit

  //if it's an in memory files return failure as Memory files cannot be copied.
  def copyTo(toPath: Path): Try[Path] =
    if (file.map(_.memory).getOrElse(false))
      Failure(CannotCopyInMemoryFiles(path))
    else {
      forceSave() flatMap {
        _ =>
          IO.copy(path, toPath) map {
            path =>
              logger.trace("{}: Copied: to {}", path, toPath)
              path
          }
      }
    }

  private def openFile(): Try[DBFileType] =
    file match {
      case Some(openedFile) =>
        Success(openedFile)

      case None =>
        if (open.compareAndSet(false, true)) {
          logger.trace(s"{}: Opening closed file.", path)
          val openResult =
            if (memory)
              file.map(Success(_)) getOrElse {
                open.set(false)
                Failure(new NoSuchFileException(path.toString))
              }
            else if (memoryMapped)
              MMAPFile.read(path)
            else
              ChannelFile.read(path)

          openResult match {
            case success @ Success(fileOpened) =>
              file.foreach(_.close())
              file = Some(fileOpened)
              onOpen(this)
              success

            case failed @ Failure(_) =>
              open.set(false)
              failed
          }
        } else {
          file match {
            case Some(fileOpened) =>
              Success(fileOpened)

            case None =>
              Failure(SegmentException.FailedToOpenFile(path))
          }
        }
    }

  override def append(slice: Slice[Byte]) =
    openFile() flatMap (_.append(slice))

  override def read(position: Int, size: Int) =
    openFile() flatMap (_.read(position, size))

  override def get(position: Int) =
    openFile() flatMap (_.get(position))

  override def readAll =
    openFile() flatMap (_.readAll)

  override def fileSize =
    openFile() flatMap (_.fileSize)

  //memory files are never closed, if it's memory file return true.
  override def isOpen =
    open.get()

  def isFileDefined =
    file.isDefined

  override def isMemoryMapped =
    openFile() flatMap (_.isMemoryMapped)

  override def isLoaded =
    openFile() flatMap (_.isLoaded)

  override def isFull: Try[Boolean] =
    openFile() flatMap (_.isFull)

  override def forceSave(): Try[Unit] =
    file.map(_.forceSave()) getOrElse TryUtil.successUnit

  override def equals(that: Any): Boolean =
    that match {
      case other: DBFile =>
        this.path == other.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()
}
