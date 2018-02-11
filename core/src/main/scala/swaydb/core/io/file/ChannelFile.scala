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

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{NoSuchFileException, Path, StandardOpenOption}

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.slice.Slice

import scala.util.{Failure, Success, Try}

private[file] object ChannelFile {
  def write(path: Path): Try[ChannelFile] =
    Try {
      val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
      new ChannelFile(path, channel)
    }

  def read(path: Path): Try[ChannelFile] =
    if (IO.exists(path))
      Try {
        val channel = FileChannel.open(path, StandardOpenOption.READ)
        new ChannelFile(path, channel)
      }
    else
      Failure(new NoSuchFileException(path.toString))
}

private[file] class ChannelFile(val path: Path,
                                channel: FileChannel) extends LazyLogging with DBFileType {

  def close: Try[Unit] =
    Try {
      //      logger.info(s"$path: Closing channel")
      channel.close()
    }

  def append(slice: Slice[Byte]): Try[Unit] =
    IO.write(slice, channel)

  def read(position: Int, size: Int): Try[Slice[Byte]] =
    Try {
      val buffer = ByteBuffer.allocate(size)
      channel.read(buffer, position)
      Slice(buffer.array())
    }

  def get(position: Int): Try[Byte] =
    read(position, 1).map(_.head)

  def readAll: Try[Slice[Byte]] =
    Try {
      val bytes = new Array[Byte](channel.size().toInt)
      channel.read(ByteBuffer.wrap(bytes))
      Slice(bytes)
    }

  def fileSize =
    Try(channel.size())

  override def isOpen =
    channel.isOpen

  override def isMemoryMapped =
    Success(false)

  override def isLoaded =
    Success(false)

  override def isFull =
    Success(false)

  override def memory: Boolean =
    false

  override def delete(): Try[Unit] =
    close flatMap {
      _ =>
        IO.delete(path)
    }

  override def forceSave(): Try[Unit] =
    Success()
}
