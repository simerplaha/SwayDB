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

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.data.io.Core
import swaydb.data.io.Core.Error.IO.ErrorHandler
import swaydb.data.slice.Slice

private[file] object ChannelFile {
  def write(path: Path): IO[Core.Error.IO, ChannelFile] =
    IO {
      val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
      new ChannelFile(
        path = path,
        channel = channel
      )
    }

  def read(path: Path): IO[Core.Error.IO, ChannelFile] =
    if (IOEffect.exists(path))
      IO {
        val channel = FileChannel.open(path, StandardOpenOption.READ)
        new ChannelFile(
          path = path,
          channel = channel
        )
      }
    else
      IO.Failure(Core.Error.NoSuchFile(path))
}

private[file] class ChannelFile(val path: Path,
                                channel: FileChannel) extends LazyLogging with DBFileType {

  def close: IO[Core.Error.IO, Unit] =
    IO {
      //      logger.info(s"$path: Closing channel")
      channel.close()
    }

  def append(slice: Slice[Byte]): IO[Core.Error.IO, Unit] =
    IOEffect.writeUnclosed(channel, slice)

  def append(slice: Iterable[Slice[Byte]]): IO[Core.Error.IO, Unit] =
    IOEffect.writeUnclosed(channel, slice)

  def read(position: Int, size: Int): IO[Core.Error.IO, Slice[Byte]] =
    IO {
      val buffer = ByteBuffer.allocate(size)
      channel.read(buffer, position)
      Slice(buffer.array())
    }

  def get(position: Int): IO[Core.Error.IO, Byte] =
    read(position, 1).map(_.head)

  def readAll: IO[Core.Error.IO, Slice[Byte]] =
    IO {
      val bytes = new Array[Byte](channel.size().toInt)
      channel.read(ByteBuffer.wrap(bytes))
      Slice(bytes)
    }

  def fileSize: IO[Core.Error.IO, Long] =
    IO(channel.size())

  override def isOpen =
    channel.isOpen

  override def isMemoryMapped =
    IO.`false`

  override def isLoaded =
    IO.`false`

  override def isFull =
    IO.`false`

  override def memory: Boolean =
    false

  override def delete(): IO[Core.Error.IO, Unit] =
    close flatMap {
      _ =>
        IOEffect.delete(path)
    }

  override def forceSave(): IO[Core.Error.IO, Unit] =
    IO.unit
}
