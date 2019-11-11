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
import swaydb.data.slice.Slice

private[file] object ChannelFile {
  def write(path: Path,
            blockCacheFileId: Long): ChannelFile = {
    val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    new ChannelFile(
      path = path,
      channel = channel,
      blockCacheFileId = blockCacheFileId
    )
  }

  def read(path: Path,
           blockCacheFileId: Long): ChannelFile =
    if (Effect.exists(path)) {
      val channel = FileChannel.open(path, StandardOpenOption.READ)
      new ChannelFile(
        path = path,
        channel = channel,
        blockCacheFileId = blockCacheFileId
      )
    }
    else
      throw swaydb.Exception.NoSuchFile(path)
}

private[file] class ChannelFile(val path: Path,
                                channel: FileChannel,
                                val blockCacheFileId: Long) extends LazyLogging with DBFileType {

  def close: Unit =
  //      logger.info(s"$path: Closing channel")
    channel.close()

  def append(slice: Slice[Byte]): Unit =
    Effect.writeUnclosed(channel, slice)

  def append(slice: Iterable[Slice[Byte]]): Unit =
    Effect.writeUnclosed(channel, slice)

  def read(position: Int, size: Int): Slice[Byte] = {
    val buffer = ByteBuffer.allocate(size)
    channel.read(buffer, position)
    Slice(buffer.array())
  }

  def read(position: Int, size: Int, slice: Slice[Byte]): Unit = {
    val buffer = slice.toByteBufferWrap
    channel.read(buffer, position)
  }

  def get(position: Int): Byte =
    read(position, 1).head

  def readAll: Slice[Byte] = {
    val bytes = new Array[Byte](channel.size().toInt)
    channel.read(ByteBuffer.wrap(bytes))
    Slice(bytes)
  }

  def fileSize: Long =
    channel.size()

  override def isOpen =
    channel.isOpen

  override def isMemoryMapped =
    false

  override def isLoaded =
    false

  override def isFull =
    false

  override def delete(): Unit = {
    close
    Effect.delete(path)
  }

  override def forceSave(): Unit =
    ()
}
