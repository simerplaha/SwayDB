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

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.config.ForceSave
import swaydb.data.slice.Slice._
import swaydb.data.slice.Slice

private[file] object ChannelFile {
  def write(path: Path,
            blockCacheFileId: Long,
            forceSave: ForceSave.ChannelFiles)(implicit forceSaveApplier: ForceSaveApplier): ChannelFile = {
    val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    new ChannelFile(
      path = path,
      mode = StandardOpenOption.WRITE,
      channel = channel,
      forceSave = forceSave,
      blockCacheFileId = blockCacheFileId
    )
  }

  def read(path: Path,
           blockCacheFileId: Long)(implicit forceSaveApplier: ForceSaveApplier): ChannelFile =
    if (Effect.exists(path)) {
      val channel = FileChannel.open(path, StandardOpenOption.READ)
      new ChannelFile(
        path = path,
        mode = StandardOpenOption.READ,
        channel = channel,
        forceSave = ForceSave.Disabled,
        blockCacheFileId = blockCacheFileId
      )
    }
    else
      throw swaydb.Exception.NoSuchFile(path)
}

private[file] class ChannelFile(val path: Path,
                                mode: StandardOpenOption,
                                channel: FileChannel,
                                forceSave: ForceSave.ChannelFiles,
                                val blockCacheFileId: Long)(implicit forceSaveApplied: ForceSaveApplier) extends LazyLogging with DBFileType {

  //Force is applied on files after they are marked immutable so it only needs
  //to be invoked once.
  private val forced = {
    if (forceSave.enableForReadOnlyMode)
      new AtomicBoolean(false)
    else
      new AtomicBoolean(mode == StandardOpenOption.READ)
  }

  def close(): Unit = {
    forceSaveApplied.beforeClose(this, forceSave)
    channel.close()
  }

  def append(slice: Slice[Byte]): Unit =
    Effect.writeUnclosed(channel, slice)

  def append(slice: Iterable[Slice[Byte]]): Unit =
    Effect.writeUnclosed(channel, slice)

  def read(position: Int, size: Int): Slice[Byte] = {
    val buffer = ByteBuffer.allocate(size)
    channel.read(buffer, position)
    Slice(buffer.array())
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
    close()
    Effect.delete(path)
  }

  override def forceSave(): Unit =
    if (channel.isOpen && forced.compareAndSet(false, true))
      try
        channel.force(false)
      catch {
        case failure: Throwable =>
          forced.set(false)
          logger.error("Unable to ForceSave", failure)
          throw failure
      }
    else
      logger.debug(s"ForceSave ignored FileChannel - $path. Mode = ${mode.toString}. isOpen = ${channel.isOpen}. forced = ${forced.get()}")
}
