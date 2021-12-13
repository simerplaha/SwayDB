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
import swaydb.config.ForceSave
import swaydb.effect.Effect
import swaydb.slice.{Slice, SliceRO, Slices}
import swaydb.utils.Collections

import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, GatheringByteChannel}
import java.nio.file.{Path, StandardOpenOption}
import java.util.concurrent.atomic.AtomicBoolean

private object StandardFile {

  def write(path: Path,
            forceSave: ForceSave.StandardFiles)(implicit forceSaveApplier: ForceSaveApplier): StandardFile =
    new StandardFile(
      path = path,
      mode = StandardOpenOption.WRITE,
      channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW),
      forceSave = forceSave
    )

  def read(path: Path)(implicit forceSaveApplier: ForceSaveApplier): StandardFile =
    if (Effect.exists(path))
      new StandardFile(
        path = path,
        mode = StandardOpenOption.READ,
        channel = FileChannel.open(path, StandardOpenOption.READ),
        forceSave = ForceSave.Off
      )
    else
      throw swaydb.Exception.NoSuchFile(path)
}

private class StandardFile(val path: Path,
                           mode: StandardOpenOption,
                           private[file] val channel: FileChannel,
                           forceSave: ForceSave.StandardFiles)(implicit forceSaveApplied: ForceSaveApplier) extends LazyLogging with CoreFileType {

  //Force is applied on files after they are marked immutable so it only needs to be invoked once.
  private val forced =
    if (forceSave.enableForReadOnlyMode)
      new AtomicBoolean(false)
    else
      new AtomicBoolean(mode == StandardOpenOption.READ)

  def close(): Unit = {
    forceSaveApplied.beforeClose(this, forceSave)
    channel.close()
  }

  def append(slice: Slice[Byte]): Unit =
    Effect.writeUnclosed(channel, slice.toByteBufferWrap)

  def appendBatch(slices: Array[Slice[Byte]]): Unit = {
    var totalBytes = 0

    val buffers = new Array[ByteBuffer](slices.length)
    var index = 0
    while (index < slices.length) {
      val slice = slices(index)
      totalBytes += slice.size
      buffers(index) = slice.toByteBufferWrap
      index += 1
    }

    Effect.writeUnclosedGathering(channel, totalBytes, buffers)
  }

  override def transfer(position: Int, count: Int, transferTo: CoreFileType): Int =
    transferTo match {
      case transferTo: StandardFile =>
        Effect.transfer(
          position = position,
          count = count,
          from = channel,
          transferTo = transferTo.channel
        )

      case transferTo: MMAPFile =>
        val bytes = read(position = position, size = count)
        transferTo.append(bytes)
        bytes.size
    }

  def read(position: Int, size: Int): Slice[Byte] = {
    val buffer = ByteBuffer.allocate(size)
    //no need for synchronized since this does not update channel's position
    channel.read(buffer, position)
    Slice.wrap(buffer.array())
  }

  def read(position: Int, size: Int, blockSize: Int): SliceRO[Byte] =
    if (size == 0) {
      Slice.emptyBytes //no need to have this as global val because core never asks for 0 size
    } else if (blockSize >= size) {
      read(position, size)
    } else {
      val buffersCount = size / blockSize //minimum buffers required
      val lastBufferLength = size % blockSize //last buffer size

      val buffers =
        if (lastBufferLength == 0) //no overflow
          Array.fill(buffersCount)(ByteBuffer.allocate(blockSize))
        else //last buffer will be of smaller size
          Collections.fillArrayWithLast(ByteBuffer.allocate(lastBufferLength), buffersCount)(ByteBuffer.allocate(blockSize))

      //TODO review these synchronized blocks. For this functions it seems we need these to avoid
      //     channel's position being updated concurrently.
      this.synchronized {
        channel.position(position)
        channel.read(buffers) //read data
      }

      Slices(buffers.map(buffer => Slice.wrap(buffer))) //create slices
    }

  def get(position: Int): Byte =
    read(position, 1).head

  def readAll(): Slice[Byte] = {
    val bytes = new Array[Byte](Effect.getIntFileSizeOrFail(channel))
    channel.read(ByteBuffer.wrap(bytes), 0)
    Slice.wrap(bytes)
  }

  def size(): Int =
    Effect.getIntFileSizeOrFail(channel)

  override def isOpen: Boolean =
    channel.isOpen

  override def memoryMapped: Boolean =
    false

  override def isLoaded(): Boolean =
    false

  override def isFull(): Boolean =
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
