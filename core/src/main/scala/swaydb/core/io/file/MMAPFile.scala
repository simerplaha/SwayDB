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

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.{Path, StandardOpenOption}
import java.nio.{BufferOverflowException, MappedByteBuffer}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.actor.ByteBufferSweeper
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.data.Reserve
import swaydb.data.slice.Slice

import scala.annotation.tailrec

private[file] object MMAPFile {

  def write(path: Path,
            bufferSize: Long,
            blockCacheFileId: Long,
            deleteAfterClean: Boolean)(implicit cleaner: ByteBufferSweeperActor): MMAPFile =
    MMAPFile(
      path = path,
      channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW),
      mode = MapMode.READ_WRITE,
      bufferSize = bufferSize,
      blockCacheFileId = blockCacheFileId,
      deleteAfterClean = deleteAfterClean
    )

  def read(path: Path,
           blockCacheFileId: Long,
           deleteAfterClean: Boolean)(implicit cleaner: ByteBufferSweeperActor): MMAPFile = {
    val channel = FileChannel.open(path, StandardOpenOption.READ)

    MMAPFile(
      path = path,
      channel = channel,
      mode = MapMode.READ_ONLY,
      bufferSize = channel.size(),
      blockCacheFileId = blockCacheFileId,
      deleteAfterClean = deleteAfterClean
    )
  }

  private def apply(path: Path,
                    channel: FileChannel,
                    mode: MapMode,
                    bufferSize: Long,
                    blockCacheFileId: Long,
                    deleteAfterClean: Boolean)(implicit cleaner: ByteBufferSweeperActor): MMAPFile = {
    val buff = channel.map(mode, 0, bufferSize)
    new MMAPFile(
      path = path,
      channel = channel,
      mode = mode,
      bufferSize = bufferSize,
      blockCacheFileId = blockCacheFileId,
      deleteAfterClean = deleteAfterClean,
      buffer = buff
    )
  }
}

private[file] class MMAPFile(val path: Path,
                             channel: FileChannel,
                             mode: MapMode,
                             bufferSize: Long,
                             val blockCacheFileId: Long,
                             val deleteAfterClean: Boolean,
                             @volatile private var buffer: MappedByteBuffer)(implicit cleaner: ByteBufferSweeperActor) extends LazyLogging with DBFileType {

  private val open = new AtomicBoolean(true)
  private val referenceCount = new AtomicLong(0)

  /**
   * [[buffer]] is set to null for safely clearing it from the RAM. Setting it to null
   * will throw [[NullPointerException]] which should be recovered into typed busy error
   * [[swaydb.Error.NullMappedByteBuffer]] so this request will get retried.
   *
   * [[NullPointerException]] should not leak outside.
   *
   * FIXME - Switch to using Option.
   */

  @inline private final def watchNullPointer[T](f: => T): T =
    try {
      referenceCount.incrementAndGet()
      f
    } catch {
      case ex: NullPointerException =>
        throw swaydb.Exception.NullMappedByteBuffer(ex, Reserve.free(name = s"${this.getClass.getSimpleName}: $path"))
    } finally {
      referenceCount.decrementAndGet()
    }

  def close(): Unit =
    if (open.compareAndSet(true, false))
      watchNullPointer {
        forceSave()
        clearBuffer()
        channel.close()
      }
    else
      logger.trace("{}: Already closed.", path)

  //forceSave and clearBuffer are never called concurrently other than when the database is being shut down.
  //so there is no blocking cost for using synchronized here on than when this file is already submitted for cleaning on shutdown.
  def forceSave(): Unit =
    synchronized {
      if (mode == MapMode.READ_ONLY || isBufferEmpty)
        IO.unit
      else
        watchNullPointer(buffer.force())
    }

  private def clearBuffer(): Unit =
    synchronized {
      val swapBuffer = buffer
      //null the buffer so that all future read requests do not read this buffer.
      //the resulting NullPointerException will re-route request to the new Segment.
      //TO-DO: Use Option here instead. Test using Option does not have read performance impact.
      buffer = null
      cleaner.actor send ByteBufferSweeper.Command.Clean(swapBuffer, hasReference, path)
    }

  override def append(slice: Iterable[Slice[Byte]]): Unit =
    slice foreach append

  @tailrec
  final def append(slice: Slice[Byte]): Unit =
    try
      watchNullPointer[Unit](buffer.put(slice.toByteBufferWrap))
    catch {
      case ex: BufferOverflowException =>
        watchNullPointer {
          //Although this code extends the buffer, currently there is no implementation that requires this feature.
          //All the bytes requires for each write operation are pre-calculated EXACTLY and an overflow should NEVER occur.
          val requiredByteSize = slice.size.toLong
          logger.debug("{}: BufferOverflowException. Required bytes: {}. Remaining bytes: {}. Extending buffer with {} bytes.",
            path, requiredByteSize, buffer.remaining(), requiredByteSize, ex)

          val positionBeforeClear = buffer.position()
          buffer.force()
          clearBuffer()
          buffer = channel.map(mode, 0, positionBeforeClear + requiredByteSize)
          buffer.position(positionBeforeClear)
        }
        append(slice)
    }

  def read(position: Int, size: Int): Slice[Byte] =
    watchNullPointer {
      val array = new Array[Byte](size)
      var i = 0
      while (i < size) {
        array(i) = buffer.get(i + position)
        i += 1
      }
      Slice(array)
    }

  def get(position: Int): Byte =
    watchNullPointer {
      buffer.get(position)
    }

  override def fileSize =
    watchNullPointer(channel.size())

  override def readAll: Slice[Byte] =
    watchNullPointer(read(0, channel.size().toInt))

  override def isOpen =
    watchNullPointer(channel.isOpen)

  override def isMemoryMapped =
    true

  override def isLoaded: Boolean =
    watchNullPointer(buffer.isLoaded)

  override def isFull: Boolean =
    watchNullPointer(buffer.remaining() == 0)

  override def delete(): Unit =
    watchNullPointer {
      close()
      if (deleteAfterClean)
        cleaner.actor send ByteBufferSweeper.Command.DeleteFile(path)
      else
        Effect.delete(path)
    }

  private def hasReference(): Boolean =
    referenceCount.get() > 0

  def isBufferEmpty: Boolean =
    buffer == null
}
