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

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.{Path, StandardOpenOption}
import java.nio.{BufferOverflowException, MappedByteBuffer}
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.IO.ErrorHandler
import swaydb.IO
import swaydb.IO._
import swaydb.data.Reserve
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._

import scala.annotation.tailrec

private[file] object MMAPFile {

  def read(path: Path): IO[swaydb.Error.IO, MMAPFile] =
    IO(FileChannel.open(path, StandardOpenOption.READ)) flatMap {
      channel =>
        MMAPFile(
          path = path,
          channel = channel,
          mode = MapMode.READ_ONLY,
          bufferSize = channel.size()
        )
    }

  def write(path: Path,
            bufferSize: Long): IO[swaydb.Error.IO, MMAPFile] =
    IO(FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) flatMap {
      channel =>
        MMAPFile(
          path = path,
          channel = channel,
          mode = MapMode.READ_WRITE,
          bufferSize = bufferSize
        )
    }

  private def apply(path: Path,
                    channel: FileChannel,
                    mode: MapMode,
                    bufferSize: Long): IO[swaydb.Error.IO, MMAPFile] =
    IO {
      val buff = channel.map(mode, 0, bufferSize)
      new MMAPFile(
        path = path,
        channel = channel,
        mode = mode,
        bufferSize = bufferSize,
        buffer = buff
      )
    }
}

private[file] class MMAPFile(val path: Path,
                             channel: FileChannel,
                             mode: MapMode,
                             bufferSize: Long,
                             @volatile private var buffer: MappedByteBuffer) extends LazyLogging with DBFileType {

  private val open = new AtomicBoolean(true)

  /**
    * [[buffer]] is set to null for safely clearing it from the RAM. Setting it to null
    * will throw [[NullPointerException]] which should be recovered into typed busy error
    * [[swaydb.Error.NullMappedByteBuffer]] so this request will get retried.
    *
    * [[NullPointerException]] should not leak outside.
    *
    * FIXME - Switch to using Option.
    */
  def recoverFromNullPointer[T](f: => T): IO[swaydb.Error.IO, T] =
    IO[swaydb.Error.IO, T](f) recoverWith {
      case swaydb.Error.Unknown(ex: NullPointerException) =>
        IO.Failure(swaydb.Error.NullMappedByteBuffer(swaydb.Exception.NullMappedByteBuffer(ex, Reserve(name = s"${this.getClass.getSimpleName}: $path"))))

      case other =>
        IO.Failure(other)
    }

  def close(): IO[swaydb.Error.IO, Unit] =
  //    logger.info(s"$path: Closing channel")
    if (open.compareAndSet(true, false)) {
      recoverFromNullPointer {
        forceSave()
        clearBuffer()
        channel.close()
      }
    } else {
      logger.trace("{}: Already closed.", path)
      IO.unit
    }

  //forceSave and clearBuffer are never called concurrently other than when the database is being shut down.
  //so there is no blocking cost for using synchronized here on than when this file is already submitted for cleaning on shutdown.
  def forceSave(): IO[swaydb.Error.IO, Unit] =
    synchronized {
      if (mode == MapMode.READ_ONLY || isBufferEmpty)
        IO.unit
      else
        recoverFromNullPointer(buffer.force())
    }

  private def clearBuffer(): Unit =
    synchronized {
      val swapBuffer = buffer
      //null the buffer so that all future read requests do not read this buffer.
      //the resulting NullPointerException will re-route request to the new Segment.
      //TO-DO: Use Option here instead. Test using Option does not have read performance impact.
      buffer = null
      BufferCleaner.clean(swapBuffer, path)
    }

  private def extendBuffer(bufferSize: Long): IO[swaydb.Error.IO, Unit] =
    recoverFromNullPointer {
      val positionBeforeClear = buffer.position()
      buffer.force()
      clearBuffer()
      buffer = channel.map(mode, 0, positionBeforeClear + bufferSize)
      buffer.position(positionBeforeClear)
    }

  override def append(slice: Iterable[Slice[Byte]]): IO[swaydb.Error.IO, Unit] =
    (slice foreachIO append) getOrElse IO.unit

  @tailrec
  final def append(slice: Slice[Byte]): IO[swaydb.Error.IO, Unit] =
    recoverFromNullPointer[Unit](buffer.put(slice.toByteBufferWrap)) match {
      case success: IO.Success[_, _] =>
        success

      //Although this code extends the buffer, currently there is no implementation that requires this feature.
      //All the bytes requires for each write operation are pre-calculated EXACTLY and an overflow should NEVER occur.
      case IO.Failure(swaydb.Error.Unknown(ex: BufferOverflowException)) =>
        val requiredByteSize = slice.size.toLong
        logger.debug("{}: BufferOverflowException. Required bytes: {}. Remaining bytes: {}. Extending buffer with {} bytes.",
          path, requiredByteSize, buffer.remaining(), requiredByteSize, ex)

        val result = extendBuffer(requiredByteSize)
        if (result.isSuccess)
          append(slice)
        else
          result

      case failure: IO.Failure[_, _] =>
        failure
    }

  def read(position: Int, size: Int): IO[swaydb.Error.IO, Slice[Byte]] =
    recoverFromNullPointer {
      val array = new Array[Byte](size)
      //      buffer position position
      //      buffer get array
      var i = 0
      while (i < size) {
        array(i) = buffer.get(i + position)
        i += 1
      }
      Slice(array)
    }

  def get(position: Int): IO[swaydb.Error.IO, Byte] =
    recoverFromNullPointer {
      buffer.get(position)
    }

  override def fileSize =
    recoverFromNullPointer(channel.size())

  override def readAll: IO[swaydb.Error.IO, Slice[Byte]] =
    read(0, channel.size().toInt)

  override def isOpen =
    channel.isOpen

  override def isMemoryMapped =
    IO.`true`

  override def isLoaded: IO[swaydb.Error.IO, Boolean] =
    recoverFromNullPointer(buffer.isLoaded)

  override def isFull: IO[swaydb.Error.IO, Boolean] =
    recoverFromNullPointer(buffer.remaining() == 0)

  override def memory: Boolean = false

  override def delete(): IO[swaydb.Error.IO, Unit] =
    close flatMap {
      _ =>
        IOEffect.delete(path)
    }

  def isBufferEmpty: Boolean =
    buffer == null
}
