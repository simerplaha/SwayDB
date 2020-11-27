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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.io.file

import java.nio.channels.{FileChannel, WritableByteChannel}
import java.nio.channels.FileChannel.MapMode
import java.nio.file.{Path, StandardOpenOption}
import java.nio.{BufferOverflowException, MappedByteBuffer}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.ByteBufferSweeper
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.data.Reserve
import swaydb.data.config.ForceSave
import swaydb.data.slice.Slice

import scala.annotation.tailrec

private[file] object MMAPFile {

  def write(path: Path,
            bufferSize: Long,
            blockCacheFileId: Long,
            deleteAfterClean: Boolean,
            forceSave: ForceSave.MMAPFiles)(implicit cleaner: ByteBufferSweeperActor,
                                            forceSaveApplier: ForceSaveApplier): MMAPFile =
    MMAPFile(
      path = path,
      channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW),
      mode = MapMode.READ_WRITE,
      bufferSize = bufferSize,
      blockCacheFileId = blockCacheFileId,
      deleteAfterClean = deleteAfterClean,
      forceSave = forceSave
    )

  def read(path: Path,
           blockCacheFileId: Long,
           deleteAfterClean: Boolean)(implicit cleaner: ByteBufferSweeperActor,
                                      forceSaveApplier: ForceSaveApplier): MMAPFile = {
    val channel = FileChannel.open(path, StandardOpenOption.READ)

    MMAPFile(
      path = path,
      channel = channel,
      mode = MapMode.READ_ONLY,
      bufferSize = channel.size(),
      blockCacheFileId = blockCacheFileId,
      deleteAfterClean = deleteAfterClean,
      forceSave = ForceSave.Off
    )
  }

  private def apply(path: Path,
                    channel: FileChannel,
                    mode: MapMode,
                    bufferSize: Long,
                    blockCacheFileId: Long,
                    deleteAfterClean: Boolean,
                    forceSave: ForceSave.MMAPFiles)(implicit cleaner: ByteBufferSweeperActor,
                                                    forceSaveApplier: ForceSaveApplier): MMAPFile = {
    val buff = channel.map(mode, 0, bufferSize)
    new MMAPFile(
      path = path,
      channel = channel,
      mode = mode,
      bufferSize = bufferSize,
      blockCacheSourceId = blockCacheFileId,
      deleteAfterClean = deleteAfterClean,
      forceSaveConfig = forceSave,
      buffer = buff
    )
  }
}

private[file] class MMAPFile(val path: Path,
                             val channel: FileChannel,
                             mode: MapMode,
                             bufferSize: Long,
                             val blockCacheSourceId: Long,
                             val deleteAfterClean: Boolean,
                             val forceSaveConfig: ForceSave.MMAPFiles,
                             @volatile private var buffer: MappedByteBuffer)(implicit cleaner: ByteBufferSweeperActor,
                                                                             forceSaveApplier: ForceSaveApplier) extends LazyLogging with DBFileType {

  //Force is applied on files after they are marked immutable so it only needs
  //to be invoked once.
  private val forced = {
    if (forceSaveConfig.enableForReadOnlyMode)
      new AtomicBoolean(false)
    else
      new AtomicBoolean(mode == MapMode.READ_ONLY)
  }

  private val open = new AtomicBoolean(true)
  //Increments on each request made to this object and decrements on request's end.
  private val referenceCount = new AtomicLong(0)

  override private[file] def writeableChannel: WritableByteChannel =
    channel

  /**
   * [[buffer]] is set to null for safely clearing it from the RAM. Setting it to null
   * will throw [[NullPointerException]] which should be recovered into typed busy error
   * [[swaydb.Error.NullMappedByteBuffer]] so this request will get retried.
   *
   * [[NullPointerException]] should not leak outside since it's not a known error.
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
        forceSaveApplier.beforeClose(this, forceSaveConfig)
        clearBuffer()
        channel.close()
      }
    else
      logger.trace("{}: Already closed.", path)

  //forceSave and clearBuffer are never called concurrently other than when the database is being shut down.
  //so there is no blocking cost for using synchronized here on than when this file is already submitted for cleaning on shutdown.
  def forceSave(): Unit =
    if (!isBufferEmpty && forced.compareAndSet(false, true))
      try
        watchNullPointer(buffer.force())
      catch {
        case failure: Throwable =>
          forced.set(false)
          logger.error("Unable to ForceSave", failure)
          throw failure
      }
    else
      logger.debug(s"ForceSave ignored MMAPFile - $path. Mode = ${mode.toString}. isBufferEmpty = $isBufferEmpty. isOpen = ${channel.isOpen}. forced = ${forced.get()}")

  private def clearBuffer(): Unit =
    synchronized {
      val swapBuffer = buffer
      //null the buffer so that all future read requests do not read this buffer.
      //the resulting NullPointerException will re-route request to the new Segment.
      //TO-DO: Use Option here instead. Test using Option does not have read performance impact.
      buffer = null

      cleaner.actor send
        ByteBufferSweeper.Command.Clean(
          buffer = swapBuffer,
          hasReference = hasReference,
          forced = forced,
          filePath = path,
          forceSave = forceSaveConfig
        )
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

  override def transfer(position: Int, count: Int, transferTo: DBFileType): Int =
    transferTo match {
      case _: ChannelFile =>
        //TODO - Is forceSave really required here? Can a buffer contain bytes that FileChannel is unaware of?
        this.forceSave()

        val transferred =
          Effect.transfer(
            position = position,
            count = count,
            from = channel,
            transferTo = transferTo.writeableChannel
          )

        assert(transferred == count, s"$transferred != $count")

        transferred

      case target: MMAPFile =>
        val duplicate = buffer.duplicate()
        duplicate.position(position)
        duplicate.limit(position + count)

        target.buffer.put(duplicate)
        count
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

  override def size =
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
