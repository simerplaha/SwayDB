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
import swaydb.core.file.sweeper.bytebuffer.ByteBufferCommand
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.effect.{Effect, Reserve}
import swaydb.slice.{Slice, SliceRO, Slices}

import java.nio.channels.FileChannel.MapMode
import java.nio.channels.{FileChannel, WritableByteChannel}
import java.nio.file.{Path, StandardOpenOption}
import java.nio.{BufferOverflowException, MappedByteBuffer}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.annotation.tailrec

private[file] object MMAPFile {

  def write(path: Path,
            bufferSize: Int,
            deleteAfterClean: Boolean,
            forceSave: ForceSave.MMAPFiles)(implicit cleaner: ByteBufferSweeperActor,
                                            forceSaveApplier: ForceSaveApplier): MMAPFile =
    MMAPFile(
      path = path,
      channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW),
      mode = MapMode.READ_WRITE,
      bufferSize = bufferSize,
      deleteAfterClean = deleteAfterClean,
      forceSave = forceSave
    )

  def read(path: Path,
           deleteAfterClean: Boolean)(implicit cleaner: ByteBufferSweeperActor,
                                      forceSaveApplier: ForceSaveApplier): MMAPFile = {
    val channel = FileChannel.open(path, StandardOpenOption.READ)

    MMAPFile(
      path = path,
      channel = channel,
      mode = MapMode.READ_ONLY,
      bufferSize = Effect.getIntFileSizeOrFail(channel),
      deleteAfterClean = deleteAfterClean,
      forceSave = ForceSave.Off
    )
  }

  private def apply(path: Path,
                    channel: FileChannel,
                    mode: MapMode,
                    bufferSize: Int,
                    deleteAfterClean: Boolean,
                    forceSave: ForceSave.MMAPFiles)(implicit cleaner: ByteBufferSweeperActor,
                                                    forceSaveApplier: ForceSaveApplier): MMAPFile = {
    val buff = channel.map(mode, 0, bufferSize)
    new MMAPFile(
      path = path,
      channel = channel,
      mode = mode,
      bufferSize = bufferSize,
      deleteAfterClean = deleteAfterClean,
      forceSaveConfig = forceSave,
      buffer = buff
    )
  }
}

private[file] class MMAPFile(val path: Path,
                             channel: FileChannel,
                             mode: MapMode,
                             bufferSize: Int,
                             val deleteAfterClean: Boolean,
                             val forceSaveConfig: ForceSave.MMAPFiles,
                             @volatile private var buffer: MappedByteBuffer)(implicit cleaner: ByteBufferSweeperActor,
                                                                             forceSaveApplier: ForceSaveApplier) extends LazyLogging with CoreFileType {

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
        ByteBufferCommand.Clean(
          buffer = swapBuffer,
          hasReference = hasReference _,
          forced = forced,
          filePath = path,
          forceSave = forceSaveConfig
        )
    }

  /**
   * Increments the current size of existing [[buffer]].
   */
  private def incrementBufferSize(increment: Int) = {
    val positionBeforeClear = buffer.position()
    buffer.force()
    clearBuffer()
    buffer = channel.map(mode, 0, positionBeforeClear + increment)
    buffer.position(positionBeforeClear)
  }

  override def appendBatch(slices: Array[Slice[Byte]]): Unit = {
    /**
     * Extends the buffer when if there is not enough room in the current mapped buffer.
     */
    @tailrec
    def run(fromIndex: Int): Unit = {
      var currentIndex = fromIndex
      try
        watchNullPointer[Unit] {
          while (currentIndex < slices.length) {
            buffer.put(slices(currentIndex).toByteBufferWrap)
            currentIndex += 1
          }
        }
      catch {
        case ex: BufferOverflowException =>
          watchNullPointer {
            //Although this code extends the buffer, currently there is no implementation that requires this feature.
            //All the bytes requires for each write operation are pre-calculated EXACTLY and an overflow should NEVER occur.

            //calculate the total number of bytes needed
            val requiredByteSize =
              (currentIndex until slices.length).foldLeft(0) {
                case (required, sliceIndex) =>
                  required + slices(sliceIndex).size
              }

            logger.error(
              "{}: BufferOverflowException. Required bytes: {}. Remaining bytes: {}. Extending buffer with {} bytes.",
              path, requiredByteSize, buffer.remaining(), requiredByteSize, ex
            )

            incrementBufferSize(requiredByteSize)
          }

          run(currentIndex)
      }
    }

    run(0)
  }

  @tailrec
  final def append(slice: Slice[Byte]): Unit =
    try
      watchNullPointer[Unit](buffer.put(slice.toByteBufferWrap))
    catch {
      case ex: BufferOverflowException =>
        watchNullPointer {
          //Although this code extends the buffer, currently there is no implementation that requires this feature.
          //All the bytes requires for each write operation are pre-calculated EXACTLY and an overflow should NEVER occur.
          val requiredByteSize = slice.size

          logger.error(
            "{}: BufferOverflowException. Required bytes: {}. Remaining bytes: {}. Extending buffer with {} bytes.",
            path, requiredByteSize, buffer.remaining(), requiredByteSize, ex
          )

          incrementBufferSize(requiredByteSize)
        }
        append(slice)
    }

  override def transfer(position: Int, count: Int, transferTo: CoreFileType): Int =
    transferTo match {
      case transferTo: StandardFile =>
        //TODO - Is forceSave really required here? Can a buffer contain bytes that FileChannel is unaware of?
        this.forceSave()

        val transferred =
          Effect.transfer(
            position = position,
            count = count,
            from = channel,
            transferTo = transferTo.channel
          )

        assert(transferred == count, s"$transferred != $count")

        transferred

      case transferTo: MMAPFile =>
        val duplicate = buffer.duplicate()
        duplicate.position(position)
        duplicate.limit(position + count)

        transferTo.buffer.put(duplicate)
        count
    }

  /**
   * Loads array from the [[buffer]] and does not watch for null pointer.
   */
  @inline private def loadFromBuffer(array: Array[Byte], position: Int): Unit = {
    val arrayLength = array.length
    var i = 0
    while (i < arrayLength) {
      array(i) = buffer.get(i + position)
      i += 1
    }
  }

  def read(position: Int, size: Int): Slice[Byte] =
    watchNullPointer {
      val array = new Array[Byte](size)
      loadFromBuffer(array = array, position = position)
      Slice.wrap(array)
    }

  def read(position: Int, size: Int, blockSize: Int): SliceRO[Byte] =
    if (size == 0) {
      Slice.emptyBytes //no need to have this as global val because core never asks for 0 size
    } else if (blockSize >= size) {
      read(position, size)
    } else {
      val buffersCount = size / blockSize //minimum buffers required
      val lastBufferLength = size % blockSize //last buffer size

      val slices =
        if (lastBufferLength == 0) //no overflow. Smaller last buffer not needed
          new Array[Slice[Byte]](buffersCount)
        else //needs another slice
          new Array[Slice[Byte]](buffersCount + 1)

      watchNullPointer {
        var i = 0
        val sliceCount = slices.length
        while (i < sliceCount) {
          val array =
            if (lastBufferLength != 0 && i == sliceCount - 1) //if last buffer is of a smaller size
              new Array[Byte](lastBufferLength)
            else
              new Array[Byte](blockSize)

          loadFromBuffer(array, position + (i * blockSize))
          slices(i) = Slice.wrap(array)

          i += 1
        }
      }

      Slices(slices)
    }

  def get(position: Int): Byte =
    watchNullPointer {
      buffer.get(position)
    }

  override def size: Int =
    watchNullPointer(Effect.getIntFileSizeOrFail(channel))

  override def readAll: Slice[Byte] =
    watchNullPointer(read(0, Effect.getIntFileSizeOrFail(channel)))

  override def isOpen =
    watchNullPointer(channel.isOpen)

  override def memoryMapped =
    true

  override def isLoaded: Boolean =
    watchNullPointer(buffer.isLoaded)

  override def isFull: Boolean =
    watchNullPointer(buffer.remaining() == 0)

  override def delete(): Unit =
    watchNullPointer {
      close()
      if (deleteAfterClean)
        cleaner.actor send ByteBufferCommand.DeleteFile(path)
      else
        Effect.delete(path)
    }

  private def hasReference(): Boolean =
    referenceCount.get() > 0

  def isBufferEmpty: Boolean =
    buffer == null
}
