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

package swaydb.core.segment.format.a.block.reader

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ErrorHandler
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.slice.{Reader, ReaderBase, Slice}
import swaydb.{Error, IO}

protected trait BlockReader extends ReaderBase[swaydb.Error.Segment] with LazyLogging {

  def offset: BlockOffset

  def blockSize: Int

  private[reader] def reader: Reader[swaydb.Error.Segment]

  override val isFile: Boolean = reader.isFile

  private var position: Int = 0

  private var previousReadEndPosition = position

  private val cache: BlockReaderCache.State = BlockReaderCache.init(0, Slice.emptyBytes)

  override val size: IO[swaydb.Error.Segment, Long] =
    IO(offset.size)

  override def moveTo(position: Long): BlockReader = {
    this.position = position.toInt
    this
  }

  def hasMore: IO[swaydb.Error.Segment, Boolean] =
    hasAtLeast(1)

  def hasAtLeast(atLeastSize: Long): IO[swaydb.Error.Segment, Boolean] =
    hasAtLeast(position, atLeastSize)

  def hasAtLeast(fromPosition: Long, atLeastSize: Long): IO[swaydb.Error.Segment, Boolean] =
    size map {
      size =>
        (size - fromPosition) >= atLeastSize
    }

  override def getPosition: Int =
    position

  def cacheSize = cache.size

  def cachedBytes = cache.bytes

  override def get(): IO[swaydb.Error.Segment, Int] =
    if (isFile)
      read(1).map(_.head)
    else
      hasMore flatMap {
        hasMore =>
          if (hasMore)
            reader
              .moveTo(offset.start + position)
              .get()
              .map {
                got =>
                  position += 1
                  got
              }
          else
            IO.Failure(swaydb.Error.Unknown(s"Has no more bytes. Position: $getPosition"))
      }

  def readFromCache(position: Int, size: Int): Slice[Byte] =
    if (isFile)
      BlockReaderCache.read(position = position, size = size, state = cache)
    else
      Slice.emptyBytes

  def isSequentialRead(): Boolean =
    previousReadEndPosition == 0 || {
      val diff = position - previousReadEndPosition
      diff >= -this.blockSize && diff <= this.blockSize
    }

  def readRandomAccess(size: Int): IO[Error.Segment, Slice[Byte]] =
    remaining flatMap {
      remaining =>
        if (remaining <= 0) {
          IO.emptyBytes
        } else {
          val bytesToRead = size min remaining.toInt
          reader
            .moveTo(offset.start + position)
            .read(bytesToRead)
            .map {
              bytes =>
                val actualSize = size min remaining.toInt
                position += actualSize
                previousReadEndPosition = position - 1
                bytes
            }
        }
    }

  def readSequentialAccess(size: Int, fromCache: Slice[Byte]): IO[Error.Segment, Slice[Byte]] =
    if (size <= fromCache.size)
      IO {
        logger.debug(s"${this.getClass.getName} #${this.hashCode()}: Path: ${reader.path}, ${offset.getClass.getSimpleName}: Seek from cache: ${fromCache.size}.bytes")
        position += size
        fromCache take size
      }
    else
      remaining flatMap {
        remaining =>
          if (remaining <= 0) {
            IO.emptyBytes
          } else {
            //if reads are random do not read full block size lets the reads below decide how much to read.
            //read full block on random reads is very slow.
            //adjust the seek size to be a multiple of blockSize.
            //also check if there are too many cache misses.
            val blockSizeToRead =
            if (blockSize <= 0)
              size
            else
              blockSize.toDouble * Math.ceil(Math.abs((size - fromCache.size) / blockSize.toDouble))
            //read the blockSize if there are enough bytes or else only read only the remaining.
            val actualBlockReadSize = blockSizeToRead.toInt min (remaining.toInt - fromCache.size)
            //skip bytes already read from the blockCache.
            val nextBlockReadPosition = offset.start + position + fromCache.size

            reader
              .moveTo(nextBlockReadPosition)
              .read(actualBlockReadSize)
              .map {
                bytes =>

                  /**
                   * [[size]] can be larger than [[blockSize]]. If the seeks are smaller than [[blockSize]]
                   * then cache the entire bytes since it's known that a minimum of [[blockSize]] is allowed to be cached.
                   * If seeks are too large then cache only the extra tail bytes which are currently un-read by the client.
                   */
                  if (blockSize > 0) {
                    logger.debug(s"${this.getClass.getName} #${this.hashCode()}: Path: ${reader.path}, ${offset.getClass.getSimpleName}: ${nextBlockReadPosition} Seek from disk: ${bytes.size}.bytes.")
                    if (bytes.size <= blockSize)
                      BlockReaderCache.set(nextBlockReadPosition - offset.start, bytes, cache)
                    else
                      BlockReaderCache.set(nextBlockReadPosition - offset.start + size, bytes.drop(size).unslice(), cache)
                  }

                  val actualSize = size min remaining.toInt
                  position += actualSize

                  previousReadEndPosition = position - 1

                  if (fromCache.isEmpty)
                    bytes take size
                  else
                    fromCache ++ bytes.take(actualSize - fromCache.size)
              }
          }
      }

  override def read(size: Int): IO[swaydb.Error.Segment, Slice[Byte]] = {
    var fromCache = Slice.emptyBytes
    //@formatter:off
    if ((isFile && isSequentialRead()) || {fromCache = readFromCache(position, size); fromCache.nonEmpty})
      readSequentialAccess(size, readFromCache(position, size))
    else
      readRandomAccess(size)
    //@formatter:on
  }

  def readFullBlock(): IO[swaydb.Error.Segment, Slice[Byte]] =
    reader
      .moveTo(offset.start)
      .read(offset.size)

  def readFullBlockOrNone(): IO[swaydb.Error.Segment, Option[Slice[Byte]]] =
    if (offset.size == 0)
      IO.none
    else
      readFullBlock().map(Some(_))

  override def readRemaining(): IO[swaydb.Error.Segment, Slice[Byte]] =
    remaining flatMap read
}
