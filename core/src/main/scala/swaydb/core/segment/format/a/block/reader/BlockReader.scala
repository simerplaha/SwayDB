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
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

protected trait BlockReader extends Reader with LazyLogging {

  private[reader] def reader: Reader

  def offset: BlockOffset

  def blockSize: Int

  private var position: Int = 0

  private val blockCache: BlockCache.State = BlockCache.create(0, Slice.emptyBytes)

  override val isFile: Boolean = reader.isFile

  override val size: IO[Long] =
    IO(offset.size)

  override def moveTo(position: Long): BlockReader = {
    this.position = position.toInt
    this
  }

  def hasMore: IO[Boolean] =
    hasAtLeast(1)

  def hasAtLeast(atLeastSize: Long): IO[Boolean] =
    hasAtLeast(position, atLeastSize)

  def hasAtLeast(fromPosition: Long, atLeastSize: Long): IO[Boolean] =
    size map {
      size =>
        (size - fromPosition) >= atLeastSize
    }

  override def getPosition: Int =
    position

  override def get(): IO[Int] =
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
            IO.Failure(IO.Error.Fatal(s"Has no more bytes. Position: $getPosition"))
      }

  def readFromCache(position: Int, size: Int): Slice[Byte] =
    if (isFile)
      BlockCache.read(position = position, size = size, state = blockCache)
    else
      Slice.emptyBytes

  override def read(size: Int): IO[Slice[Byte]] = {
    val fromCache = readFromCache(position, size)
    if (size <= fromCache.size)
      IO {
        logger.debug(s"${this.hashCode()}: Seek from cache: ${fromCache.size}.bytes")
        position += size
        fromCache take size
      }
    else
      remaining flatMap {
        remaining =>

          //adjust the seek size to be a multiple of blockSize.
          val sizeToSeek = blockSize.toDouble * Math.ceil(Math.abs((size - fromCache.size) / blockSize.toDouble))
          //read the blockSize if there are enough bytes or else only read only the remaining.
          val canReadSize = sizeToSeek.toInt min (remaining.toInt - fromCache.size)
          //skip bytes already read from the blockCache.
          val nextReadPosition = offset.start + position + fromCache.size

          reader
            .moveTo(nextReadPosition)
            .read(canReadSize)
            .map {
              bytes =>

                /**
                  * [[size]] can be larger than blockSize. If the seeks are smaller than [[blockSize]]
                  * then cache the entire bytes since it's known that these bytes will be cached.
                  * If seeks are too large then cache only the extra tail bytes read to complete the block.
                  */
                if (isFile) {
                  logger.debug(s"${this.hashCode()}: Seek from disk: ${bytes.size}.bytes")
                  if (bytes.size <= blockSize)
                    BlockCache.set(nextReadPosition, bytes, blockCache)
                  else
                    BlockCache.set(nextReadPosition + size, bytes.drop(size).unslice(), blockCache)
                }

                position += (size min remaining.toInt)

                if (fromCache.isEmpty)
                  bytes take size
                else
                  fromCache ++ bytes.take(size - fromCache.size)
            }
      }
  }

  def readAll(): IO[Slice[Byte]] =
    reader
      .moveTo(offset.start)
      .read(offset.size)

  def readAllOrNone(): IO[Option[Slice[Byte]]] =
    if (offset.size == 0)
      IO.none
    else
      readAll().map(Some(_))

  override def readRemaining(): IO[Slice[Byte]] =
    remaining flatMap read
}
