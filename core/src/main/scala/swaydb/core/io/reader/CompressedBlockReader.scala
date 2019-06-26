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

package swaydb.core.io.reader

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.segment.format.a.OffsetBase
import swaydb.core.segment.format.a.block.BlockCompression
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

private[core] class CompressedBlockReader(reader: Reader,
                                          offset: OffsetBase,
                                          headerSize: Int,
                                          blockCompression: Option[BlockCompression.State]) extends Reader with LazyLogging {

  private var position: Int = 0

  def block: IO[(Int, Reader)] =
    blockCompression
      .map {
        blockCompression =>
          BlockCompression.getDecompressedReader(
            blockCompression = blockCompression,
            compressedReader = reader.copy(),
            offset = offset
          ) map ((0, _)) //decompressed bytes, offsets not required, set to 0.
      }
      .getOrElse {
        IO.Success((offset.start + headerSize, reader.copy())) //no compression used. Set the offset.
      }

  override def size: IO[Long] =
    IO.Success(offset.size - headerSize)

  def moveTo(newPosition: Long): Reader = {
    position = newPosition.toInt
    this
  }

  def hasMore: IO[Boolean] =
    IO.Success(position <= offset.end)

  def hasAtLeast(atLeastSize: Long): IO[Boolean] =
    size map {
      size =>
        (size - position) >= atLeastSize
    }

  override def copy(): CompressedBlockReader =
    new CompressedBlockReader(
      reader = reader.copy(),
      offset = offset,
      headerSize = headerSize,
      blockCompression = blockCompression
    )

  override def getPosition: Int =
    position

  override def get() =
    block flatMap {
      case (offset, reader) =>
        reader
          .moveTo(offset + position)
          .get()
          .map {
            got =>
              position += 1
              got
          }
    }

  override def read(size: Int) =
    block flatMap {
      case (offset, reader) =>
        reader
          .moveTo(offset + position)
          .read(size)
          .map {
            bytes =>
              position += size
              bytes
          }
    }

  override def readRemaining(): IO[Slice[Byte]] =
    remaining flatMap read
}