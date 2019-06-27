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
import swaydb.core.segment.format.a.block.Block
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

/**
  * Reader for the [[Block.CompressionInfo]] that skips [[Block.Header]] bytes.
  */
private[core] class BlockReader(compressedReader: Reader,
                                offset: OffsetBase,
                                headerSize: Int,
                                compressionInfo: Option[Block.CompressionInfo]) extends Reader with LazyLogging {

  private var position: Int = 0

  private def blockReader: IO[(Int, Reader)] =
    compressionInfo
      .map {
        block =>
          Block.decompress(
            compressionInfo = block,
            compressedReader = compressedReader.copy(),
            offset = offset
          ) map {
            decompressedBytes =>
              //decompressed bytes, offsets not required, set to 0.
              (0, Reader(decompressedBytes))
          }
      }
      .getOrElse {
        IO.Success((offset.start + headerSize, compressedReader.copy())) //no compression used. Set the offset.
      }

  //this is the size of the
  override def size: IO[Long] =
    IO.Success {
      compressionInfo
        .map(_.decompressedLength)
        .getOrElse(offset.size - headerSize)
        .toLong
    }

  def moveTo(newPosition: Long): Reader = {
    position = newPosition.toInt
    this
  }

  def hasMore: IO[Boolean] =
    hasAtLeast(1)

  def hasAtLeast(atLeastSize: Long): IO[Boolean] =
    size map {
      size =>
        (size - 1 - position) >= atLeastSize
    }

  override def copy(): BlockReader =
    new BlockReader(
      compressedReader = compressedReader.copy(),
      offset = offset,
      headerSize = headerSize,
      compressionInfo = compressionInfo
    )

  override def getPosition: Int =
    position

  override def get() =
    blockReader flatMap {
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
    blockReader flatMap {
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