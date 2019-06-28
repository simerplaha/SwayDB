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
import swaydb.core.segment.format.a.block.Block
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

/**
  * Reader for the [[Block.CompressionInfo]] that skips [[Block.Header]] bytes.
  */
object BlockReader {
  def apply[B <: Block](segmentReader: Reader, block: B): BlockReader[B] =
    new BlockReader[B](
      segmentReader = segmentReader,
      block = block
    )
}

private[core] class BlockReader[B <: Block](segmentReader: Reader,
                                            val block: B) extends Reader with LazyLogging {

  private var position: Int = 0

  private def blockReader: IO[(Int, Reader)] =
    block
      .compressionInfo
      .map {
        compressionInfo =>
          Block.decompress(
            compressionInfo = compressionInfo,
            //do not copy, decompress already copies if required.
            segmentReader = segmentReader,
            offset = block.blockOffset
          ) map {
            decompressedBytes =>
              //decompressed bytes, offsets not required, set to 0.
              (0, Reader(decompressedBytes))
          }
      }
      .getOrElse {
        IO.Success((block.blockOffset.start + block.headerSize, segmentReader.copy())) //no compression used. Set the offset.
      }

  override val size: IO[Long] =
    IO.Success {
      block.
        compressionInfo
        .map(_.decompressedLength)
        .getOrElse(block.blockOffset.size - block.headerSize)
        .toLong
    }

  def moveTo(newPosition: Long): BlockReader[B] = {
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

  def hasAtLeast(fromPosition: Long, atLeastSize: Long): IO[Boolean] =
    size map {
      size =>
        (size - 1 - fromPosition) >= atLeastSize
    }

  override def copy(): BlockReader[B] =
    new BlockReader(
      segmentReader = segmentReader.copy(),
      block = block
    )

  override def getPosition: Int =
    position

  override def get() =
    hasMore flatMap {
      hasMore =>
        if (hasMore)
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
        else
          IO.Failure(IO.Error.Fatal(s"Has no more bytes. Position: $getPosition"))
    }

  override def read(size: Int) =
    blockReader flatMap {
      case (offset, reader) =>
        remaining flatMap {
          remaining =>
            val minimum = size min remaining.toInt
            reader
              .moveTo(offset + position)
              .read(minimum)
              .map {
                bytes =>
                  position += minimum
                  bytes
              }
        }
    }

  def readFullBlock(): IO[Slice[Byte]] =
    segmentReader
      .moveTo(block.blockOffset.start)
      .read(block.blockOffset.size)

  def readFullBlockAndGetReader(): IO[BlockReader[B]] =
    readFullBlock()
      .map {
        bytes =>
          BlockReader[B](
            segmentReader = Reader(bytes),
            block = block
          )
      }

  override def readRemaining(): IO[Slice[Byte]] =
    remaining flatMap read
}