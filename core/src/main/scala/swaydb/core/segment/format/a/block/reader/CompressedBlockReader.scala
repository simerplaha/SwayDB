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
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.{Block, BlockUpdater, SegmentBlock, ValuesBlock}
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

/**
  * Reader for the [[Block.CompressionInfo]] that skips [[Block.Header]] bytes.
  */
private[core] object CompressedBlockReader {

  def apply[B <: Block](reader: Reader, block: B): CompressedBlockReader[B] =
    new CompressedBlockReader[B](
      reader = reader,
      block = block
    )

  def unblockedValues(bytes: Slice[Byte]): IO[CompressedBlockReader[ValuesBlock]] =
    unblockedValues(Reader(bytes))

  def unblockedValues(reader: Reader): IO[CompressedBlockReader[ValuesBlock]] =
    reader.size map {
      readerSize =>
        new CompressedBlockReader(
          reader = reader,
          block = ValuesBlock(ValuesBlock.Offset(0, readerSize.toInt), 0, None)
        )
    }

  def unblockedSegment(bytes: Slice[Byte]): IO[CompressedBlockReader[SegmentBlock]] =
    unblockedSegment(Reader(bytes))

  def unblockedSegment(segmentReader: Reader): IO[CompressedBlockReader[SegmentBlock]] =
    segmentReader.size map {
      size =>
        CompressedBlockReader(
          reader = segmentReader,
          block = SegmentBlock(
            offset = SegmentBlock.Offset(0, size.toInt),
            headerSize = 0,
            compressionInfo = None
          )
        )
    }
}

private[core] class CompressedBlockReader[B <: Block](reader: Reader,
                                                      override val block: B) extends BlockReader[B](reader, block) with LazyLogging {
  override def moveTo(newPosition: Long): CompressedBlockReader[B] = {
    super.moveTo(newPosition)
    this
  }

  def readFullBlockAndGetBlockReader()(implicit blockUpdater: BlockUpdater[B]): IO[CompressedBlockReader[B]] =
    readAll()
      .map {
        bytes =>
          new CompressedBlockReader[B](
            reader = Reader(bytes),
            block = blockUpdater.updateOffset(block, 0, bytes.size)
          )
      }

  override def copy(): CompressedBlockReader[B] =
    new CompressedBlockReader(
      reader = reader.copy(),
      block = block
    )
}
