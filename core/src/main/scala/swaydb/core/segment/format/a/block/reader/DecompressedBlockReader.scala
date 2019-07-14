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
import swaydb.data.slice.Reader

/**
  * A typed object that indicates that block is already decompressed and now is reading data bytes.
  *
  * [[Block.decompress]] creates the object and should be the only function that creates it.
  */

private[core] object DecompressedBlockReader {
  def emptyValuesBlock =
    new DecompressedBlockReader(
      reader = Reader.empty,
      block = ValuesBlock.empty
    )

  def apply[B <: Block](block: B,
                        readFullBlockIfUncompressed: Boolean,
                        segmentReader: DecompressedBlockReader[SegmentBlock])(implicit updater: BlockUpdater[B]) =
    Block.decompress(
      block = block,
      readFullBlockIfUncompressed = readFullBlockIfUncompressed,
      segmentReader = segmentReader
    )
}

private[core] class DecompressedBlockReader[B <: Block](reader: Reader,
                                                        override val block: B) extends BlockReader(reader, block) with LazyLogging {
  override def moveTo(newPosition: Long): DecompressedBlockReader[B] = {
    super.moveTo(newPosition)
    this
  }

  def readFullBlockAndGetBlockReader()(implicit blockUpdater: BlockUpdater[B]): IO[DecompressedBlockReader[B]] =
    readAll()
      .map {
        bytes =>
          new DecompressedBlockReader[B](
            reader = Reader(bytes),
            block = blockUpdater.updateOffset(block, 0, bytes.size)
          )
      }

  def copy(): DecompressedBlockReader[B] =
    new DecompressedBlockReader(
      reader = reader.copy(),
      block = block
    )
}
