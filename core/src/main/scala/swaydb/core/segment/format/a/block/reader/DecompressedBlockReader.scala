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
import swaydb.core.segment.format.a.block.{Block, BlockUpdater, SegmentBlock}
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

/**
  * A typed object that indicates that block is already decompressed and now is reading data bytes.
  *
  * [[Block.decompress]] creates the object and should be the only function that creates it.
  */

private[core] object DecompressedBlockReader {

  def empty[B <: Block](block: B)(implicit blockUpdater: BlockUpdater[B]) =
    new DecompressedBlockReader[B](
      reader = Reader.empty,
      block = blockUpdater.updateOffset(block, 0, 0)
    )

  /**
    * Returns reader for decompressed bytes.
    *
    * @param block - the offset will get updated to the decompressed bytes.
    */
  def decompressed[B <: Block](block: B, decompressedBytes: Slice[Byte]): DecompressedBlockReader[B] =
    new DecompressedBlockReader[B](
      reader = Reader(decompressedBytes),
      block = block
    )

  /**
    * Returns reader for decompressed bytes.
    *
    * @param block - the offset will get updated to the decompressed bytes.
    */
  def decompressed[B <: Block](block: B,
                               reader: DecompressedBlockReader[SegmentBlock]): DecompressedBlockReader[B] =
    new DecompressedBlockReader[B](
      reader = reader.copy(),
      block = block
    )

  /**
    * Decompressed parent readers are always required for child blocks to read from.
    * But for root readers the parent readers are non-existent so here an unblocked [[DecompressedBlockReader]]
    * is created where a the parent is itself with the same offsets.
    **/
  def unblocked[B <: Block](reader: CompressedBlockReader[B]): DecompressedBlockReader[B] =
    new DecompressedBlockReader[B](
      reader = reader.copy(),
      block = reader.block
    )

  def decompress[B <: Block](block: B,
                             readAllIfUncompressed: Boolean,
                             segmentReader: DecompressedBlockReader[SegmentBlock])(implicit updater: BlockUpdater[B]) =
    Block.decompress(
      childBlock = block,
      readAllIfUncompressed = readAllIfUncompressed,
      parentBlock = segmentReader
    )
}

private[core] class DecompressedBlockReader[B <: Block] private(reader: Reader,
                                                                val block: B) extends BlockReader(reader, block) with LazyLogging {

  override def moveTo(newPosition: Long): DecompressedBlockReader[B] = {
    super.moveTo(newPosition)
    this
  }

  def readAllAndGetReader()(implicit blockUpdater: BlockUpdater[B]): IO[DecompressedBlockReader[B]] =
    readAll()
      .map {
        bytes =>
          DecompressedBlockReader.decompressed[B](
            decompressedBytes = bytes,
            block = blockUpdater.updateOffset(block, 0, bytes.size)
          )
      }

  def copy(): DecompressedBlockReader[B] =
    new DecompressedBlockReader(
      reader = reader.copy(),
      block = block
    )
}
