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
import swaydb.IO
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.{Block, BlockOffset, BlockOps, SegmentBlock}
import swaydb.data.io.Core
import swaydb.data.slice.{Reader, Slice}

/**
  * Reader[swaydb.Error.Segment] for the [[Block.CompressionInfo]] that skips [[Block.Header]] bytes.
  */
private[core] object BlockedReader {

  def apply[O <: BlockOffset, B <: Block[O]](block: B,
                                             bytes: Slice[Byte]) =
    new BlockedReader[O, B](
      reader = Reader(bytes),
      block = block
    )

  def apply[O <: BlockOffset, B <: Block[O]](ref: BlockRefReader[O])(implicit blockOps: BlockOps[O, B]): IO[swaydb.Error.Segment, BlockedReader[O, B]] =
    Block.readHeader(ref) flatMap {
      header =>
        blockOps.readBlock(header) map {
          block =>
            new BlockedReader[O, B](
              reader = ref.copy(),
              block = block
            )
        }
    }

  def apply[O <: BlockOffset, B <: Block[O]](block: B, parent: UnblockedReader[SegmentBlock.Offset, SegmentBlock]): BlockedReader[O, B] =
    new BlockedReader[O, B](
      reader = parent.copy(),
      block = block
    )
}

private[core] class BlockedReader[O <: BlockOffset, B <: Block[O]] private(private[reader] val reader: Reader[swaydb.Error.Segment],
                                                                           val block: B) extends BlockReader with LazyLogging {

  def offset = block.offset

  override def moveTo(newPosition: Long): BlockedReader[O, B] = {
    super.moveTo(newPosition)
    this
  }

  def readAllAndGetReader()(implicit blockOps: BlockOps[O, B]): IO[swaydb.Error.Segment, BlockedReader[O, B]] =
    readAll()
      .map {
        bytes =>
          BlockedReader[O, B](
            bytes = bytes,
            block = blockOps.updateBlockOffset(block, 0, bytes.size)
          )
      }

  override def copy(): BlockedReader[O, B] =
    new BlockedReader(
      reader = reader.copy(),
      block = block
    )

  override val isFile: Boolean = reader.isFile
  override def blockSize: Int = 4096
}
