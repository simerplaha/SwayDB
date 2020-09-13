/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.reader

import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.{Block, BlockOffset, BlockOps}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteOps

private[core] object BlockedReader {

  def apply[O <: BlockOffset, B <: Block[O]](block: B,
                                             bytes: Slice[Byte]) =
    new BlockedReader[O, B](
      reader = Reader(bytes),
      block = block
    )

  def apply[O <: BlockOffset, B <: Block[O]](ref: BlockRefReader[O])(implicit blockOps: BlockOps[O, B]): BlockedReader[O, B] = {
    val header = Block.readHeader(ref)
    val block = blockOps.readBlock(header)
    new BlockedReader[O, B](
      reader = ref.reader,
      block = block
    )
  }

  def apply[O <: BlockOffset, B <: Block[O]](block: B, reader: UnblockedReader[SegmentBlock.Offset, SegmentBlock]): BlockedReader[O, B] =
    new BlockedReader[O, B](
      reader = reader.reader,
      block = block
    )
}

private[core] class BlockedReader[O <: BlockOffset, B <: Block[O]] private(private[reader] val reader: Reader[Byte],
                                                                           val block: B)(implicit val byteOps: ByteOps[Byte]) extends BlockReaderBase {

  val offset = block.offset

  override val state: BlockReader.State =
    BlockReader(offset, reader)

  override def moveTo(newPosition: Long): BlockedReader[O, B] = {
    state moveTo newPosition.toInt
    this
  }

  override def moveTo(newPosition: Int): BlockedReader[O, B] = {
    state moveTo newPosition
    this
  }

  def readAllAndGetReader()(implicit blockOps: BlockOps[O, B]): BlockedReader[O, B] = {
    val bytes = readFullBlock()
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
}
