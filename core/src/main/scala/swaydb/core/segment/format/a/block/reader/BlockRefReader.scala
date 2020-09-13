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

import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteOps

private[core] object BlockRefReader {

  def apply(file: DBFile): BlockRefReader[SegmentBlock.Offset] =
    new BlockRefReader(
      offset = SegmentBlock.Offset(0, file.fileSize.toInt),
      reader = Reader(file)
    )

  def apply(file: DBFile, fileSize: Int): BlockRefReader[SegmentBlock.Offset] =
    new BlockRefReader(
      offset = SegmentBlock.Offset(0, fileSize),
      reader = Reader(file)
    )

  def apply(file: DBFile, start: Int, fileSize: Int): BlockRefReader[SegmentBlock.Offset] =
    new BlockRefReader(
      offset = SegmentBlock.Offset(start, fileSize),
      reader = Reader(file)
    )

  def apply[O <: BlockOffset](ref: BlockRefReader[_ <: BlockOffset], start: Int)(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader[O](
      offset = blockOps.createOffset(ref.offset.start + start, ref.size.toInt),
      reader = ref.reader
    )

  def apply[O <: BlockOffset](ref: BlockRefReader[_ <: BlockOffset], start: Int, size: Int)(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader[O](
      offset = blockOps.createOffset(ref.offset.start + start, size),
      reader = ref.reader
    )

  def apply[O <: BlockOffset](bytes: Slice[Byte])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader(
      offset = blockOps.createOffset(0, bytes.size),
      reader = Reader(bytes)
    )

  def apply[O <: BlockOffset](reader: Reader[Byte])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader(
      offset = blockOps.createOffset(0, reader.size.toInt),
      reader = reader
    )

  /**
   * @note these readers are required to be nested because [[UnblockedReader]] might have a header size which is not current read.
   */
  def moveTo[O <: BlockOffset, OO <: BlockOffset](start: Int, size: Int, reader: UnblockedReader[OO, _])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader(
      offset = blockOps.createOffset(reader.offset.start + start, size),
      reader = reader.reader
    )

  def moveTo[O <: BlockOffset, OO <: BlockOffset](offset: O, reader: UnblockedReader[OO, _])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader(
      offset = blockOps.createOffset(reader.offset.start + offset.start, offset.size),
      reader = reader.reader
    )
}

private[core] class BlockRefReader[O <: BlockOffset] private(val offset: O,
                                                             private[reader] val reader: Reader[Byte])(implicit val byteOps: ByteOps[Byte]) extends BlockReaderBase {

  override val state: BlockReader.State =
    BlockReader(offset, reader)

  override def moveTo(newPosition: Long): BlockRefReader[O] = {
    state moveTo newPosition.toInt
    this
  }

  override def moveTo(newPosition: Int): BlockRefReader[O] = {
    state moveTo newPosition
    this
  }

  def readFullBlockAndGetReader()(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    BlockRefReader(readFullBlock())

  def copy(): BlockRefReader[O] =
    new BlockRefReader(
      reader = reader.copy(),
      offset = offset
    )
}
