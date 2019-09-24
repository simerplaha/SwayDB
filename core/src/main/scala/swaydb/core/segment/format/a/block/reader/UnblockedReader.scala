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
import swaydb.core.io.reader.{FileReader, Reader}
import swaydb.core.segment.format.a.block.{Block, BlockOffset, BlockOps}
import swaydb.data.slice.{Reader, Slice, SliceReader}

/**
 * A typed object that indicates that block is already decompressed and now is reading data bytes.
 *
 * [[Block.unblock]] creates the object and should be the only function that creates it.
 */

private[core] object UnblockedReader {

  def empty[O <: BlockOffset, B <: Block[O]](block: B)(implicit blockOps: BlockOps[O, B]) =
    new UnblockedReader[O, B](
      reader = Reader.empty,
      block = blockOps.updateBlockOffset(block, 0, 0)
    )

  def apply[O <: BlockOffset, B <: Block[O]](block: B,
                                             bytes: Slice[Byte]): UnblockedReader[O, B] =
    new UnblockedReader[O, B](
      reader = Reader(bytes),
      block = block
    )

  def moveTo[O <: BlockOffset, B <: Block[O]](offset: O,
                                              reader: UnblockedReader[O, B])(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    new UnblockedReader[O, B](
      block = blockOps.updateBlockOffset(reader.block, reader.offset.start + offset.start, offset.size),
      reader = reader.reader
    )

  def apply[O <: BlockOffset, B <: Block[O]](blockedReader: BlockedReader[O, B],
                                             readAllIfUncompressed: Boolean)(implicit blockOps: BlockOps[O, B]): IO[swaydb.Error.Segment, UnblockedReader[O, B]] =
    Block.unblock(
      reader = blockedReader,
      readAllIfUncompressed = readAllIfUncompressed
    )

  def fromUncompressed[O <: BlockOffset, B <: Block[O]](blockedReader: BlockedReader[O, B])(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    new UnblockedReader[O, B](
      block = blockedReader.block,
      reader = blockedReader.reader
    )
}

private[core] class UnblockedReader[O <: BlockOffset, B <: Block[O]] private(val block: B,
                                                                             private[reader] val reader: Reader[swaydb.Error.Segment]) extends BlockReaderBase with LazyLogging {

  val offset = block.offset

  override val state: BlockReader.State =
    BlockReader(offset, reader)

  val hasBlockCache =
    blockSize.isDefined

  def readBlock(position: Int) =
    reader.readBlock(position)

  def blockSize: Option[Int] =
    reader match {
      case reader: FileReader =>
        reader.file.blockSize

      case SliceReader(_, _) =>
        None
    }

  override def moveTo(newPosition: Long): UnblockedReader[O, B] = {
    state moveTo newPosition.toInt
    this
  }

  def moveTo(newPosition: Int): UnblockedReader[O, B] = {
    state moveTo newPosition
    this
  }

  def readAllAndGetReader()(implicit blockOps: BlockOps[O, B]): IO[swaydb.Error.Segment, UnblockedReader[O, B]] =
    readFullBlock()
      .map {
        bytes =>
          UnblockedReader[O, B](
            bytes = bytes,
            block = blockOps.updateBlockOffset(block, 0, bytes.size)
          )
      }

  def copy(): UnblockedReader[O, B] =
    new UnblockedReader(
      block = block,
      reader = reader.copy()
    )
}
