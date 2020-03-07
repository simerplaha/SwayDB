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

import swaydb.core.io.reader.{FileReader, Reader}
import swaydb.core.segment.format.a.block.{Block, BlockOffset, BlockOps}
import swaydb.data.slice.{Reader, Slice, SliceReader}

/**
 * A typed object that indicates that block is already decompressed and now is reading data bytes.
 */

sealed trait UnblockedReaderOption[+O <: BlockOffset, +B <: Block[O]] {
  def isNone: Boolean
  def isSome: Boolean = !isNone
}

private[core] object UnblockedReader {

  final case object Null extends UnblockedReaderOption[Nothing, Nothing] {
    override def isNone: Boolean = true
  }

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
                                             readAllIfUncompressed: Boolean)(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    Block.unblock(
      reader = blockedReader,
      readAllIfUncompressed = readAllIfUncompressed
    )

  def fromUncompressed[O <: BlockOffset, B <: Block[O]](blockedReader: BlockedReader[O, B]): UnblockedReader[O, B] =
    new UnblockedReader[O, B](
      block = blockedReader.block,
      reader = blockedReader.reader
    )
}

private[core] class UnblockedReader[O <: BlockOffset, B <: Block[O]] private(val block: B,
                                                                             private[reader] val reader: Reader) extends BlockReaderBase with UnblockedReaderOption[O, B] {

  val offset = block.offset

  override def isNone: Boolean = false

  override val state: BlockReader.State =
    BlockReader(offset, reader)

  val hasBlockCache =
    blockSize.isDefined

  def blockSize: Option[Int] =
    reader match {
      case reader: FileReader =>
        reader.file.blockSize

      case SliceReader(_, _) =>
        None
    }

  def underlyingArraySizeOrReaderSize: Int =
    reader match {
      case reader: FileReader =>
        reader.size.toInt

      case SliceReader(slice, _) =>
        slice.underlyingArraySize
    }

  override def moveTo(newPosition: Long): UnblockedReader[O, B] = {
    state moveTo newPosition.toInt
    this
  }

  def moveTo(newPosition: Int): UnblockedReader[O, B] = {
    state moveTo newPosition
    this
  }

  def readAllAndGetReader()(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] = {
    val bytes = readFullBlock()
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
