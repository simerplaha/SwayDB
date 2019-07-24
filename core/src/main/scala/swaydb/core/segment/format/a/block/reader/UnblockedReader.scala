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
import swaydb.core.segment.format.a.block.{Block, BlockOffset, BlockOps}
import swaydb.data.io.Core
import swaydb.data.slice.{Reader, Slice}

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
                                              parent: UnblockedReader[O, B])(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    new UnblockedReader[O, B](
      reader = parent,
      block = blockOps.updateBlockOffset(parent.block, offset.start, offset.size)
    )

  def apply[O <: BlockOffset, B <: Block[O]](blockedReader: BlockedReader[O, B],
                                             readAllIfUncompressed: Boolean)(implicit blockOps: BlockOps[O, B]): IO[Core.IO.Error, UnblockedReader[O, B]] =
    Block.unblock(
      reader = blockedReader,
      readAllIfUncompressed = readAllIfUncompressed
    )

  def asUnblocked[O <: BlockOffset, B <: Block[O]](blockedReader: BlockedReader[O, B])(implicit blockOps: BlockOps[O, B]): IO[Core.IO.Error, UnblockedReader[O, B]] =
    blockedReader.size map {
      blockSize =>
        new UnblockedReader(
          block = blockOps.updateBlockOffset(blockedReader.block, 0, blockSize.toInt),
          reader = blockedReader
        )
    }
}

private[core] class UnblockedReader[O <: BlockOffset, B <: Block[O]] private(val block: B,
                                                                             private[reader] val reader: Reader[Core.IO.Error]) extends BlockReader with LazyLogging {

  def offset = block.offset

  override def moveTo(newPosition: Long): UnblockedReader[O, B] = {
    super.moveTo(newPosition)
    this
  }

  def readAllAndGetReader()(implicit blockOps: BlockOps[O, B]): IO[Core.IO.Error, UnblockedReader[O, B]] =
    readAll()
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

  override val isFile: Boolean = reader.isFile
  override def blockSize: Int = 4096
}
