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
import swaydb.core.segment.format.a.block.{Block, BlockUpdater}
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

/**
  * Reader for the [[Block.CompressionInfo]] that skips [[Block.Header]] bytes.
  */
private[core] object BlockedReader {

  def apply[B <: Block](block: B,
                        bytes: Slice[Byte]) =
    new BlockedReader[B](
      reader = Reader(bytes),
      block = block
    )

  def apply[B <: Block](block: B,
                        reader: Reader): BlockedReader[B] =
    new BlockedReader[B](
      reader = reader.copy(),
      block = block
    )
}

private[core] class BlockedReader[B <: Block] private(reader: Reader,
                                                      val block: B) extends BlockReader[B](reader, block) with LazyLogging {
  override def moveTo(newPosition: Long): BlockedReader[B] = {
    super.moveTo(newPosition)
    this
  }

  def readAllAndGetReader()(implicit blockUpdater: BlockUpdater[B]): IO[BlockedReader[B]] =
    readAll()
      .map {
        compressedBytes =>
          BlockedReader[B](
            bytes = compressedBytes,
            block = blockUpdater.updateOffset(block, 0, compressedBytes.size)
          )
      }

  override def copy(): BlockedReader[B] =
    new BlockedReader(
      reader = reader.copy(),
      block = block
    )
}
