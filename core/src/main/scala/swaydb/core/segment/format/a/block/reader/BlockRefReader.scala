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
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block._
import swaydb.data.slice.{Reader, Slice}

private[core] object BlockRefReader {

  def apply(file: DBFile): IO[swaydb.Error.Segment, BlockRefReader[SegmentBlock.Offset]] =
    file.fileSize map {
      fileSize =>
        new BlockRefReader(
          offset = SegmentBlock.Offset(0, fileSize.toInt),
          reader = Reader(file)
        )
    }

  def apply[O <: BlockOffset](bytes: Slice[Byte])(implicit blockOps: BlockOps[O, _]): BlockRefReader[O] =
    new BlockRefReader(
      offset = blockOps.createOffset(0, bytes.size),
      reader = Reader(bytes)
    )

  def apply[O <: BlockOffset](reader: Reader[swaydb.Error.Segment])(implicit blockOps: BlockOps[O, _]): IO[swaydb.Error.Segment, BlockRefReader[O]] =
    reader.size map {
      readerSize =>
        new BlockRefReader(
          offset = blockOps.createOffset(0, readerSize.toInt),
          reader = reader
        )
    }

  def moveTo[O <: BlockOffset, B <: Block[O]](offset: O, reader: UnblockedReader[O, B]): BlockRefReader[O] =
    new BlockRefReader(
      offset = offset,
      reader = reader
    )

  def moveTo(offset: SegmentBlock.Offset, reader: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): BlockRefReader[SegmentBlock.Offset] =
    new BlockRefReader(
      offset = offset,
      reader = reader
    )

  def moveWithin[O <: BlockOffset](offset: O, reader: UnblockedReader[SegmentBlock.Offset, SegmentBlock]): BlockRefReader[O] =
    new BlockRefReader(
      offset = offset,
      reader = reader
    )
}

private[core] class BlockRefReader[O <: BlockOffset] private(val offset: O,
                                                             private[reader] val reader: Reader[swaydb.Error.Segment]) extends BlockReader with LazyLogging {

  override def moveTo(newPosition: Long): BlockRefReader[O] = {
    super.moveTo(newPosition)
    this
  }

  def readAllAndGetReader()(implicit blockOps: BlockOps[O, _]): IO[swaydb.Error.Segment, BlockRefReader[O]] =
    readAll() map (BlockRefReader(_))

  def copy(): BlockRefReader[O] =
    new BlockRefReader(
      reader = reader.copy(),
      offset = offset
    )

  override val isFile: Boolean = reader.isFile
  override def blockSize: Int = 4096
}
