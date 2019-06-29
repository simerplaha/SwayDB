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

package swaydb.core.segment.format.a

import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.format.a.block.Block
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

object SegmentBlock {

  def read(offset: SegmentWriter.Offset,
           segmentReader: Reader): IO[SegmentBlock] =
    Block.readHeader(
      offset = offset,
      segmentReader = segmentReader
    ) map {
      header =>
        SegmentBlock(
          blockOffset = offset,
          headerSize = header.headerSize,
          compressionInfo = header.compressionInfo
        )
    }
}

case class SegmentBlock(blockOffset: SegmentWriter.Offset,
                        headerSize: Int,
                        compressionInfo: Option[Block.CompressionInfo]) extends Block {

  override def createBlockReader(bytes: Slice[Byte]): BlockReader[SegmentBlock] =
    createBlockReader(Reader(bytes))

  def createBlockReader(segmentReader: Reader): BlockReader[SegmentBlock] =
    BlockReader(
      reader = segmentReader,
      block = this
    )

  override def updateOffset(start: Int, size: Int): Block =
    copy(blockOffset = SegmentWriter.Offset(start = start, size = size))
}
