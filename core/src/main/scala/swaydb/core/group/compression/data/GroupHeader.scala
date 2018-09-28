/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.group.compression.data

import swaydb.compression.DecompressorInternal
import swaydb.core.segment.format.one.SegmentFooter

private[core] case class ValueInfo(valuesDecompressor: DecompressorInternal,
                                   valuesDecompressedLength: Int,
                                   valuesCompressedLength: Int)

private[core] case class GroupHeader(headerSize: Int,
                                     hasRange: Boolean,
                                     indexDecompressor: DecompressorInternal,
                                     keyValueCount: Int,
                                     indexCompressedLength: Int,
                                     indexDecompressedLength: Int,
                                     compressedStartIndexOffset: Int,
                                     compressedEndIndexOffset: Int,
                                     decompressedStartIndexOffset: Int,
                                     decompressedEndIndexOffset: Int,
                                     bloomFilterItemsCount: Int,
                                     valueInfo: Option[ValueInfo]) {
  val footer =
    SegmentFooter(
      crc = 0,
      startIndexOffset = decompressedStartIndexOffset,
      endIndexOffset = decompressedEndIndexOffset,
      keyValueCount = keyValueCount,
      hasRange = hasRange,
      bloomFilterItemsCount = bloomFilterItemsCount,
      bloomFilter = None
    )
}
