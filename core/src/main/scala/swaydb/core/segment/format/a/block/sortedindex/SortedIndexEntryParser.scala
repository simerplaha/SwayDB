/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.format.a.block.sortedindex

import swaydb.core.data.{Persistent, PersistentOption}
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.format.a.entry.reader.PersistentParser
import swaydb.data.slice.Slice

sealed trait SortedIndexEntryParser[T] {

  def parse(readPosition: Int,
            headerInteger: Int,
            tailBytes: Slice[Byte],
            previous: PersistentOption,
            sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): T

}

object SortedIndexEntryParser {
  final object PartialEntry extends SortedIndexEntryParser[Persistent.Partial] {
    override def parse(readPosition: Int,
                       headerInteger: Int,
                       tailBytes: Slice[Byte],
                       previous: PersistentOption,
                       sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial =
      PersistentParser.parsePartial(
        offset = readPosition,
        headerInteger = headerInteger,
        tailBytes = tailBytes,
        sortedIndex = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
  }

  final object PersistentEntry extends SortedIndexEntryParser[Persistent] {
    override def parse(readPosition: Int,
                       headerInteger: Int,
                       tailBytes: Slice[Byte],
                       previous: PersistentOption,
                       sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent =
      PersistentParser.parse(
        headerInteger = headerInteger,
        indexOffset = readPosition,
        tailBytes = tailBytes,
        previous = previous,
        mightBeCompressed = sortedIndexReader.block.hasPrefixCompression,
        keyCompressionOnly = sortedIndexReader.block.prefixCompressKeysOnly,
        sortedIndexEndOffset = sortedIndexReader.block.sortedIndexEndOffsetForReads,
        normalisedByteSize = sortedIndexReader.block.normalisedByteSize,
        hasAccessPositionIndex = sortedIndexReader.block.enableAccessPositionIndex,
        valuesReaderOrNull = valuesReaderOrNull
      )
  }
}
