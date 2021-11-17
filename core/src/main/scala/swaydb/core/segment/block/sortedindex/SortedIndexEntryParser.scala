/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.block.sortedindex

import swaydb.core.segment.data.{Persistent, PersistentOption}
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}
import swaydb.core.segment.entry.reader.PersistentParser
import swaydb.slice.Slice

sealed trait SortedIndexEntryParser[T] {

  def parse(readPosition: Int,
            headerInteger: Int,
            tailBytes: Slice[Byte],
            previous: PersistentOption,
            sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
            valuesReaderOrNull: UnblockedReader[ValuesBlockOffset, ValuesBlock]): T

}

object SortedIndexEntryParser {

  final object PartialEntry extends SortedIndexEntryParser[Persistent.Partial] {
    override def parse(readPosition: Int,
                       headerInteger: Int,
                       tailBytes: Slice[Byte],
                       previous: PersistentOption,
                       sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlockOffset, ValuesBlock]): Persistent.Partial =
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
                       sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlockOffset, ValuesBlock]): Persistent =
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
        optimisedForReverseIteration = sortedIndexReader.block.optimiseForReverseIteration,
        valuesReaderOrNull = valuesReaderOrNull
      )
  }
}
