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

package swaydb.core.segment.format.a.entry.reader

import swaydb.core.data.{Persistent, PersistentOptional}
import swaydb.core.data.Persistent.Partial
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base._
import swaydb.core.util.{Bytes, NullOps}
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.Maybe

sealed trait BaseEntryApplier[E] {

  def apply[T <: BaseEntryId](baseId: T)(implicit timeReader: TimeReader[T],
                                         deadlineReader: DeadlineReader[T],
                                         valueOffsetReader: ValueOffsetReader[T],
                                         valueLengthReader: ValueLengthReader[T],
                                         valueBytesReader: ValueReader[T]): E
}

object BaseEntryApplier {

  object ReturnFinders extends BaseEntryApplier[(TimeReader[_], DeadlineReader[_], ValueOffsetReader[_], ValueLengthReader[_], ValueReader[_])] {

    override def apply[T <: BaseEntryId](baseId: T)(implicit timeReader: TimeReader[T],
                                                    deadlineReader: DeadlineReader[T],
                                                    valueOffsetReader: ValueOffsetReader[T],
                                                    valueLengthReader: ValueLengthReader[T],
                                                    valueBytesReader: ValueReader[T]): (TimeReader[T], DeadlineReader[T], ValueOffsetReader[T], ValueLengthReader[T], ValueReader[T]) =
      (timeReader, deadlineReader, valueOffsetReader, valueLengthReader, valueBytesReader)
  }

  def parsePartial(offset: Int,
                   indexEntry: SortedIndexBlock.IndexEntry,
                   sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial = {

    val entryKey = sortedIndex.read(indexEntry.headerInteger)

    val keyValueId = sortedIndex.readUnsignedInt()

    if (KeyValueId isFixedId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          entryKey

        override def toPersistent: Persistent =
          PersistentParser.parse(
            headerInteger = indexEntry.headerInteger,
            indexOffset = offset,
            tailBytes = indexEntry.tailBytes,
            previous = Persistent.Null,
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            valuesReaderNullable = valuesReaderNullable
          )
      }
    else if (KeyValueId.Range hasKeyValueId keyValueId)
      new Partial.Range {
        val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          fromKey

        override def toPersistent: Persistent =
          PersistentParser.parse(
            headerInteger = indexEntry.headerInteger,
            indexOffset = offset,
            tailBytes = indexEntry.tailBytes,
            previous = Persistent.Null,
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            valuesReaderNullable = valuesReaderNullable
          )
      }
    else
      throw new Exception(s"Invalid keyType: $keyValueId, offset: $offset, headerInteger: ${indexEntry.headerInteger}")
  }
}
