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

import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Persistent, PersistentOptional}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice

object PersistentParser {

  def parse(headerInteger: Int,
            indexOffset: Int,
            tailBytes: Slice[Byte],
            previous: PersistentOptional,
            mightBeCompressed: Boolean,
            keyCompressionOnly: Boolean,
            sortedIndexEndOffset: Int,
            normalisedByteSize: Int,
            hasAccessPositionIndex: Boolean,
            valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent = {
    val reader = Reader(tailBytes)

    val headerKeyBytes = reader.read(headerInteger)

    val keyValueId = reader.readUnsignedInt()

    if (KeyValueId.Put hasKeyValueId keyValueId)
      PersistentReader.read(
        indexOffset = indexOffset,
        headerInteger = headerInteger,
        headerKeyBytes = headerKeyBytes,
        keyValueId = keyValueId,
        tailReader = reader,
        previous = previous,
        //sorted index stats
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        sortedIndexEndOffset = sortedIndexEndOffset,
        normalisedByteSize = normalisedByteSize,
        hasAccessPositionIndex = hasAccessPositionIndex,
        valuesReaderNullable = valuesReaderNullable,
        reader = Persistent.Put
      )
    else if (KeyValueId.Range hasKeyValueId keyValueId)
      PersistentReader.read(
        indexOffset = indexOffset,
        headerInteger = headerInteger,
        headerKeyBytes = headerKeyBytes,
        keyValueId = keyValueId,
        tailReader = reader,
        previous = previous,
        //sorted index stats
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        sortedIndexEndOffset = sortedIndexEndOffset,
        normalisedByteSize = normalisedByteSize,
        hasAccessPositionIndex = hasAccessPositionIndex,
        valuesReaderNullable = valuesReaderNullable,
        reader = Persistent.Range
      )
    else if (KeyValueId.Remove hasKeyValueId keyValueId)
      PersistentReader.read(
        indexOffset = indexOffset,
        headerInteger = headerInteger,
        headerKeyBytes = headerKeyBytes,
        keyValueId = keyValueId,
        tailReader = reader,
        previous = previous,
        //sorted index stats
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        sortedIndexEndOffset = sortedIndexEndOffset,
        normalisedByteSize = normalisedByteSize,
        hasAccessPositionIndex = hasAccessPositionIndex,
        valuesReaderNullable = valuesReaderNullable,
        reader = Persistent.Remove
      )
    else if (KeyValueId.Update hasKeyValueId keyValueId)
      PersistentReader.read(
        indexOffset = indexOffset,
        headerInteger = headerInteger,
        headerKeyBytes = headerKeyBytes,
        keyValueId = keyValueId,
        tailReader = reader,
        previous = previous,
        //sorted index stats
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        sortedIndexEndOffset = sortedIndexEndOffset,
        normalisedByteSize = normalisedByteSize,
        hasAccessPositionIndex = hasAccessPositionIndex,
        valuesReaderNullable = valuesReaderNullable,
        reader = Persistent.Update
      )
    else if (KeyValueId.Function hasKeyValueId keyValueId)
      PersistentReader.read(
        indexOffset = indexOffset,
        headerInteger = headerInteger,
        headerKeyBytes = headerKeyBytes,
        keyValueId = keyValueId,
        tailReader = reader,
        previous = previous,
        //sorted index stats
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        sortedIndexEndOffset = sortedIndexEndOffset,
        normalisedByteSize = normalisedByteSize,
        hasAccessPositionIndex = hasAccessPositionIndex,
        valuesReaderNullable = valuesReaderNullable,
        reader = Persistent.Function
      )
    else if (KeyValueId.PendingApply hasKeyValueId keyValueId)
      PersistentReader.read(
        indexOffset = indexOffset,
        headerInteger = headerInteger,
        headerKeyBytes = headerKeyBytes,
        keyValueId = keyValueId,
        tailReader = reader,
        previous = previous,
        //sorted index stats
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        sortedIndexEndOffset = sortedIndexEndOffset,
        normalisedByteSize = normalisedByteSize,
        hasAccessPositionIndex = hasAccessPositionIndex,
        valuesReaderNullable = valuesReaderNullable,
        reader = Persistent.PendingApply
      )
    else
      throw swaydb.Exception.InvalidBaseId(keyValueId)
  }

  def parsePartial(offset: Int,
                   headerInteger: Int,
                   tailBytes: Slice[Byte],
                   sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial = {
    val tailReader = Reader(tailBytes)

    val headerKeyBytes = tailReader.read(headerInteger)

    val keyValueId = tailReader.readUnsignedInt()

    if (KeyValueId.Put hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent.Put =
          PersistentReader.read(
            indexOffset = indexOffset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = tailReader,
            previous = Persistent.Null,
            //sorted index stats
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            valuesReaderNullable = valuesReaderNullable,
            reader = Persistent.Put
          )
      }
    else if (KeyValueId.Remove hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent.Remove =
          PersistentReader.read(
            indexOffset = indexOffset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = tailReader,
            previous = Persistent.Null,
            //sorted index stats
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            valuesReaderNullable = valuesReaderNullable,
            reader = Persistent.Remove
          )
      }
    else if (KeyValueId.Function hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent.Function =
          PersistentReader.read(
            indexOffset = indexOffset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = tailReader,
            previous = Persistent.Null,
            //sorted index stats
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            valuesReaderNullable = valuesReaderNullable,
            reader = Persistent.Function
          )
      }
    else if (KeyValueId.Update hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent.Update =
          PersistentReader.read(
            indexOffset = indexOffset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = tailReader,
            previous = Persistent.Null,
            //sorted index stats
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            valuesReaderNullable = valuesReaderNullable,
            reader = Persistent.Update
          )
      }
    else if (KeyValueId.PendingApply hasKeyValueId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          headerKeyBytes

        override def toPersistent: Persistent.PendingApply =
          PersistentReader.read(
            indexOffset = indexOffset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = tailReader,
            previous = Persistent.Null,
            //sorted index stats
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            valuesReaderNullable = valuesReaderNullable,
            reader = Persistent.PendingApply
          )
      }
    else if (KeyValueId.Range hasKeyValueId keyValueId)
      new Partial.Range {
        val (fromKey, toKey) = Bytes.decompressJoin(headerKeyBytes)

        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          fromKey

        override def toPersistent: Persistent.Range =
          PersistentReader.read(
            indexOffset = indexOffset,
            headerInteger = headerInteger,
            headerKeyBytes = headerKeyBytes,
            keyValueId = keyValueId,
            tailReader = tailReader,
            previous = Persistent.Null,
            //sorted index stats
            normalisedByteSize = sortedIndex.block.normalisedByteSize,
            mightBeCompressed = sortedIndex.block.hasPrefixCompression,
            keyCompressionOnly = sortedIndex.block.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndex.block.sortedIndexEndOffsetForReads,
            hasAccessPositionIndex = sortedIndex.block.enableAccessPositionIndex,
            valuesReaderNullable = valuesReaderNullable,
            reader = Persistent.Range
          )
      }
    else
      throw new Exception(s"Invalid keyType: $keyValueId, offset: $offset, headerInteger: $headerInteger")
  }
}
