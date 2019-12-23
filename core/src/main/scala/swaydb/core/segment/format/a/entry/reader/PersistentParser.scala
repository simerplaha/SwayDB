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
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.ValuesBlock
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
    val reader = Reader(tailBytes, Bytes.sizeOfUnsignedInt(headerInteger))

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
}
