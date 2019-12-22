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
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, KeyValueId}
import swaydb.data.slice.{ReaderBase, Slice}

object RemoveReader extends EntryReader[Persistent.Remove] {

  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              sortedIndexEndOffset: Int,
                              sortedIndexAccessPosition: Int,
                              headerKeyBytes: Slice[Byte],
                              indexReader: ReaderBase,
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                              indexOffset: Int,
                              normalisedByteSize: Int,
                              previous: PersistentOptional)(implicit timeReader: TimeReader[T],
                                                            deadlineReader: DeadlineReader[T],
                                                            valueOffsetReader: ValueOffsetReader[T],
                                                            valueLengthReader: ValueLengthReader[T],
                                                            valueBytesReader: ValueReader[T]): Persistent.Remove = {
    val deadline = deadlineReader.read(indexReader, previous)
    val time = timeReader.read(indexReader, previous)

    val key =
      KeyReader.read(
        keyValueIdInt = keyValueId,
        keyBytes = headerKeyBytes,
        previous = previous,
        keyValueId = KeyValueId.Remove
      )

    val (nextIndexOffset: Int, nextKeySize: Int) =
      EntryReader.calculateNextKeyValueOffsetAndSize(
        sortedIndexEndOffset = sortedIndexEndOffset,
        previousKeyValueHeaderKeyBytes = headerKeyBytes,
        previousKeyValueIndexReader = indexReader,
        previousKeyValueIndexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize
      )

    Persistent.Remove(
      _key = key,
      indexOffset = indexOffset,
      nextIndexOffset = nextIndexOffset,
      nextKeySize = nextKeySize,
      deadline = deadline,
      sortedIndexAccessPosition = sortedIndexAccessPosition,
      _time = time
    )
  }
}
