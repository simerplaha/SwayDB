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

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, KeyValueId}
import swaydb.core.util.Bytes
import swaydb.data.slice.{ReaderBase, Slice}

object RangeReader extends EntryReader[Persistent.Range] {

  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              sortedIndexEndOffset: Int,
                              sortedIndexAccessPosition: Int,
                              headerKeyBytes: Slice[Byte],
                              indexReader: ReaderBase,
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                              indexOffset: Int,
                              previous: Option[Persistent])(implicit timeReader: TimeReader[T],
                                                            deadlineReader: DeadlineReader[T],
                                                            valueOffsetReader: ValueOffsetReader[T],
                                                            valueLengthReader: ValueLengthReader[T],
                                                            valueBytesReader: ValueReader[T]): Persistent.Range = {
    val valueOffsetAndLength = valueBytesReader.read(indexReader, previous)

    val key =
      KeyReader.read(
        keyValueIdInt = keyValueId,
        keyBytes = headerKeyBytes,
        previous = previous,
        keyValueId = KeyValueId.Range
      )

    val bytesRead =
      Bytes.sizeOfUnsignedInt(headerKeyBytes.size) +
        indexReader.getPosition

    val nextIndexOffsetMaybe = indexOffset + bytesRead - 1

    val nextIndexOffset =
      if (nextIndexOffsetMaybe == sortedIndexEndOffset)
        -1
      else
        nextIndexOffsetMaybe + 1

    //temporary check to ensure that only the required bytes are read.
    assert(indexOffset + bytesRead - 1 <= sortedIndexEndOffset, s"Read more: ${indexOffset + bytesRead - 1} not <= $sortedIndexEndOffset")

    val nextKeySize =
      if (indexReader.hasMore)
        indexReader.readUnsignedInt()
      else
        0

    val (valueOffset, valueLength) = valueOffsetAndLength getOrElse EntryReader.zeroValueOffsetAndLength

    Persistent.Range(
      key = key,
      valuesReader = valuesReader,
      nextIndexOffset = nextIndexOffset,
      nextKeySize = nextKeySize,
      indexOffset = indexOffset,
      valueOffset = valueOffset,
      valueLength = valueLength,
      sortedIndexAccessPosition = sortedIndexAccessPosition
    )
  }
}
