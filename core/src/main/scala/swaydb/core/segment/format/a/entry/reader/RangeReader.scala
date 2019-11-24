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
import swaydb.data.slice.ReaderBase

object RangeReader extends EntryReader[Persistent.Range] {

  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              sortedIndexAccessPosition: Int,
                              keySize: Option[Int],
                              indexReader: ReaderBase,
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                              indexOffset: Int,
                              nextIndexOffset: Int,
                              nextIndexSize: Int,
                              previous: Option[Persistent])(implicit timeReader: TimeReader[T],
                                                            deadlineReader: DeadlineReader[T],
                                                            valueOffsetReader: ValueOffsetReader[T],
                                                            valueLengthReader: ValueLengthReader[T],
                                                            valueBytesReader: ValueReader[T]): Persistent.Range = {
    val valueOffsetAndLength = valueBytesReader.read(indexReader, previous)
    val key =
      KeyReader.read(
        keyValueIdInt = keyValueId,
        indexReader = indexReader,
        keySize = keySize,
        previous = previous,
        keyValueId = KeyValueId.Range
      )

    val valueOffset = valueOffsetAndLength.map(_._1).getOrElse(-1)
    val valueLength = valueOffsetAndLength.map(_._2).getOrElse(0)

    Persistent.Range(
      key = key,
      valuesReader = valuesReader,
      nextIndexOffset = nextIndexOffset,
      nextIndexSize = nextIndexSize,
      indexOffset = indexOffset,
      valueOffset = valueOffset,
      valueLength = valueLength,
      sortedIndexAccessPosition = sortedIndexAccessPosition
    )
  }
}
