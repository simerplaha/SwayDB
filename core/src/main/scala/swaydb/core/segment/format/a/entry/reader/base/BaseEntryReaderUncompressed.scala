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

package swaydb.core.segment.format.a.entry.reader.base

import swaydb.IO
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.slice.ReaderBase

private[core] object BaseEntryReaderUncompressed extends BaseEntryReader {

  def read[T](baseId: Int,
              keyValueId: Int,
              sortedIndexAccessPosition: Int,
              keyInfo: Option[Either[Int, Persistent.Partial.Key]],
              indexReader: ReaderBase[swaydb.Error.Segment],
              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent.Partial],
              reader: EntryReader[T]): Option[IO[swaydb.Error.Segment, T]] =
  //GENERATED CONDITIONS
    if (baseId == 95)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (baseId < 95)
      if (baseId == 23)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (baseId < 23)
        if (baseId == 14)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.NoDeadline, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (baseId < 14)
          if (baseId == 13)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else
            None
        else if (baseId > 14)
          if (baseId == 19)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else
            None
        else
          None
      else if (baseId > 23)
        if (baseId == 24)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.NoDeadline, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (baseId == 29)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else
          None
      else
        None
    else if (baseId > 95)
      if (baseId == 141)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (baseId < 141)
        if (baseId == 96)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineUncompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (baseId == 121)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else
          None
      else if (baseId > 141)
        if (baseId == 142)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineUncompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (baseId == 167)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else
          None
      else
        None
    else
      None

  val minID = 13
  val maxID = 167
}