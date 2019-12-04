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

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.slice.ReaderBase
import swaydb.data.slice.{ReaderBase, Slice}

private[core] object BaseEntryReaderUncompressed extends BaseEntryReader {

  def read[T](baseId: Int,
              keyValueId: Int,
              sortedIndexEndOffset: Int,
              sortedIndexAccessPosition: Int,
              headerKeyBytes: Slice[Byte],
              indexReader: ReaderBase,
              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
              indexOffset: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): T =
  //GENERATED CONDITIONS
    if (baseId == 95)
      reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
    else if (baseId < 95)
      if (baseId == 23)
        reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
      else if (baseId < 23)
        if (baseId == 14)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.NoDeadline, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else if (baseId < 14)
          if (baseId == 13)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 14)
          if (baseId == 19)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else if (baseId > 23)
        if (baseId == 24)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.NoDeadline, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else if (baseId == 29)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else
        throw swaydb.Exception.InvalidKeyValueId(baseId)
    else if (baseId > 95)
      if (baseId == 141)
        reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
      else if (baseId < 141)
        if (baseId == 96)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineUncompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else if (baseId == 121)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else if (baseId > 141)
        if (baseId == 142)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineUncompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else if (baseId == 167)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else
        throw swaydb.Exception.InvalidKeyValueId(baseId)
    else
      throw swaydb.Exception.InvalidKeyValueId(baseId)

  val minID = 13
  val maxID = 167
}