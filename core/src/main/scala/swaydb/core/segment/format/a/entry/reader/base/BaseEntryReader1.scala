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
import swaydb.core.cache.Cache
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.SortedIndexEntryReader
import swaydb.data.MaxKey
import swaydb.data.slice.{ReaderBase, Slice}

private[core] object BaseEntryReader1 extends BaseEntryReader {

  def read[T](baseId: Int,
              keyValueId: Int,
              accessPosition: Int,
              keyInfo: Option[Either[Int, Persistent.Partial.Key]],
              indexReader: ReaderBase[swaydb.Error.Segment],
              valueCache: Option[Cache[swaydb.Error.Segment, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]]],
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: SortedIndexEntryReader[T]): Option[IO[swaydb.Error.Segment, T]] =
  //GENERATED CONDITIONS
    if (baseId == 172)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (baseId < 172)
      if (baseId == 86)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (baseId < 86)
        if (baseId == 43)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (baseId < 43)
          if (baseId == 21)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (baseId < 21)
            if (baseId == 10)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 10)
              if (baseId == 5)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 5)
                if (baseId == 2)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 2)
                  if (baseId == 0)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 1)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 2)
                  if (baseId == 3)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 4)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 5)
                if (baseId == 8)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 8)
                  if (baseId == 6)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 7)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 8)
                  if (baseId == 9)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 10)
              if (baseId == 16)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 16)
                if (baseId == 13)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 13)
                  if (baseId == 11)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 12)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 13)
                  if (baseId == 14)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 15)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 16)
                if (baseId == 19)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 19)
                  if (baseId == 17)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 18)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 19)
                  if (baseId == 20)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 21)
            if (baseId == 32)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 32)
              if (baseId == 27)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 27)
                if (baseId == 24)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 24)
                  if (baseId == 22)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 23)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 24)
                  if (baseId == 25)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 26)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 27)
                if (baseId == 30)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 30)
                  if (baseId == 28)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 29)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 30)
                  if (baseId == 31)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 32)
              if (baseId == 38)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 38)
                if (baseId == 35)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 35)
                  if (baseId == 33)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 34)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 35)
                  if (baseId == 36)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 37)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 38)
                if (baseId == 41)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 41)
                  if (baseId == 39)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 40)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 41)
                  if (baseId == 42)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else
            None
        else if (baseId > 43)
          if (baseId == 65)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (baseId < 65)
            if (baseId == 54)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 54)
              if (baseId == 49)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 49)
                if (baseId == 46)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 46)
                  if (baseId == 44)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 45)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 46)
                  if (baseId == 47)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 48)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 49)
                if (baseId == 52)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 52)
                  if (baseId == 50)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 51)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 52)
                  if (baseId == 53)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 54)
              if (baseId == 60)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 60)
                if (baseId == 57)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 57)
                  if (baseId == 55)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 56)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 57)
                  if (baseId == 58)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 59)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 60)
                if (baseId == 63)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 63)
                  if (baseId == 61)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 62)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 63)
                  if (baseId == 64)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 65)
            if (baseId == 76)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 76)
              if (baseId == 71)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 71)
                if (baseId == 68)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 68)
                  if (baseId == 66)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 67)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 68)
                  if (baseId == 69)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 70)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 71)
                if (baseId == 74)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 74)
                  if (baseId == 72)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 73)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 74)
                  if (baseId == 75)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 76)
              if (baseId == 81)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 81)
                if (baseId == 79)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 79)
                  if (baseId == 77)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 78)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 79)
                  if (baseId == 80)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 81)
                if (baseId == 84)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 84)
                  if (baseId == 82)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 83)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 84)
                  if (baseId == 85)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else
            None
        else
          None
      else if (baseId > 86)
        if (baseId == 129)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (baseId < 129)
          if (baseId == 108)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (baseId < 108)
            if (baseId == 97)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 97)
              if (baseId == 92)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 92)
                if (baseId == 89)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 89)
                  if (baseId == 87)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 88)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 89)
                  if (baseId == 90)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 91)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 92)
                if (baseId == 95)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 95)
                  if (baseId == 93)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 94)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 95)
                  if (baseId == 96)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 97)
              if (baseId == 103)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 103)
                if (baseId == 100)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 100)
                  if (baseId == 98)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 99)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 100)
                  if (baseId == 101)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 102)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 103)
                if (baseId == 106)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 106)
                  if (baseId == 104)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 105)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 106)
                  if (baseId == 107)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 108)
            if (baseId == 119)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 119)
              if (baseId == 114)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 114)
                if (baseId == 111)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 111)
                  if (baseId == 109)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 110)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 111)
                  if (baseId == 112)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 113)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 114)
                if (baseId == 117)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 117)
                  if (baseId == 115)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 116)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 117)
                  if (baseId == 118)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 119)
              if (baseId == 124)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 124)
                if (baseId == 122)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 122)
                  if (baseId == 120)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 121)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 122)
                  if (baseId == 123)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 124)
                if (baseId == 127)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 127)
                  if (baseId == 125)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 126)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 127)
                  if (baseId == 128)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else
            None
        else if (baseId > 129)
          if (baseId == 151)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (baseId < 151)
            if (baseId == 140)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 140)
              if (baseId == 135)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 135)
                if (baseId == 132)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 132)
                  if (baseId == 130)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 131)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 132)
                  if (baseId == 133)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 134)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 135)
                if (baseId == 138)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 138)
                  if (baseId == 136)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 137)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 138)
                  if (baseId == 139)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 140)
              if (baseId == 146)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 146)
                if (baseId == 143)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 143)
                  if (baseId == 141)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 142)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 143)
                  if (baseId == 144)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 145)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 146)
                if (baseId == 149)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 149)
                  if (baseId == 147)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 148)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 149)
                  if (baseId == 150)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 151)
            if (baseId == 162)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 162)
              if (baseId == 157)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 157)
                if (baseId == 154)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 154)
                  if (baseId == 152)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 153)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 154)
                  if (baseId == 155)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 156)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 157)
                if (baseId == 160)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 160)
                  if (baseId == 158)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 159)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 160)
                  if (baseId == 161)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 162)
              if (baseId == 167)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 167)
                if (baseId == 165)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 165)
                  if (baseId == 163)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 164)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 165)
                  if (baseId == 166)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 167)
                if (baseId == 170)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 170)
                  if (baseId == 168)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 169)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 170)
                  if (baseId == 171)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else
            None
        else
          None
      else
        None
    else if (baseId > 172)
      if (baseId == 259)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (baseId < 259)
        if (baseId == 216)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (baseId < 216)
          if (baseId == 194)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (baseId < 194)
            if (baseId == 183)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 183)
              if (baseId == 178)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 178)
                if (baseId == 175)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 175)
                  if (baseId == 173)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 174)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 175)
                  if (baseId == 176)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 177)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 178)
                if (baseId == 181)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 181)
                  if (baseId == 179)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 180)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 181)
                  if (baseId == 182)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 183)
              if (baseId == 189)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 189)
                if (baseId == 186)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 186)
                  if (baseId == 184)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 185)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 186)
                  if (baseId == 187)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 188)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 189)
                if (baseId == 192)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 192)
                  if (baseId == 190)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 191)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 192)
                  if (baseId == 193)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 194)
            if (baseId == 205)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 205)
              if (baseId == 200)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 200)
                if (baseId == 197)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 197)
                  if (baseId == 195)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 196)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 197)
                  if (baseId == 198)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 199)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 200)
                if (baseId == 203)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 203)
                  if (baseId == 201)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 202)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 203)
                  if (baseId == 204)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 205)
              if (baseId == 211)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 211)
                if (baseId == 208)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 208)
                  if (baseId == 206)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 207)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 208)
                  if (baseId == 209)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 210)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 211)
                if (baseId == 214)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 214)
                  if (baseId == 212)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 213)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 214)
                  if (baseId == 215)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else
            None
        else if (baseId > 216)
          if (baseId == 238)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (baseId < 238)
            if (baseId == 227)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 227)
              if (baseId == 222)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 222)
                if (baseId == 219)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 219)
                  if (baseId == 217)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 218)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 219)
                  if (baseId == 220)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 221)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 222)
                if (baseId == 225)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 225)
                  if (baseId == 223)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 224)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 225)
                  if (baseId == 226)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 227)
              if (baseId == 233)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 233)
                if (baseId == 230)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 230)
                  if (baseId == 228)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 229)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 230)
                  if (baseId == 231)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 232)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 233)
                if (baseId == 236)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 236)
                  if (baseId == 234)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 235)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 236)
                  if (baseId == 237)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 238)
            if (baseId == 249)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 249)
              if (baseId == 244)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 244)
                if (baseId == 241)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 241)
                  if (baseId == 239)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 240)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 241)
                  if (baseId == 242)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 243)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 244)
                if (baseId == 247)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 247)
                  if (baseId == 245)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 246)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 247)
                  if (baseId == 248)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 249)
              if (baseId == 254)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 254)
                if (baseId == 252)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 252)
                  if (baseId == 250)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 251)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 252)
                  if (baseId == 253)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 254)
                if (baseId == 257)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 257)
                  if (baseId == 255)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 256)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 257)
                  if (baseId == 258)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else
            None
        else
          None
      else if (baseId > 259)
        if (baseId == 302)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (baseId < 302)
          if (baseId == 281)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (baseId < 281)
            if (baseId == 270)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 270)
              if (baseId == 265)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 265)
                if (baseId == 262)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 262)
                  if (baseId == 260)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 261)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 262)
                  if (baseId == 263)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 264)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 265)
                if (baseId == 268)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 268)
                  if (baseId == 266)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 267)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 268)
                  if (baseId == 269)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 270)
              if (baseId == 276)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 276)
                if (baseId == 273)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 273)
                  if (baseId == 271)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 272)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 273)
                  if (baseId == 274)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 275)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 276)
                if (baseId == 279)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 279)
                  if (baseId == 277)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 278)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 279)
                  if (baseId == 280)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 281)
            if (baseId == 292)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 292)
              if (baseId == 287)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 287)
                if (baseId == 284)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 284)
                  if (baseId == 282)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 283)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 284)
                  if (baseId == 285)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 286)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 287)
                if (baseId == 290)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 290)
                  if (baseId == 288)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 289)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 290)
                  if (baseId == 291)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 292)
              if (baseId == 297)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 297)
                if (baseId == 295)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 295)
                  if (baseId == 293)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 294)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 295)
                  if (baseId == 296)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 297)
                if (baseId == 300)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 300)
                  if (baseId == 298)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 299)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 300)
                  if (baseId == 301)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else
            None
        else if (baseId > 302)
          if (baseId == 324)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (baseId < 324)
            if (baseId == 313)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 313)
              if (baseId == 308)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 308)
                if (baseId == 305)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 305)
                  if (baseId == 303)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 304)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 305)
                  if (baseId == 306)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 307)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 308)
                if (baseId == 311)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 311)
                  if (baseId == 309)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 310)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 311)
                  if (baseId == 312)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 313)
              if (baseId == 319)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 319)
                if (baseId == 316)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 316)
                  if (baseId == 314)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 315)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 316)
                  if (baseId == 317)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 318)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 319)
                if (baseId == 322)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 322)
                  if (baseId == 320)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 321)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 322)
                  if (baseId == 323)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 324)
            if (baseId == 335)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (baseId < 335)
              if (baseId == 330)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 330)
                if (baseId == 327)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 327)
                  if (baseId == 325)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 326)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 327)
                  if (baseId == 328)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 329)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 330)
                if (baseId == 333)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 333)
                  if (baseId == 331)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 332)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 333)
                  if (baseId == 334)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 335)
              if (baseId == 340)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (baseId < 340)
                if (baseId == 338)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 338)
                  if (baseId == 336)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 337)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 338)
                  if (baseId == 339)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (baseId > 340)
                if (baseId == 343)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (baseId < 343)
                  if (baseId == 341)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (baseId == 342)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (baseId > 343)
                  if (baseId == 344)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, accessPosition, keyInfo, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else
            None
        else
          None
      else
        None
    else
      None

  val minID = 0
  val maxID = 344
}