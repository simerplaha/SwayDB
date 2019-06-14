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
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.IO
import swaydb.data.slice.Reader

object BaseEntryReader1 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 172)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 172)
      if (id == 86)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 86)
        if (id == 43)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 43)
          if (id == 21)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 21)
            if (id == 10)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 10)
              if (id == 5)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 5)
                if (id == 2)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 2)
                  if (id == 0)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 2)
                  if (id == 3)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 4)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 5)
                if (id == 8)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 8)
                  if (id == 6)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 7)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 8)
                  if (id == 9)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 10)
              if (id == 16)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 16)
                if (id == 13)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 13)
                  if (id == 11)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 12)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 13)
                  if (id == 14)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 15)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 16)
                if (id == 19)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 19)
                  if (id == 17)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 18)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 19)
                  if (id == 20)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 21)
            if (id == 32)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 32)
              if (id == 27)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 27)
                if (id == 24)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 24)
                  if (id == 22)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 23)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 24)
                  if (id == 25)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 26)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 27)
                if (id == 30)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 30)
                  if (id == 28)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 29)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 30)
                  if (id == 31)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 32)
              if (id == 38)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 38)
                if (id == 35)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 35)
                  if (id == 33)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 34)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 35)
                  if (id == 36)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 37)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 38)
                if (id == 41)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 41)
                  if (id == 39)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 40)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 41)
                  if (id == 42)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 43)
          if (id == 65)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 65)
            if (id == 54)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 54)
              if (id == 49)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 49)
                if (id == 46)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 46)
                  if (id == 44)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 45)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 46)
                  if (id == 47)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 48)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 49)
                if (id == 52)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 52)
                  if (id == 50)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 51)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 52)
                  if (id == 53)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 54)
              if (id == 60)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 60)
                if (id == 57)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 57)
                  if (id == 55)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 56)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 57)
                  if (id == 58)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 59)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 60)
                if (id == 63)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 63)
                  if (id == 61)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 62)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 63)
                  if (id == 64)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 65)
            if (id == 76)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 76)
              if (id == 71)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 71)
                if (id == 68)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 68)
                  if (id == 66)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 67)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 68)
                  if (id == 69)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 70)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 71)
                if (id == 74)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 74)
                  if (id == 72)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 73)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 74)
                  if (id == 75)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 76)
              if (id == 81)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 81)
                if (id == 79)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 79)
                  if (id == 77)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 78)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 79)
                  if (id == 80)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 81)
                if (id == 84)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 84)
                  if (id == 82)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 83)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 84)
                  if (id == 85)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 86)
        if (id == 129)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 129)
          if (id == 108)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 108)
            if (id == 97)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 97)
              if (id == 92)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 92)
                if (id == 89)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 89)
                  if (id == 87)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 88)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 89)
                  if (id == 90)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 91)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 92)
                if (id == 95)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 95)
                  if (id == 93)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 94)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 95)
                  if (id == 96)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 97)
              if (id == 103)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 103)
                if (id == 100)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 100)
                  if (id == 98)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 99)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 100)
                  if (id == 101)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 102)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 103)
                if (id == 106)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 106)
                  if (id == 104)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 105)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 106)
                  if (id == 107)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 108)
            if (id == 119)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 119)
              if (id == 114)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 114)
                if (id == 111)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 111)
                  if (id == 109)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 110)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 111)
                  if (id == 112)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 113)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 114)
                if (id == 117)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 117)
                  if (id == 115)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 116)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 117)
                  if (id == 118)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 119)
              if (id == 124)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 124)
                if (id == 122)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 122)
                  if (id == 120)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 121)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 122)
                  if (id == 123)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 124)
                if (id == 127)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 127)
                  if (id == 125)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 126)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 127)
                  if (id == 128)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 129)
          if (id == 151)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 151)
            if (id == 140)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 140)
              if (id == 135)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 135)
                if (id == 132)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 132)
                  if (id == 130)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 131)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 132)
                  if (id == 133)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 134)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 135)
                if (id == 138)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 138)
                  if (id == 136)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 137)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 138)
                  if (id == 139)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 140)
              if (id == 146)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 146)
                if (id == 143)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 143)
                  if (id == 141)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 142)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 143)
                  if (id == 144)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 145)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 146)
                if (id == 149)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 149)
                  if (id == 147)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 148)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 149)
                  if (id == 150)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 151)
            if (id == 162)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 162)
              if (id == 157)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 157)
                if (id == 154)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 154)
                  if (id == 152)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 153)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 154)
                  if (id == 155)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 156)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 157)
                if (id == 160)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 160)
                  if (id == 158)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 159)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 160)
                  if (id == 161)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 162)
              if (id == 167)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 167)
                if (id == 165)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 165)
                  if (id == 163)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 164)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 165)
                  if (id == 166)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 167)
                if (id == 170)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 170)
                  if (id == 168)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 169)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 170)
                  if (id == 171)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 172)
      if (id == 259)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 259)
        if (id == 216)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 216)
          if (id == 194)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 194)
            if (id == 183)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 183)
              if (id == 178)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 178)
                if (id == 175)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 175)
                  if (id == 173)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 174)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 175)
                  if (id == 176)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 177)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 178)
                if (id == 181)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 181)
                  if (id == 179)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 180)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 181)
                  if (id == 182)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 183)
              if (id == 189)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 189)
                if (id == 186)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 186)
                  if (id == 184)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 185)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 186)
                  if (id == 187)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 188)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 189)
                if (id == 192)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 192)
                  if (id == 190)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 191)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 192)
                  if (id == 193)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 194)
            if (id == 205)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 205)
              if (id == 200)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 200)
                if (id == 197)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 197)
                  if (id == 195)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 196)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 197)
                  if (id == 198)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 199)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 200)
                if (id == 203)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 203)
                  if (id == 201)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 202)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 203)
                  if (id == 204)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 205)
              if (id == 211)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 211)
                if (id == 208)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 208)
                  if (id == 206)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 207)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 208)
                  if (id == 209)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 210)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 211)
                if (id == 214)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 214)
                  if (id == 212)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 213)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 214)
                  if (id == 215)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 216)
          if (id == 238)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 238)
            if (id == 227)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 227)
              if (id == 222)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 222)
                if (id == 219)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 219)
                  if (id == 217)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 218)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 219)
                  if (id == 220)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 221)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 222)
                if (id == 225)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 225)
                  if (id == 223)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 224)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 225)
                  if (id == 226)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 227)
              if (id == 233)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 233)
                if (id == 230)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 230)
                  if (id == 228)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 229)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 230)
                  if (id == 231)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 232)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 233)
                if (id == 236)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 236)
                  if (id == 234)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 235)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 236)
                  if (id == 237)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 238)
            if (id == 249)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 249)
              if (id == 244)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 244)
                if (id == 241)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 241)
                  if (id == 239)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 240)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 241)
                  if (id == 242)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 243)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 244)
                if (id == 247)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 247)
                  if (id == 245)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 246)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 247)
                  if (id == 248)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 249)
              if (id == 254)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 254)
                if (id == 252)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 252)
                  if (id == 250)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 251)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 252)
                  if (id == 253)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 254)
                if (id == 257)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 257)
                  if (id == 255)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 256)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 257)
                  if (id == 258)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 259)
        if (id == 302)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 302)
          if (id == 281)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 281)
            if (id == 270)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 270)
              if (id == 265)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 265)
                if (id == 262)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 262)
                  if (id == 260)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 261)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 262)
                  if (id == 263)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 264)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 265)
                if (id == 268)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 268)
                  if (id == 266)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 267)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 268)
                  if (id == 269)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 270)
              if (id == 276)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 276)
                if (id == 273)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 273)
                  if (id == 271)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 272)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 273)
                  if (id == 274)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 275)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 276)
                if (id == 279)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 279)
                  if (id == 277)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 278)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 279)
                  if (id == 280)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 281)
            if (id == 292)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 292)
              if (id == 287)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 287)
                if (id == 284)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 284)
                  if (id == 282)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 283)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 284)
                  if (id == 285)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 286)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 287)
                if (id == 290)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 290)
                  if (id == 288)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 289)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 290)
                  if (id == 291)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 292)
              if (id == 297)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 297)
                if (id == 295)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 295)
                  if (id == 293)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 294)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 295)
                  if (id == 296)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 297)
                if (id == 300)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 300)
                  if (id == 298)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 299)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 300)
                  if (id == 301)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 302)
          if (id == 324)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 324)
            if (id == 313)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 313)
              if (id == 308)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 308)
                if (id == 305)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 305)
                  if (id == 303)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 304)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 305)
                  if (id == 306)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 307)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 308)
                if (id == 311)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 311)
                  if (id == 309)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 310)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 311)
                  if (id == 312)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 313)
              if (id == 319)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 319)
                if (id == 316)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 316)
                  if (id == 314)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 315)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 316)
                  if (id == 317)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 318)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 319)
                if (id == 322)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 322)
                  if (id == 320)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 321)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 322)
                  if (id == 323)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 324)
            if (id == 335)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 335)
              if (id == 330)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 330)
                if (id == 327)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 327)
                  if (id == 325)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 326)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 327)
                  if (id == 328)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 329)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 330)
                if (id == 333)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 333)
                  if (id == 331)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 332)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 333)
                  if (id == 334)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 335)
              if (id == 340)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 340)
                if (id == 338)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 338)
                  if (id == 336)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 337)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 338)
                  if (id == 339)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 340)
                if (id == 343)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 343)
                  if (id == 341)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 342)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 343)
                  if (id == 344)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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