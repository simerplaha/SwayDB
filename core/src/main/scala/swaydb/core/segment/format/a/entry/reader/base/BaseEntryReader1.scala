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
import swaydb.core.segment.format.a.entry.id.BaseEntryId
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
    if (id == 49)
      Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 49)
      if (id == 24)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 24)
        if (id == 12)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 12)
          if (id == 6)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 6)
            if (id == 3)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 3)
              if (id == 1)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1)
                if (id == 0)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1)
                if (id == 2)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 3)
              if (id == 4)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 5)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 6)
            if (id == 9)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 9)
              if (id == 7)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 8)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 9)
              if (id == 10)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 11)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 12)
          if (id == 18)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 18)
            if (id == 15)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 15)
              if (id == 13)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 14)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 15)
              if (id == 16)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 17)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 18)
            if (id == 21)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 21)
              if (id == 19)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 20)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 21)
              if (id == 22)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 23)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 24)
        if (id == 37)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 37)
          if (id == 31)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 31)
            if (id == 28)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 28)
              if (id == 26)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 26)
                if (id == 25)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 26)
                if (id == 27)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 28)
              if (id == 29)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 30)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 31)
            if (id == 34)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 34)
              if (id == 32)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 33)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 34)
              if (id == 35)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 36)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 37)
          if (id == 43)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 43)
            if (id == 40)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 40)
              if (id == 38)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 39)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 40)
              if (id == 41)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 42)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 43)
            if (id == 46)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 46)
              if (id == 44)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 45)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 46)
              if (id == 47)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 48)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 49)
      if (id == 74)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 74)
        if (id == 62)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 62)
          if (id == 56)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 56)
            if (id == 53)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 53)
              if (id == 51)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 51)
                if (id == 50)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 51)
                if (id == 52)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 53)
              if (id == 54)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 55)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 56)
            if (id == 59)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 59)
              if (id == 57)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 58)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 59)
              if (id == 60)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 61)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 62)
          if (id == 68)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 68)
            if (id == 65)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 65)
              if (id == 63)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 64)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 65)
              if (id == 66)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 67)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 68)
            if (id == 71)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 71)
              if (id == 69)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 70)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 71)
              if (id == 72)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 73)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 74)
        if (id == 87)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 87)
          if (id == 81)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 81)
            if (id == 78)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 78)
              if (id == 76)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 76)
                if (id == 75)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 76)
                if (id == 77)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 78)
              if (id == 79)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 80)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 81)
            if (id == 84)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 84)
              if (id == 82)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 83)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 84)
              if (id == 85)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 86)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 87)
          if (id == 93)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 93)
            if (id == 90)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 90)
              if (id == 88)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 89)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 90)
              if (id == 91)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 92)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 93)
            if (id == 96)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 96)
              if (id == 94)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 95)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 96)
              if (id == 97)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 98)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
  val maxID = 98
}