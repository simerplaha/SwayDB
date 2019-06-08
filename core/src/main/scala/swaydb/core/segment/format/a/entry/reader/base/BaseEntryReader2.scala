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

object BaseEntryReader2 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 148)
      Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 148)
      if (id == 123)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 123)
        if (id == 111)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 111)
          if (id == 105)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 105)
            if (id == 102)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 102)
              if (id == 100)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 100)
                if (id == 99)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 100)
                if (id == 101)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 102)
              if (id == 103)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 104)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 105)
            if (id == 108)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 108)
              if (id == 106)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 107)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 108)
              if (id == 109)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 110)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 111)
          if (id == 117)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 117)
            if (id == 114)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 114)
              if (id == 112)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 113)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 114)
              if (id == 115)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 116)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 117)
            if (id == 120)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 120)
              if (id == 118)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 119)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 120)
              if (id == 121)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 122)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 123)
        if (id == 136)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 136)
          if (id == 130)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 130)
            if (id == 127)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 127)
              if (id == 125)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 125)
                if (id == 124)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 125)
                if (id == 126)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 127)
              if (id == 128)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 129)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 130)
            if (id == 133)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 133)
              if (id == 131)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 132)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 133)
              if (id == 134)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 135)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 136)
          if (id == 142)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 142)
            if (id == 139)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 139)
              if (id == 137)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 138)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 139)
              if (id == 140)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 141)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 142)
            if (id == 145)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 145)
              if (id == 143)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 144)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 145)
              if (id == 146)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 147)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 148)
      if (id == 173)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 173)
        if (id == 161)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 161)
          if (id == 155)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 155)
            if (id == 152)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 152)
              if (id == 150)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 150)
                if (id == 149)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 150)
                if (id == 151)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 152)
              if (id == 153)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 154)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 155)
            if (id == 158)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 158)
              if (id == 156)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 157)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 158)
              if (id == 159)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 160)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 161)
          if (id == 167)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 167)
            if (id == 164)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 164)
              if (id == 162)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 163)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 164)
              if (id == 165)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 166)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 167)
            if (id == 170)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 170)
              if (id == 168)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 169)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 170)
              if (id == 171)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 172)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 173)
        if (id == 186)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 186)
          if (id == 180)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 180)
            if (id == 177)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 177)
              if (id == 175)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 175)
                if (id == 174)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 175)
                if (id == 176)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 177)
              if (id == 178)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 179)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 180)
            if (id == 183)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 183)
              if (id == 181)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 182)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 183)
              if (id == 184)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 185)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 186)
          if (id == 192)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 192)
            if (id == 189)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 189)
              if (id == 187)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 188)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 189)
              if (id == 190)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 191)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 192)
            if (id == 195)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 195)
              if (id == 193)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 194)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 195)
              if (id == 196)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 197)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 99
  val maxID = 197
}