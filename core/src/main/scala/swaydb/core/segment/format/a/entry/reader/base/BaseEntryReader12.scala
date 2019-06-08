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

object BaseEntryReader12 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1138)
      Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1138)
      if (id == 1113)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1113)
        if (id == 1101)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1101)
          if (id == 1095)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1095)
            if (id == 1092)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1092)
              if (id == 1090)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1090)
                if (id == 1089)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1090)
                if (id == 1091)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1092)
              if (id == 1093)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1094)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1095)
            if (id == 1098)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1098)
              if (id == 1096)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1097)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1098)
              if (id == 1099)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1100)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1101)
          if (id == 1107)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1107)
            if (id == 1104)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1104)
              if (id == 1102)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1103)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1104)
              if (id == 1105)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1106)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1107)
            if (id == 1110)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1110)
              if (id == 1108)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1109)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1110)
              if (id == 1111)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1112)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1113)
        if (id == 1126)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1126)
          if (id == 1120)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1120)
            if (id == 1117)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1117)
              if (id == 1115)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1115)
                if (id == 1114)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1115)
                if (id == 1116)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1117)
              if (id == 1118)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1119)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1120)
            if (id == 1123)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1123)
              if (id == 1121)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1122)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1123)
              if (id == 1124)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1125)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1126)
          if (id == 1132)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1132)
            if (id == 1129)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1129)
              if (id == 1127)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1128)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1129)
              if (id == 1130)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1131)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1132)
            if (id == 1135)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1135)
              if (id == 1133)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1134)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1135)
              if (id == 1136)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1137)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1138)
      if (id == 1163)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1163)
        if (id == 1151)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1151)
          if (id == 1145)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1145)
            if (id == 1142)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1142)
              if (id == 1140)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1140)
                if (id == 1139)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1140)
                if (id == 1141)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1142)
              if (id == 1143)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1144)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1145)
            if (id == 1148)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1148)
              if (id == 1146)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1147)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1148)
              if (id == 1149)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1150)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1151)
          if (id == 1157)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1157)
            if (id == 1154)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1154)
              if (id == 1152)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1153)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1154)
              if (id == 1155)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1156)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1157)
            if (id == 1160)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1160)
              if (id == 1158)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1159)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1160)
              if (id == 1161)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1162)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1163)
        if (id == 1176)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1176)
          if (id == 1170)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1170)
            if (id == 1167)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1167)
              if (id == 1165)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1165)
                if (id == 1164)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1165)
                if (id == 1166)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1167)
              if (id == 1168)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1169)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1170)
            if (id == 1173)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1173)
              if (id == 1171)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1172)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1173)
              if (id == 1174)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1175)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1176)
          if (id == 1182)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1182)
            if (id == 1179)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1179)
              if (id == 1177)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1178)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1179)
              if (id == 1180)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1181)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1182)
            if (id == 1185)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1185)
              if (id == 1183)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1184)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1185)
              if (id == 1186)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1187)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 1089
  val maxID = 1187
}