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

object BaseEntryReader4 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1207)
      Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1207)
      if (id == 1121)
        Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1121)
        if (id == 1078)
          Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1078)
          if (id == 1056)
            Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1056)
            if (id == 1045)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1045)
              if (id == 1040)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1040)
                if (id == 1037)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1037)
                  if (id == 1035)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1036)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1037)
                  if (id == 1038)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1039)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1040)
                if (id == 1043)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1043)
                  if (id == 1041)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1042)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1043)
                  if (id == 1044)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1045)
              if (id == 1051)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1051)
                if (id == 1048)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1048)
                  if (id == 1046)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1047)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1048)
                  if (id == 1049)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1050)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1051)
                if (id == 1054)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1054)
                  if (id == 1052)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1053)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1054)
                  if (id == 1055)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1056)
            if (id == 1067)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1067)
              if (id == 1062)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1062)
                if (id == 1059)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1059)
                  if (id == 1057)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1058)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1059)
                  if (id == 1060)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1061)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1062)
                if (id == 1065)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1065)
                  if (id == 1063)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1064)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1065)
                  if (id == 1066)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1067)
              if (id == 1073)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1073)
                if (id == 1070)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1070)
                  if (id == 1068)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1069)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1070)
                  if (id == 1071)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1072)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1073)
                if (id == 1076)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1076)
                  if (id == 1074)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1075)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1076)
                  if (id == 1077)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 1078)
          if (id == 1100)
            Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1100)
            if (id == 1089)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1089)
              if (id == 1084)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1084)
                if (id == 1081)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1081)
                  if (id == 1079)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1080)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1081)
                  if (id == 1082)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1083)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1084)
                if (id == 1087)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1087)
                  if (id == 1085)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1086)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1087)
                  if (id == 1088)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1089)
              if (id == 1095)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1095)
                if (id == 1092)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1092)
                  if (id == 1090)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1091)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1092)
                  if (id == 1093)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1094)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1095)
                if (id == 1098)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1098)
                  if (id == 1096)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1097)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1098)
                  if (id == 1099)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1100)
            if (id == 1111)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1111)
              if (id == 1106)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1106)
                if (id == 1103)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1103)
                  if (id == 1101)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1102)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1103)
                  if (id == 1104)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1105)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1106)
                if (id == 1109)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1109)
                  if (id == 1107)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1108)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1109)
                  if (id == 1110)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1111)
              if (id == 1116)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1116)
                if (id == 1114)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1114)
                  if (id == 1112)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1113)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1114)
                  if (id == 1115)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1116)
                if (id == 1119)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1119)
                  if (id == 1117)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1118)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1119)
                  if (id == 1120)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 1121)
        if (id == 1164)
          Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1164)
          if (id == 1143)
            Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1143)
            if (id == 1132)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1132)
              if (id == 1127)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1127)
                if (id == 1124)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1124)
                  if (id == 1122)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1123)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1124)
                  if (id == 1125)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1126)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1127)
                if (id == 1130)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1130)
                  if (id == 1128)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1129)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1130)
                  if (id == 1131)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1132)
              if (id == 1138)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1138)
                if (id == 1135)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1135)
                  if (id == 1133)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1134)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1135)
                  if (id == 1136)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1137)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1138)
                if (id == 1141)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1141)
                  if (id == 1139)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1140)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1141)
                  if (id == 1142)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1143)
            if (id == 1154)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1154)
              if (id == 1149)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1149)
                if (id == 1146)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1146)
                  if (id == 1144)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1145)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1146)
                  if (id == 1147)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1148)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1149)
                if (id == 1152)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1152)
                  if (id == 1150)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1151)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1152)
                  if (id == 1153)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1154)
              if (id == 1159)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1159)
                if (id == 1157)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1157)
                  if (id == 1155)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1156)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1157)
                  if (id == 1158)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1159)
                if (id == 1162)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1162)
                  if (id == 1160)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1161)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1162)
                  if (id == 1163)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 1164)
          if (id == 1186)
            Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1186)
            if (id == 1175)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1175)
              if (id == 1170)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1170)
                if (id == 1167)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1167)
                  if (id == 1165)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1166)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1167)
                  if (id == 1168)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1169)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1170)
                if (id == 1173)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1173)
                  if (id == 1171)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1172)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1173)
                  if (id == 1174)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1175)
              if (id == 1181)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1181)
                if (id == 1178)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1178)
                  if (id == 1176)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1177)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1178)
                  if (id == 1179)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1180)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1181)
                if (id == 1184)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1184)
                  if (id == 1182)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1183)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1184)
                  if (id == 1185)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1186)
            if (id == 1197)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1197)
              if (id == 1192)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1192)
                if (id == 1189)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1189)
                  if (id == 1187)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1188)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1189)
                  if (id == 1190)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1191)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1192)
                if (id == 1195)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1195)
                  if (id == 1193)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1194)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1195)
                  if (id == 1196)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1197)
              if (id == 1202)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1202)
                if (id == 1200)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1200)
                  if (id == 1198)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1199)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1200)
                  if (id == 1201)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1202)
                if (id == 1205)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1205)
                  if (id == 1203)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1204)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1205)
                  if (id == 1206)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1207)
      if (id == 1294)
        Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1294)
        if (id == 1251)
          Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1251)
          if (id == 1229)
            Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1229)
            if (id == 1218)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1218)
              if (id == 1213)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1213)
                if (id == 1210)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1210)
                  if (id == 1208)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1209)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1210)
                  if (id == 1211)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1212)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1213)
                if (id == 1216)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1216)
                  if (id == 1214)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1215)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1216)
                  if (id == 1217)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1218)
              if (id == 1224)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1224)
                if (id == 1221)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1221)
                  if (id == 1219)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1220)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1221)
                  if (id == 1222)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1223)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1224)
                if (id == 1227)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1227)
                  if (id == 1225)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1226)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1227)
                  if (id == 1228)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1229)
            if (id == 1240)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1240)
              if (id == 1235)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1235)
                if (id == 1232)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1232)
                  if (id == 1230)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1231)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1232)
                  if (id == 1233)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1234)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1235)
                if (id == 1238)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1238)
                  if (id == 1236)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1237)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1238)
                  if (id == 1239)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1240)
              if (id == 1246)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1246)
                if (id == 1243)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1243)
                  if (id == 1241)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1242)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1243)
                  if (id == 1244)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1245)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1246)
                if (id == 1249)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1249)
                  if (id == 1247)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1248)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1249)
                  if (id == 1250)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 1251)
          if (id == 1273)
            Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1273)
            if (id == 1262)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1262)
              if (id == 1257)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1257)
                if (id == 1254)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1254)
                  if (id == 1252)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1253)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1254)
                  if (id == 1255)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1256)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1257)
                if (id == 1260)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1260)
                  if (id == 1258)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1259)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1260)
                  if (id == 1261)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1262)
              if (id == 1268)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1268)
                if (id == 1265)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1265)
                  if (id == 1263)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1264)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1265)
                  if (id == 1266)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1267)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1268)
                if (id == 1271)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1271)
                  if (id == 1269)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1270)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1271)
                  if (id == 1272)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1273)
            if (id == 1284)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1284)
              if (id == 1279)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1279)
                if (id == 1276)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1276)
                  if (id == 1274)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1275)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1276)
                  if (id == 1277)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1278)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1279)
                if (id == 1282)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1282)
                  if (id == 1280)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1281)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1282)
                  if (id == 1283)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1284)
              if (id == 1289)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1289)
                if (id == 1287)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1287)
                  if (id == 1285)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1286)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1287)
                  if (id == 1288)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1289)
                if (id == 1292)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1292)
                  if (id == 1290)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1291)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1292)
                  if (id == 1293)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 1294)
        if (id == 1337)
          Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1337)
          if (id == 1316)
            Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1316)
            if (id == 1305)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1305)
              if (id == 1300)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1300)
                if (id == 1297)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1297)
                  if (id == 1295)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1296)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1297)
                  if (id == 1298)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1299)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1300)
                if (id == 1303)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1303)
                  if (id == 1301)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1302)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1303)
                  if (id == 1304)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1305)
              if (id == 1311)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1311)
                if (id == 1308)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1308)
                  if (id == 1306)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1307)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1308)
                  if (id == 1309)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1310)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1311)
                if (id == 1314)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1314)
                  if (id == 1312)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1313)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1314)
                  if (id == 1315)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1316)
            if (id == 1327)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1327)
              if (id == 1322)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1322)
                if (id == 1319)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1319)
                  if (id == 1317)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1318)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1319)
                  if (id == 1320)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1321)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1322)
                if (id == 1325)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1325)
                  if (id == 1323)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1324)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1325)
                  if (id == 1326)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1327)
              if (id == 1332)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1332)
                if (id == 1330)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1330)
                  if (id == 1328)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1329)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1330)
                  if (id == 1331)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1332)
                if (id == 1335)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1335)
                  if (id == 1333)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1334)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1335)
                  if (id == 1336)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 1337)
          if (id == 1359)
            Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1359)
            if (id == 1348)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1348)
              if (id == 1343)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1343)
                if (id == 1340)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1340)
                  if (id == 1338)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1339)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1340)
                  if (id == 1341)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1342)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1343)
                if (id == 1346)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1346)
                  if (id == 1344)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1345)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1346)
                  if (id == 1347)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1348)
              if (id == 1354)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1354)
                if (id == 1351)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1351)
                  if (id == 1349)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1350)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1351)
                  if (id == 1352)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1353)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1354)
                if (id == 1357)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1357)
                  if (id == 1355)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1356)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1357)
                  if (id == 1358)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1359)
            if (id == 1370)
              Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1370)
              if (id == 1365)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1365)
                if (id == 1362)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1362)
                  if (id == 1360)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1361)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1362)
                  if (id == 1363)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1364)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1365)
                if (id == 1368)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1368)
                  if (id == 1366)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1367)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1368)
                  if (id == 1369)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1370)
              if (id == 1375)
                Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1375)
                if (id == 1373)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1373)
                  if (id == 1371)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1372)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1373)
                  if (id == 1374)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1375)
                if (id == 1378)
                  Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1378)
                  if (id == 1376)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1377)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1378)
                  if (id == 1379)
                    Some(reader(BaseEntryId.FormatA.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 1035
  val maxID = 1379
}