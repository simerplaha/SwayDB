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
import swaydb.core.util.cache.Cache
import swaydb.data.io.Core
import swaydb.data.slice.Reader

private[core] object BaseEntryReader4 extends BaseEntryReader {

  def read[T](baseId: Int,
              keyValueId: Int,
              indexReader: Reader[swaydb.Error.Segment],
              valueCache: Option[Cache[swaydb.Error.Segment, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]]],
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              accessPosition: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[swaydb.Error.Segment, T]] =
  //GENERATED CONDITIONS
    if (baseId == 1207)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
    else if (baseId < 1207)
      if (baseId == 1121)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
      else if (baseId < 1121)
        if (baseId == 1078)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 1078)
          if (baseId == 1056)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1056)
            if (baseId == 1045)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1045)
              if (baseId == 1040)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1040)
                if (baseId == 1037)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1037)
                  if (baseId == 1035)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1036)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1037)
                  if (baseId == 1038)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1039)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1040)
                if (baseId == 1043)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1043)
                  if (baseId == 1041)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1042)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1043)
                  if (baseId == 1044)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1045)
              if (baseId == 1051)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1051)
                if (baseId == 1048)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1048)
                  if (baseId == 1046)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1047)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1048)
                  if (baseId == 1049)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1050)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1051)
                if (baseId == 1054)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1054)
                  if (baseId == 1052)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1053)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1054)
                  if (baseId == 1055)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1056)
            if (baseId == 1067)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1067)
              if (baseId == 1062)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1062)
                if (baseId == 1059)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1059)
                  if (baseId == 1057)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1058)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1059)
                  if (baseId == 1060)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1061)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1062)
                if (baseId == 1065)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1065)
                  if (baseId == 1063)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1064)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1065)
                  if (baseId == 1066)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1067)
              if (baseId == 1073)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1073)
                if (baseId == 1070)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1070)
                  if (baseId == 1068)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1069)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1070)
                  if (baseId == 1071)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1072)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1073)
                if (baseId == 1076)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1076)
                  if (baseId == 1074)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1075)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1076)
                  if (baseId == 1077)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 1078)
          if (baseId == 1100)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1100)
            if (baseId == 1089)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1089)
              if (baseId == 1084)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1084)
                if (baseId == 1081)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1081)
                  if (baseId == 1079)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1080)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1081)
                  if (baseId == 1082)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1083)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1084)
                if (baseId == 1087)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1087)
                  if (baseId == 1085)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1086)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1087)
                  if (baseId == 1088)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1089)
              if (baseId == 1095)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1095)
                if (baseId == 1092)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1092)
                  if (baseId == 1090)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1091)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1092)
                  if (baseId == 1093)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1094)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1095)
                if (baseId == 1098)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1098)
                  if (baseId == 1096)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1097)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1098)
                  if (baseId == 1099)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1100)
            if (baseId == 1111)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1111)
              if (baseId == 1106)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1106)
                if (baseId == 1103)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1103)
                  if (baseId == 1101)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1102)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1103)
                  if (baseId == 1104)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1105)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1106)
                if (baseId == 1109)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1109)
                  if (baseId == 1107)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1108)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1109)
                  if (baseId == 1110)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1111)
              if (baseId == 1116)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1116)
                if (baseId == 1114)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1114)
                  if (baseId == 1112)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1113)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1114)
                  if (baseId == 1115)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1116)
                if (baseId == 1119)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1119)
                  if (baseId == 1117)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1118)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1119)
                  if (baseId == 1120)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
      else if (baseId > 1121)
        if (baseId == 1164)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 1164)
          if (baseId == 1143)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1143)
            if (baseId == 1132)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1132)
              if (baseId == 1127)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1127)
                if (baseId == 1124)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1124)
                  if (baseId == 1122)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1123)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1124)
                  if (baseId == 1125)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1126)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1127)
                if (baseId == 1130)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1130)
                  if (baseId == 1128)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1129)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1130)
                  if (baseId == 1131)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1132)
              if (baseId == 1138)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1138)
                if (baseId == 1135)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1135)
                  if (baseId == 1133)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1134)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1135)
                  if (baseId == 1136)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1137)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1138)
                if (baseId == 1141)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1141)
                  if (baseId == 1139)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1140)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1141)
                  if (baseId == 1142)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1143)
            if (baseId == 1154)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1154)
              if (baseId == 1149)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1149)
                if (baseId == 1146)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1146)
                  if (baseId == 1144)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1145)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1146)
                  if (baseId == 1147)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1148)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1149)
                if (baseId == 1152)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1152)
                  if (baseId == 1150)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1151)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1152)
                  if (baseId == 1153)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1154)
              if (baseId == 1159)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1159)
                if (baseId == 1157)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1157)
                  if (baseId == 1155)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1156)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1157)
                  if (baseId == 1158)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1159)
                if (baseId == 1162)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1162)
                  if (baseId == 1160)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1161)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1162)
                  if (baseId == 1163)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 1164)
          if (baseId == 1186)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1186)
            if (baseId == 1175)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1175)
              if (baseId == 1170)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1170)
                if (baseId == 1167)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1167)
                  if (baseId == 1165)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1166)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1167)
                  if (baseId == 1168)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1169)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1170)
                if (baseId == 1173)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1173)
                  if (baseId == 1171)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1172)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1173)
                  if (baseId == 1174)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1175)
              if (baseId == 1181)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1181)
                if (baseId == 1178)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1178)
                  if (baseId == 1176)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1177)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1178)
                  if (baseId == 1179)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1180)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1181)
                if (baseId == 1184)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1184)
                  if (baseId == 1182)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1183)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1184)
                  if (baseId == 1185)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1186)
            if (baseId == 1197)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1197)
              if (baseId == 1192)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1192)
                if (baseId == 1189)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1189)
                  if (baseId == 1187)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1188)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1189)
                  if (baseId == 1190)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1191)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1192)
                if (baseId == 1195)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1195)
                  if (baseId == 1193)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1194)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1195)
                  if (baseId == 1196)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1197)
              if (baseId == 1202)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1202)
                if (baseId == 1200)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1200)
                  if (baseId == 1198)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1199)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1200)
                  if (baseId == 1201)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1202)
                if (baseId == 1205)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1205)
                  if (baseId == 1203)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1204)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1205)
                  if (baseId == 1206)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
    else if (baseId > 1207)
      if (baseId == 1294)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
      else if (baseId < 1294)
        if (baseId == 1251)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 1251)
          if (baseId == 1229)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1229)
            if (baseId == 1218)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1218)
              if (baseId == 1213)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1213)
                if (baseId == 1210)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1210)
                  if (baseId == 1208)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1209)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1210)
                  if (baseId == 1211)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1212)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1213)
                if (baseId == 1216)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1216)
                  if (baseId == 1214)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1215)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1216)
                  if (baseId == 1217)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1218)
              if (baseId == 1224)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1224)
                if (baseId == 1221)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1221)
                  if (baseId == 1219)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1220)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1221)
                  if (baseId == 1222)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1223)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1224)
                if (baseId == 1227)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1227)
                  if (baseId == 1225)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1226)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1227)
                  if (baseId == 1228)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1229)
            if (baseId == 1240)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1240)
              if (baseId == 1235)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1235)
                if (baseId == 1232)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1232)
                  if (baseId == 1230)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1231)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1232)
                  if (baseId == 1233)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1234)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1235)
                if (baseId == 1238)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1238)
                  if (baseId == 1236)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1237)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1238)
                  if (baseId == 1239)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1240)
              if (baseId == 1246)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1246)
                if (baseId == 1243)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1243)
                  if (baseId == 1241)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1242)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1243)
                  if (baseId == 1244)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1245)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1246)
                if (baseId == 1249)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1249)
                  if (baseId == 1247)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1248)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1249)
                  if (baseId == 1250)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 1251)
          if (baseId == 1273)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1273)
            if (baseId == 1262)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1262)
              if (baseId == 1257)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1257)
                if (baseId == 1254)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1254)
                  if (baseId == 1252)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1253)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1254)
                  if (baseId == 1255)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1256)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1257)
                if (baseId == 1260)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1260)
                  if (baseId == 1258)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1259)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1260)
                  if (baseId == 1261)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1262)
              if (baseId == 1268)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1268)
                if (baseId == 1265)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1265)
                  if (baseId == 1263)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1264)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1265)
                  if (baseId == 1266)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1267)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1268)
                if (baseId == 1271)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1271)
                  if (baseId == 1269)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1270)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1271)
                  if (baseId == 1272)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1273)
            if (baseId == 1284)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1284)
              if (baseId == 1279)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1279)
                if (baseId == 1276)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1276)
                  if (baseId == 1274)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1275)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1276)
                  if (baseId == 1277)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1278)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1279)
                if (baseId == 1282)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1282)
                  if (baseId == 1280)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1281)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1282)
                  if (baseId == 1283)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1284)
              if (baseId == 1289)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1289)
                if (baseId == 1287)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1287)
                  if (baseId == 1285)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1286)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1287)
                  if (baseId == 1288)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1289)
                if (baseId == 1292)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1292)
                  if (baseId == 1290)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1291)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1292)
                  if (baseId == 1293)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
      else if (baseId > 1294)
        if (baseId == 1337)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 1337)
          if (baseId == 1316)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1316)
            if (baseId == 1305)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1305)
              if (baseId == 1300)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1300)
                if (baseId == 1297)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1297)
                  if (baseId == 1295)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1296)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1297)
                  if (baseId == 1298)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1299)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1300)
                if (baseId == 1303)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1303)
                  if (baseId == 1301)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1302)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1303)
                  if (baseId == 1304)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1305)
              if (baseId == 1311)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1311)
                if (baseId == 1308)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1308)
                  if (baseId == 1306)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1307)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1308)
                  if (baseId == 1309)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1310)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1311)
                if (baseId == 1314)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1314)
                  if (baseId == 1312)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1313)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1314)
                  if (baseId == 1315)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1316)
            if (baseId == 1327)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1327)
              if (baseId == 1322)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1322)
                if (baseId == 1319)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1319)
                  if (baseId == 1317)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1318)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1319)
                  if (baseId == 1320)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1321)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1322)
                if (baseId == 1325)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1325)
                  if (baseId == 1323)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1324)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1325)
                  if (baseId == 1326)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1327)
              if (baseId == 1332)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1332)
                if (baseId == 1330)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1330)
                  if (baseId == 1328)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1329)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1330)
                  if (baseId == 1331)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1332)
                if (baseId == 1335)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1335)
                  if (baseId == 1333)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1334)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1335)
                  if (baseId == 1336)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 1337)
          if (baseId == 1359)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1359)
            if (baseId == 1348)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1348)
              if (baseId == 1343)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1343)
                if (baseId == 1340)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1340)
                  if (baseId == 1338)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1339)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1340)
                  if (baseId == 1341)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1342)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1343)
                if (baseId == 1346)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1346)
                  if (baseId == 1344)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1345)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1346)
                  if (baseId == 1347)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1348)
              if (baseId == 1354)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1354)
                if (baseId == 1351)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1351)
                  if (baseId == 1349)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1350)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1351)
                  if (baseId == 1352)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1353)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1354)
                if (baseId == 1357)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1357)
                  if (baseId == 1355)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1356)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1357)
                  if (baseId == 1358)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1359)
            if (baseId == 1370)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1370)
              if (baseId == 1365)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1365)
                if (baseId == 1362)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1362)
                  if (baseId == 1360)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1361)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1362)
                  if (baseId == 1363)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1364)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1365)
                if (baseId == 1368)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1368)
                  if (baseId == 1366)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1367)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1368)
                  if (baseId == 1369)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1370)
              if (baseId == 1375)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1375)
                if (baseId == 1373)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1373)
                  if (baseId == 1371)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1372)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1373)
                  if (baseId == 1374)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1375)
                if (baseId == 1378)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1378)
                  if (baseId == 1376)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1377)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1378)
                  if (baseId == 1379)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueCache, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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