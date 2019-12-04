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

private[core] object BaseEntryReader4 extends BaseEntryReader {

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
    if (baseId == 1207)
      reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
    else if (baseId < 1207)
      if (baseId == 1121)
        reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
      else if (baseId < 1121)
        if (baseId == 1078)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else if (baseId < 1078)
          if (baseId == 1056)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else if (baseId < 1056)
            if (baseId == 1045)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1045)
              if (baseId == 1040)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1040)
                if (baseId == 1037)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1037)
                  if (baseId == 1035)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1036)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1037)
                  if (baseId == 1038)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1039)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1040)
                if (baseId == 1043)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1043)
                  if (baseId == 1041)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1042)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1043)
                  if (baseId == 1044)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1045)
              if (baseId == 1051)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1051)
                if (baseId == 1048)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1048)
                  if (baseId == 1046)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1047)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1048)
                  if (baseId == 1049)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1050)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1051)
                if (baseId == 1054)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1054)
                  if (baseId == 1052)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1053)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1054)
                  if (baseId == 1055)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 1056)
            if (baseId == 1067)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1067)
              if (baseId == 1062)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1062)
                if (baseId == 1059)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1059)
                  if (baseId == 1057)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1058)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1059)
                  if (baseId == 1060)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1061)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1062)
                if (baseId == 1065)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1065)
                  if (baseId == 1063)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1064)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1065)
                  if (baseId == 1066)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1067)
              if (baseId == 1073)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1073)
                if (baseId == 1070)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1070)
                  if (baseId == 1068)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1069)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1070)
                  if (baseId == 1071)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1072)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1073)
                if (baseId == 1076)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1076)
                  if (baseId == 1074)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1075)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1076)
                  if (baseId == 1077)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 1078)
          if (baseId == 1100)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else if (baseId < 1100)
            if (baseId == 1089)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1089)
              if (baseId == 1084)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1084)
                if (baseId == 1081)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1081)
                  if (baseId == 1079)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1080)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1081)
                  if (baseId == 1082)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1083)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1084)
                if (baseId == 1087)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1087)
                  if (baseId == 1085)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1086)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1087)
                  if (baseId == 1088)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1089)
              if (baseId == 1095)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1095)
                if (baseId == 1092)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1092)
                  if (baseId == 1090)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1091)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1092)
                  if (baseId == 1093)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1094)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1095)
                if (baseId == 1098)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1098)
                  if (baseId == 1096)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1097)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1098)
                  if (baseId == 1099)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 1100)
            if (baseId == 1111)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1111)
              if (baseId == 1106)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1106)
                if (baseId == 1103)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1103)
                  if (baseId == 1101)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1102)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1103)
                  if (baseId == 1104)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1105)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1106)
                if (baseId == 1109)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1109)
                  if (baseId == 1107)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1108)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1109)
                  if (baseId == 1110)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1111)
              if (baseId == 1116)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1116)
                if (baseId == 1114)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1114)
                  if (baseId == 1112)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1113)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1114)
                  if (baseId == 1115)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1116)
                if (baseId == 1119)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1119)
                  if (baseId == 1117)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1118)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1119)
                  if (baseId == 1120)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else if (baseId > 1121)
        if (baseId == 1164)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else if (baseId < 1164)
          if (baseId == 1143)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else if (baseId < 1143)
            if (baseId == 1132)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1132)
              if (baseId == 1127)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1127)
                if (baseId == 1124)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1124)
                  if (baseId == 1122)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1123)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1124)
                  if (baseId == 1125)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1126)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1127)
                if (baseId == 1130)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1130)
                  if (baseId == 1128)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1129)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1130)
                  if (baseId == 1131)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1132)
              if (baseId == 1138)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1138)
                if (baseId == 1135)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1135)
                  if (baseId == 1133)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1134)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1135)
                  if (baseId == 1136)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1137)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1138)
                if (baseId == 1141)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1141)
                  if (baseId == 1139)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1140)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1141)
                  if (baseId == 1142)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 1143)
            if (baseId == 1154)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1154)
              if (baseId == 1149)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1149)
                if (baseId == 1146)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1146)
                  if (baseId == 1144)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1145)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1146)
                  if (baseId == 1147)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1148)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1149)
                if (baseId == 1152)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1152)
                  if (baseId == 1150)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1151)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1152)
                  if (baseId == 1153)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1154)
              if (baseId == 1159)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1159)
                if (baseId == 1157)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1157)
                  if (baseId == 1155)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1156)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1157)
                  if (baseId == 1158)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1159)
                if (baseId == 1162)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1162)
                  if (baseId == 1160)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1161)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1162)
                  if (baseId == 1163)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 1164)
          if (baseId == 1186)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else if (baseId < 1186)
            if (baseId == 1175)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1175)
              if (baseId == 1170)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1170)
                if (baseId == 1167)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1167)
                  if (baseId == 1165)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1166)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1167)
                  if (baseId == 1168)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1169)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1170)
                if (baseId == 1173)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1173)
                  if (baseId == 1171)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1172)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1173)
                  if (baseId == 1174)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1175)
              if (baseId == 1181)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1181)
                if (baseId == 1178)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1178)
                  if (baseId == 1176)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1177)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1178)
                  if (baseId == 1179)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1180)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1181)
                if (baseId == 1184)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1184)
                  if (baseId == 1182)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1183)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1184)
                  if (baseId == 1185)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 1186)
            if (baseId == 1197)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1197)
              if (baseId == 1192)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1192)
                if (baseId == 1189)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1189)
                  if (baseId == 1187)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1188)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1189)
                  if (baseId == 1190)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1191)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1192)
                if (baseId == 1195)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1195)
                  if (baseId == 1193)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1194)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1195)
                  if (baseId == 1196)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1197)
              if (baseId == 1202)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1202)
                if (baseId == 1200)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1200)
                  if (baseId == 1198)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1199)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1200)
                  if (baseId == 1201)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1202)
                if (baseId == 1205)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1205)
                  if (baseId == 1203)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1204)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1205)
                  if (baseId == 1206)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else
        throw swaydb.Exception.InvalidKeyValueId(baseId)
    else if (baseId > 1207)
      if (baseId == 1294)
        reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
      else if (baseId < 1294)
        if (baseId == 1251)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else if (baseId < 1251)
          if (baseId == 1229)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else if (baseId < 1229)
            if (baseId == 1218)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1218)
              if (baseId == 1213)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1213)
                if (baseId == 1210)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1210)
                  if (baseId == 1208)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1209)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1210)
                  if (baseId == 1211)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1212)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1213)
                if (baseId == 1216)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1216)
                  if (baseId == 1214)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1215)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1216)
                  if (baseId == 1217)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1218)
              if (baseId == 1224)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1224)
                if (baseId == 1221)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1221)
                  if (baseId == 1219)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1220)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1221)
                  if (baseId == 1222)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1223)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1224)
                if (baseId == 1227)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1227)
                  if (baseId == 1225)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1226)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1227)
                  if (baseId == 1228)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 1229)
            if (baseId == 1240)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1240)
              if (baseId == 1235)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1235)
                if (baseId == 1232)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1232)
                  if (baseId == 1230)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1231)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1232)
                  if (baseId == 1233)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1234)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1235)
                if (baseId == 1238)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1238)
                  if (baseId == 1236)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1237)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1238)
                  if (baseId == 1239)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1240)
              if (baseId == 1246)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1246)
                if (baseId == 1243)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1243)
                  if (baseId == 1241)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1242)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1243)
                  if (baseId == 1244)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1245)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1246)
                if (baseId == 1249)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1249)
                  if (baseId == 1247)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1248)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1249)
                  if (baseId == 1250)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 1251)
          if (baseId == 1273)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else if (baseId < 1273)
            if (baseId == 1262)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1262)
              if (baseId == 1257)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1257)
                if (baseId == 1254)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1254)
                  if (baseId == 1252)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1253)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1254)
                  if (baseId == 1255)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1256)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1257)
                if (baseId == 1260)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1260)
                  if (baseId == 1258)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1259)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1260)
                  if (baseId == 1261)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1262)
              if (baseId == 1268)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1268)
                if (baseId == 1265)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1265)
                  if (baseId == 1263)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1264)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1265)
                  if (baseId == 1266)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1267)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1268)
                if (baseId == 1271)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1271)
                  if (baseId == 1269)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1270)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1271)
                  if (baseId == 1272)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 1273)
            if (baseId == 1284)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1284)
              if (baseId == 1279)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1279)
                if (baseId == 1276)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1276)
                  if (baseId == 1274)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1275)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1276)
                  if (baseId == 1277)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1278)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1279)
                if (baseId == 1282)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1282)
                  if (baseId == 1280)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1281)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1282)
                  if (baseId == 1283)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1284)
              if (baseId == 1289)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1289)
                if (baseId == 1287)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1287)
                  if (baseId == 1285)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1286)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1287)
                  if (baseId == 1288)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1289)
                if (baseId == 1292)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1292)
                  if (baseId == 1290)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1291)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1292)
                  if (baseId == 1293)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else if (baseId > 1294)
        if (baseId == 1337)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
        else if (baseId < 1337)
          if (baseId == 1316)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else if (baseId < 1316)
            if (baseId == 1305)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1305)
              if (baseId == 1300)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1300)
                if (baseId == 1297)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1297)
                  if (baseId == 1295)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1296)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1297)
                  if (baseId == 1298)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1299)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1300)
                if (baseId == 1303)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1303)
                  if (baseId == 1301)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1302)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1303)
                  if (baseId == 1304)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1305)
              if (baseId == 1311)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1311)
                if (baseId == 1308)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1308)
                  if (baseId == 1306)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1307)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1308)
                  if (baseId == 1309)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1310)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1311)
                if (baseId == 1314)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1314)
                  if (baseId == 1312)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1313)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1314)
                  if (baseId == 1315)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 1316)
            if (baseId == 1327)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1327)
              if (baseId == 1322)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1322)
                if (baseId == 1319)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1319)
                  if (baseId == 1317)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1318)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1319)
                  if (baseId == 1320)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1321)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1322)
                if (baseId == 1325)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1325)
                  if (baseId == 1323)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1324)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1325)
                  if (baseId == 1326)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1327)
              if (baseId == 1332)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1332)
                if (baseId == 1330)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1330)
                  if (baseId == 1328)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1329)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1330)
                  if (baseId == 1331)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1332)
                if (baseId == 1335)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1335)
                  if (baseId == 1333)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1334)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1335)
                  if (baseId == 1336)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 1337)
          if (baseId == 1359)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
          else if (baseId < 1359)
            if (baseId == 1348)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1348)
              if (baseId == 1343)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1343)
                if (baseId == 1340)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1340)
                  if (baseId == 1338)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1339)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1340)
                  if (baseId == 1341)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1342)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1343)
                if (baseId == 1346)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1346)
                  if (baseId == 1344)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1345)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1346)
                  if (baseId == 1347)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1348)
              if (baseId == 1354)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1354)
                if (baseId == 1351)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1351)
                  if (baseId == 1349)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1350)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1351)
                  if (baseId == 1352)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1353)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1354)
                if (baseId == 1357)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1357)
                  if (baseId == 1355)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1356)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1357)
                  if (baseId == 1358)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 1359)
            if (baseId == 1370)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
            else if (baseId < 1370)
              if (baseId == 1365)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1365)
                if (baseId == 1362)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1362)
                  if (baseId == 1360)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1361)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1362)
                  if (baseId == 1363)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1364)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1365)
                if (baseId == 1368)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1368)
                  if (baseId == 1366)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1367)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1368)
                  if (baseId == 1369)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 1370)
              if (baseId == 1375)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
              else if (baseId < 1375)
                if (baseId == 1373)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1373)
                  if (baseId == 1371)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1372)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1373)
                  if (baseId == 1374)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 1375)
                if (baseId == 1378)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                else if (baseId < 1378)
                  if (baseId == 1376)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else if (baseId == 1377)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 1378)
                  if (baseId == 1379)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexEndOffset, sortedIndexAccessPosition, headerKeyBytes, indexReader, valuesReader, indexOffset, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else
        throw swaydb.Exception.InvalidKeyValueId(baseId)
    else
      throw swaydb.Exception.InvalidKeyValueId(baseId)

  val minID = 1035
  val maxID = 1379
}