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

object BaseEntryReader11 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1039)
      Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1039)
      if (id == 1014)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1014)
        if (id == 1002)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1002)
          if (id == 996)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 996)
            if (id == 993)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 993)
              if (id == 991)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 991)
                if (id == 990)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 991)
                if (id == 992)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 993)
              if (id == 994)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 995)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 996)
            if (id == 999)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 999)
              if (id == 997)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 998)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 999)
              if (id == 1000)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1001)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1002)
          if (id == 1008)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1008)
            if (id == 1005)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1005)
              if (id == 1003)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1004)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1005)
              if (id == 1006)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1007)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1008)
            if (id == 1011)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1011)
              if (id == 1009)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1010)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1011)
              if (id == 1012)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1013)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1014)
        if (id == 1027)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1027)
          if (id == 1021)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1021)
            if (id == 1018)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1018)
              if (id == 1016)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1016)
                if (id == 1015)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1016)
                if (id == 1017)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1018)
              if (id == 1019)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1020)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1021)
            if (id == 1024)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1024)
              if (id == 1022)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1023)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1024)
              if (id == 1025)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1026)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1027)
          if (id == 1033)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1033)
            if (id == 1030)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1030)
              if (id == 1028)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1029)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1030)
              if (id == 1031)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1032)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1033)
            if (id == 1036)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1036)
              if (id == 1034)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1035)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1036)
              if (id == 1037)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1038)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1039)
      if (id == 1064)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1064)
        if (id == 1052)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1052)
          if (id == 1046)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1046)
            if (id == 1043)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1043)
              if (id == 1041)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1041)
                if (id == 1040)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1041)
                if (id == 1042)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1043)
              if (id == 1044)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1045)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1046)
            if (id == 1049)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1049)
              if (id == 1047)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1048)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1049)
              if (id == 1050)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1051)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1052)
          if (id == 1058)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1058)
            if (id == 1055)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1055)
              if (id == 1053)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1054)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1055)
              if (id == 1056)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1057)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1058)
            if (id == 1061)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1061)
              if (id == 1059)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1060)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1061)
              if (id == 1062)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1063)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1064)
        if (id == 1077)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1077)
          if (id == 1071)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1071)
            if (id == 1068)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1068)
              if (id == 1066)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1066)
                if (id == 1065)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1066)
                if (id == 1067)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1068)
              if (id == 1069)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1070)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1071)
            if (id == 1074)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1074)
              if (id == 1072)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1073)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1074)
              if (id == 1075)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1076)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1077)
          if (id == 1083)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1083)
            if (id == 1080)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1080)
              if (id == 1078)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1079)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1080)
              if (id == 1081)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1082)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1083)
            if (id == 1086)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1086)
              if (id == 1084)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1085)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1086)
              if (id == 1087)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1088)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 990
  val maxID = 1088
}