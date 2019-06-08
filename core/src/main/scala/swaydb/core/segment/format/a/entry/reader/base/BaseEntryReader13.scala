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

object BaseEntryReader13 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1237)
      Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1237)
      if (id == 1212)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1212)
        if (id == 1200)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1200)
          if (id == 1194)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1194)
            if (id == 1191)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1191)
              if (id == 1189)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1189)
                if (id == 1188)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1189)
                if (id == 1190)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1191)
              if (id == 1192)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1193)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1194)
            if (id == 1197)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1197)
              if (id == 1195)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1196)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1197)
              if (id == 1198)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1199)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1200)
          if (id == 1206)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1206)
            if (id == 1203)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1203)
              if (id == 1201)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1202)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1203)
              if (id == 1204)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1205)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1206)
            if (id == 1209)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1209)
              if (id == 1207)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1208)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1209)
              if (id == 1210)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1211)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1212)
        if (id == 1225)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1225)
          if (id == 1219)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1219)
            if (id == 1216)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1216)
              if (id == 1214)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1214)
                if (id == 1213)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1214)
                if (id == 1215)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1216)
              if (id == 1217)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1218)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1219)
            if (id == 1222)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1222)
              if (id == 1220)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1221)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1222)
              if (id == 1223)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1224)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1225)
          if (id == 1231)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1231)
            if (id == 1228)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1228)
              if (id == 1226)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1227)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1228)
              if (id == 1229)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1230)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1231)
            if (id == 1234)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1234)
              if (id == 1232)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1233)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1234)
              if (id == 1235)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1236)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1237)
      if (id == 1262)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1262)
        if (id == 1250)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1250)
          if (id == 1244)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1244)
            if (id == 1241)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1241)
              if (id == 1239)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1239)
                if (id == 1238)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1239)
                if (id == 1240)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1241)
              if (id == 1242)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1243)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1244)
            if (id == 1247)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1247)
              if (id == 1245)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1246)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1247)
              if (id == 1248)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1249)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1250)
          if (id == 1256)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1256)
            if (id == 1253)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1253)
              if (id == 1251)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1252)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1253)
              if (id == 1254)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1255)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1256)
            if (id == 1259)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1259)
              if (id == 1257)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1258)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1259)
              if (id == 1260)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1261)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1262)
        if (id == 1275)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1275)
          if (id == 1269)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1269)
            if (id == 1266)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1266)
              if (id == 1264)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1264)
                if (id == 1263)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1264)
                if (id == 1265)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1266)
              if (id == 1267)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1268)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1269)
            if (id == 1272)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1272)
              if (id == 1270)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1271)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1272)
              if (id == 1273)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1274)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1275)
          if (id == 1281)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1281)
            if (id == 1278)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1278)
              if (id == 1276)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1277)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1278)
              if (id == 1279)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1280)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1281)
            if (id == 1284)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1284)
              if (id == 1282)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1283)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1284)
              if (id == 1285)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1286)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 1188
  val maxID = 1286
}