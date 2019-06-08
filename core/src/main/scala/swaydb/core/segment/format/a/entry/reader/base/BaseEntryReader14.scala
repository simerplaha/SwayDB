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

object BaseEntryReader14 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1336)
      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1336)
      if (id == 1311)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1311)
        if (id == 1299)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1299)
          if (id == 1293)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1293)
            if (id == 1290)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1290)
              if (id == 1288)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1288)
                if (id == 1287)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1288)
                if (id == 1289)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1290)
              if (id == 1291)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1292)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1293)
            if (id == 1296)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1296)
              if (id == 1294)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1295)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1296)
              if (id == 1297)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1298)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1299)
          if (id == 1305)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1305)
            if (id == 1302)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1302)
              if (id == 1300)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1301)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1302)
              if (id == 1303)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1304)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1305)
            if (id == 1308)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1308)
              if (id == 1306)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1307)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1308)
              if (id == 1309)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1310)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1311)
        if (id == 1324)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1324)
          if (id == 1318)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1318)
            if (id == 1315)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1315)
              if (id == 1313)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1313)
                if (id == 1312)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1313)
                if (id == 1314)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1315)
              if (id == 1316)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1317)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1318)
            if (id == 1321)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1321)
              if (id == 1319)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1320)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1321)
              if (id == 1322)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1323)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1324)
          if (id == 1330)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1330)
            if (id == 1327)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1327)
              if (id == 1325)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1326)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1327)
              if (id == 1328)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1329)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1330)
            if (id == 1333)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1333)
              if (id == 1331)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1332)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1333)
              if (id == 1334)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1335)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1336)
      if (id == 1361)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1361)
        if (id == 1349)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1349)
          if (id == 1343)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1343)
            if (id == 1340)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1340)
              if (id == 1338)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1338)
                if (id == 1337)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1338)
                if (id == 1339)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1340)
              if (id == 1341)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1342)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1343)
            if (id == 1346)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1346)
              if (id == 1344)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1345)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1346)
              if (id == 1347)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1348)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1349)
          if (id == 1355)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1355)
            if (id == 1352)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1352)
              if (id == 1350)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1351)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1352)
              if (id == 1353)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1354)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1355)
            if (id == 1358)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1358)
              if (id == 1356)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1357)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1358)
              if (id == 1359)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1360)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1361)
        if (id == 1374)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1374)
          if (id == 1368)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1368)
            if (id == 1365)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1365)
              if (id == 1363)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1363)
                if (id == 1362)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1363)
                if (id == 1364)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1365)
              if (id == 1366)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1367)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1368)
            if (id == 1371)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1371)
              if (id == 1369)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1370)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1371)
              if (id == 1372)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1373)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1374)
          if (id == 1380)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1380)
            if (id == 1377)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1377)
              if (id == 1375)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1376)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1377)
              if (id == 1378)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1379)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1380)
            if (id == 1383)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1383)
              if (id == 1381)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1382)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1383)
              if (id == 1384)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1385)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 1287
  val maxID = 1385
}