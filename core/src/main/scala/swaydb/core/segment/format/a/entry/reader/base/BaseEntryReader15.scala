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

object BaseEntryReader15 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1435)
      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1435)
      if (id == 1410)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1410)
        if (id == 1398)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1398)
          if (id == 1392)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1392)
            if (id == 1389)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1389)
              if (id == 1387)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1387)
                if (id == 1386)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1387)
                if (id == 1388)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1389)
              if (id == 1390)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1391)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1392)
            if (id == 1395)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1395)
              if (id == 1393)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1394)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1395)
              if (id == 1396)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1397)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1398)
          if (id == 1404)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1404)
            if (id == 1401)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1401)
              if (id == 1399)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1400)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1401)
              if (id == 1402)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1403)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1404)
            if (id == 1407)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1407)
              if (id == 1405)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1406)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1407)
              if (id == 1408)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1409)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1410)
        if (id == 1423)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1423)
          if (id == 1417)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1417)
            if (id == 1414)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1414)
              if (id == 1412)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1412)
                if (id == 1411)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1412)
                if (id == 1413)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1414)
              if (id == 1415)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1416)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1417)
            if (id == 1420)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1420)
              if (id == 1418)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1419)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1420)
              if (id == 1421)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1422)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1423)
          if (id == 1429)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1429)
            if (id == 1426)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1426)
              if (id == 1424)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1425)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1426)
              if (id == 1427)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1428)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1429)
            if (id == 1432)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1432)
              if (id == 1430)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1431)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1432)
              if (id == 1433)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1434)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1435)
      if (id == 1460)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1460)
        if (id == 1448)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1448)
          if (id == 1442)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1442)
            if (id == 1439)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1439)
              if (id == 1437)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1437)
                if (id == 1436)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1437)
                if (id == 1438)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1439)
              if (id == 1440)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1441)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1442)
            if (id == 1445)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1445)
              if (id == 1443)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1444)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1445)
              if (id == 1446)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1447)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1448)
          if (id == 1454)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1454)
            if (id == 1451)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1451)
              if (id == 1449)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1450)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1451)
              if (id == 1452)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1453)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1454)
            if (id == 1457)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1457)
              if (id == 1455)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1456)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1457)
              if (id == 1458)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1459)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1460)
        if (id == 1473)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1473)
          if (id == 1467)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1467)
            if (id == 1464)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1464)
              if (id == 1462)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1462)
                if (id == 1461)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1462)
                if (id == 1463)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1464)
              if (id == 1465)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1466)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1467)
            if (id == 1470)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1470)
              if (id == 1468)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1469)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1470)
              if (id == 1471)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1472)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1473)
          if (id == 1479)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1479)
            if (id == 1476)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1476)
              if (id == 1474)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1475)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1476)
              if (id == 1477)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1478)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1479)
            if (id == 1482)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1482)
              if (id == 1480)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1481)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1482)
              if (id == 1483)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1484)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 1386
  val maxID = 1484
}