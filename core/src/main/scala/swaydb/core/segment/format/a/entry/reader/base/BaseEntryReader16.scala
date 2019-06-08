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


object BaseEntryReader16 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1534)
      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1534)
      if (id == 1509)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1509)
        if (id == 1497)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1497)
          if (id == 1491)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1491)
            if (id == 1488)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1488)
              if (id == 1486)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1486)
                if (id == 1485)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1486)
                if (id == 1487)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1488)
              if (id == 1489)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1490)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1491)
            if (id == 1494)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1494)
              if (id == 1492)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1493)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1494)
              if (id == 1495)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1496)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1497)
          if (id == 1503)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1503)
            if (id == 1500)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1500)
              if (id == 1498)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1499)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1500)
              if (id == 1501)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1502)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1503)
            if (id == 1506)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1506)
              if (id == 1504)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1505)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1506)
              if (id == 1507)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1508)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1509)
        if (id == 1522)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1522)
          if (id == 1516)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1516)
            if (id == 1513)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1513)
              if (id == 1511)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1511)
                if (id == 1510)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1511)
                if (id == 1512)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1513)
              if (id == 1514)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1515)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1516)
            if (id == 1519)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1519)
              if (id == 1517)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1518)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1519)
              if (id == 1520)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1521)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1522)
          if (id == 1528)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1528)
            if (id == 1525)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1525)
              if (id == 1523)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1524)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1525)
              if (id == 1526)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1527)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1528)
            if (id == 1531)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1531)
              if (id == 1529)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1530)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1531)
              if (id == 1532)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1533)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1534)
      if (id == 1559)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1559)
        if (id == 1547)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1547)
          if (id == 1541)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1541)
            if (id == 1538)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1538)
              if (id == 1536)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1536)
                if (id == 1535)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1536)
                if (id == 1537)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1538)
              if (id == 1539)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1540)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1541)
            if (id == 1544)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1544)
              if (id == 1542)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1543)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1544)
              if (id == 1545)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1546)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1547)
          if (id == 1553)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1553)
            if (id == 1550)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1550)
              if (id == 1548)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1549)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1550)
              if (id == 1551)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1552)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1553)
            if (id == 1556)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1556)
              if (id == 1554)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1555)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1556)
              if (id == 1557)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1558)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1559)
        if (id == 1572)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1572)
          if (id == 1566)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1566)
            if (id == 1563)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1563)
              if (id == 1561)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1561)
                if (id == 1560)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1561)
                if (id == 1562)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1563)
              if (id == 1564)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1565)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1566)
            if (id == 1569)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1569)
              if (id == 1567)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1568)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1569)
              if (id == 1570)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1571)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1572)
          if (id == 1578)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1578)
            if (id == 1575)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1575)
              if (id == 1573)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1574)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1575)
              if (id == 1576)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1577)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1578)
            if (id == 1581)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1581)
              if (id == 1579)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1580)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1581)
              if (id == 1582)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1583)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 1485
  val maxID = 1583
}