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

object BaseEntryReader5 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1782)
      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1782)
      if (id == 1683)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1683)
        if (id == 1633)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1633)
          if (id == 1608)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1608)
            if (id == 1596)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1596)
              if (id == 1590)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1590)
                if (id == 1587)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1587)
                  if (id == 1585)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1585)
                    if (id == 1584)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1585)
                    if (id == 1586)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1587)
                  if (id == 1588)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1589)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1590)
                if (id == 1593)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1593)
                  if (id == 1591)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1592)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1593)
                  if (id == 1594)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1595)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1596)
              if (id == 1602)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1602)
                if (id == 1599)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1599)
                  if (id == 1597)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1598)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1599)
                  if (id == 1600)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1601)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1602)
                if (id == 1605)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1605)
                  if (id == 1603)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1604)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1605)
                  if (id == 1606)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1607)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1608)
            if (id == 1621)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1621)
              if (id == 1615)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1615)
                if (id == 1612)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1612)
                  if (id == 1610)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1610)
                    if (id == 1609)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1610)
                    if (id == 1611)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1612)
                  if (id == 1613)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1614)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1615)
                if (id == 1618)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1618)
                  if (id == 1616)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1617)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1618)
                  if (id == 1619)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1620)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1621)
              if (id == 1627)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1627)
                if (id == 1624)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1624)
                  if (id == 1622)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1623)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1624)
                  if (id == 1625)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1626)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1627)
                if (id == 1630)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1630)
                  if (id == 1628)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1629)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1630)
                  if (id == 1631)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1632)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 1633)
          if (id == 1658)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1658)
            if (id == 1646)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1646)
              if (id == 1640)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1640)
                if (id == 1637)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1637)
                  if (id == 1635)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1635)
                    if (id == 1634)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1635)
                    if (id == 1636)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1637)
                  if (id == 1638)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1639)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1640)
                if (id == 1643)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1643)
                  if (id == 1641)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1642)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1643)
                  if (id == 1644)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1645)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1646)
              if (id == 1652)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1652)
                if (id == 1649)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1649)
                  if (id == 1647)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1648)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1649)
                  if (id == 1650)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1651)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1652)
                if (id == 1655)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1655)
                  if (id == 1653)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1654)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1655)
                  if (id == 1656)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1657)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1658)
            if (id == 1671)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1671)
              if (id == 1665)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1665)
                if (id == 1662)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1662)
                  if (id == 1660)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1660)
                    if (id == 1659)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1660)
                    if (id == 1661)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1662)
                  if (id == 1663)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1664)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1665)
                if (id == 1668)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1668)
                  if (id == 1666)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1667)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1668)
                  if (id == 1669)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1670)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1671)
              if (id == 1677)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1677)
                if (id == 1674)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1674)
                  if (id == 1672)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1673)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1674)
                  if (id == 1675)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1676)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1677)
                if (id == 1680)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1680)
                  if (id == 1678)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1679)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1680)
                  if (id == 1681)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1682)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 1683)
        if (id == 1733)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1733)
          if (id == 1708)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1708)
            if (id == 1696)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1696)
              if (id == 1690)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1690)
                if (id == 1687)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1687)
                  if (id == 1685)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1685)
                    if (id == 1684)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1685)
                    if (id == 1686)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1687)
                  if (id == 1688)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1689)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1690)
                if (id == 1693)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1693)
                  if (id == 1691)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1692)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1693)
                  if (id == 1694)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1695)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1696)
              if (id == 1702)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1702)
                if (id == 1699)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1699)
                  if (id == 1697)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1698)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1699)
                  if (id == 1700)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1701)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1702)
                if (id == 1705)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1705)
                  if (id == 1703)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1704)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1705)
                  if (id == 1706)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1707)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1708)
            if (id == 1721)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1721)
              if (id == 1715)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1715)
                if (id == 1712)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1712)
                  if (id == 1710)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1710)
                    if (id == 1709)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1710)
                    if (id == 1711)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1712)
                  if (id == 1713)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1714)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1715)
                if (id == 1718)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1718)
                  if (id == 1716)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1717)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1718)
                  if (id == 1719)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1720)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1721)
              if (id == 1727)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1727)
                if (id == 1724)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1724)
                  if (id == 1722)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1723)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1724)
                  if (id == 1725)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1726)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1727)
                if (id == 1730)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1730)
                  if (id == 1728)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1729)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1730)
                  if (id == 1731)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1732)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 1733)
          if (id == 1758)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1758)
            if (id == 1746)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1746)
              if (id == 1740)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1740)
                if (id == 1737)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1737)
                  if (id == 1735)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1735)
                    if (id == 1734)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1735)
                    if (id == 1736)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1737)
                  if (id == 1738)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1739)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1740)
                if (id == 1743)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1743)
                  if (id == 1741)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1742)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1743)
                  if (id == 1744)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1745)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1746)
              if (id == 1752)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1752)
                if (id == 1749)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1749)
                  if (id == 1747)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1748)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1749)
                  if (id == 1750)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1751)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1752)
                if (id == 1755)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1755)
                  if (id == 1753)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1754)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1755)
                  if (id == 1756)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1757)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1758)
            if (id == 1770)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1770)
              if (id == 1764)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1764)
                if (id == 1761)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1761)
                  if (id == 1759)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1760)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1761)
                  if (id == 1762)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1763)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1764)
                if (id == 1767)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1767)
                  if (id == 1765)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1766)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1767)
                  if (id == 1768)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1769)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1770)
              if (id == 1776)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1776)
                if (id == 1773)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1773)
                  if (id == 1771)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1772)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1773)
                  if (id == 1774)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1775)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1776)
                if (id == 1779)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1779)
                  if (id == 1777)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1778)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1779)
                  if (id == 1780)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1781)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1782)
      if (id == 1881)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1881)
        if (id == 1832)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1832)
          if (id == 1807)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1807)
            if (id == 1795)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1795)
              if (id == 1789)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1789)
                if (id == 1786)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1786)
                  if (id == 1784)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1784)
                    if (id == 1783)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1784)
                    if (id == 1785)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1786)
                  if (id == 1787)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1788)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1789)
                if (id == 1792)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1792)
                  if (id == 1790)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1791)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1792)
                  if (id == 1793)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1794)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1795)
              if (id == 1801)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1801)
                if (id == 1798)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1798)
                  if (id == 1796)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1797)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1798)
                  if (id == 1799)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1800)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1801)
                if (id == 1804)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1804)
                  if (id == 1802)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1803)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1804)
                  if (id == 1805)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1806)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1807)
            if (id == 1820)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1820)
              if (id == 1814)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1814)
                if (id == 1811)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1811)
                  if (id == 1809)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1809)
                    if (id == 1808)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1809)
                    if (id == 1810)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1811)
                  if (id == 1812)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1813)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1814)
                if (id == 1817)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1817)
                  if (id == 1815)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1816)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1817)
                  if (id == 1818)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1819)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1820)
              if (id == 1826)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1826)
                if (id == 1823)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1823)
                  if (id == 1821)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1822)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1823)
                  if (id == 1824)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1825)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1826)
                if (id == 1829)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1829)
                  if (id == 1827)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1828)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1829)
                  if (id == 1830)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1831)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 1832)
          if (id == 1857)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1857)
            if (id == 1845)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1845)
              if (id == 1839)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1839)
                if (id == 1836)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1836)
                  if (id == 1834)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1834)
                    if (id == 1833)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1834)
                    if (id == 1835)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1836)
                  if (id == 1837)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1838)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1839)
                if (id == 1842)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1842)
                  if (id == 1840)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1841)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1842)
                  if (id == 1843)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1844)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1845)
              if (id == 1851)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1851)
                if (id == 1848)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1848)
                  if (id == 1846)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1847)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1848)
                  if (id == 1849)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1850)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1851)
                if (id == 1854)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1854)
                  if (id == 1852)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1853)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1854)
                  if (id == 1855)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1856)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1857)
            if (id == 1869)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1869)
              if (id == 1863)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1863)
                if (id == 1860)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1860)
                  if (id == 1858)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1859)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1860)
                  if (id == 1861)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1862)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1863)
                if (id == 1866)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1866)
                  if (id == 1864)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1865)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1866)
                  if (id == 1867)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1868)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1869)
              if (id == 1875)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1875)
                if (id == 1872)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1872)
                  if (id == 1870)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1871)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1872)
                  if (id == 1873)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1874)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1875)
                if (id == 1878)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1878)
                  if (id == 1876)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1877)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1878)
                  if (id == 1879)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1880)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 1881)
        if (id == 1931)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1931)
          if (id == 1906)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1906)
            if (id == 1894)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1894)
              if (id == 1888)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1888)
                if (id == 1885)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1885)
                  if (id == 1883)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1883)
                    if (id == 1882)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1883)
                    if (id == 1884)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1885)
                  if (id == 1886)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1887)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1888)
                if (id == 1891)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1891)
                  if (id == 1889)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1890)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1891)
                  if (id == 1892)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1893)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1894)
              if (id == 1900)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1900)
                if (id == 1897)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1897)
                  if (id == 1895)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1896)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1897)
                  if (id == 1898)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1899)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1900)
                if (id == 1903)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1903)
                  if (id == 1901)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1902)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1903)
                  if (id == 1904)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1905)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1906)
            if (id == 1919)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1919)
              if (id == 1913)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1913)
                if (id == 1910)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1910)
                  if (id == 1908)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1908)
                    if (id == 1907)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1908)
                    if (id == 1909)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1910)
                  if (id == 1911)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1912)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1913)
                if (id == 1916)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1916)
                  if (id == 1914)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1915)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1916)
                  if (id == 1917)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1918)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1919)
              if (id == 1925)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1925)
                if (id == 1922)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1922)
                  if (id == 1920)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1921)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1922)
                  if (id == 1923)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1924)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1925)
                if (id == 1928)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1928)
                  if (id == 1926)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1927)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1928)
                  if (id == 1929)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1930)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 1931)
          if (id == 1956)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1956)
            if (id == 1944)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1944)
              if (id == 1938)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1938)
                if (id == 1935)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1935)
                  if (id == 1933)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id < 1933)
                    if (id == 1932)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else if (id > 1933)
                    if (id == 1934)
                      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                    else
                      None
                  else
                    None
                else if (id > 1935)
                  if (id == 1936)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1937)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1938)
                if (id == 1941)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1941)
                  if (id == 1939)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1940)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1941)
                  if (id == 1942)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1943)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1944)
              if (id == 1950)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1950)
                if (id == 1947)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1947)
                  if (id == 1945)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1946)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1947)
                  if (id == 1948)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1949)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1950)
                if (id == 1953)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1953)
                  if (id == 1951)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1952)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1953)
                  if (id == 1954)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1955)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1956)
            if (id == 1968)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1968)
              if (id == 1962)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1962)
                if (id == 1959)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1959)
                  if (id == 1957)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1958)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1959)
                  if (id == 1960)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1961)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1962)
                if (id == 1965)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1965)
                  if (id == 1963)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1964)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1965)
                  if (id == 1966)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1967)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1968)
              if (id == 1974)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1974)
                if (id == 1971)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1971)
                  if (id == 1969)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1970)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1971)
                  if (id == 1972)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1973)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1974)
                if (id == 1977)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1977)
                  if (id == 1975)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1976)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1977)
                  if (id == 1978)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1979)
                    Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 1584
  val maxID = 1979
}