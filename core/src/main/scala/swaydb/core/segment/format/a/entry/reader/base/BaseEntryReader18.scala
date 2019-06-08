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


object BaseEntryReader18 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1732)
      Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1732)
      if (id == 1707)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1707)
        if (id == 1695)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1695)
          if (id == 1689)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1689)
            if (id == 1686)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1686)
              if (id == 1684)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1684)
                if (id == 1683)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1684)
                if (id == 1685)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1686)
              if (id == 1687)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1688)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1689)
            if (id == 1692)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1692)
              if (id == 1690)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1691)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1692)
              if (id == 1693)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1694)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1695)
          if (id == 1701)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1701)
            if (id == 1698)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1698)
              if (id == 1696)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1697)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1698)
              if (id == 1699)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1700)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1701)
            if (id == 1704)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1704)
              if (id == 1702)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1703)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1704)
              if (id == 1705)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1706)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1707)
        if (id == 1720)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1720)
          if (id == 1714)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1714)
            if (id == 1711)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1711)
              if (id == 1709)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1709)
                if (id == 1708)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1709)
                if (id == 1710)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1711)
              if (id == 1712)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1713)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1714)
            if (id == 1717)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1717)
              if (id == 1715)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1716)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1717)
              if (id == 1718)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1719)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1720)
          if (id == 1726)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1726)
            if (id == 1723)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1723)
              if (id == 1721)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1722)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1723)
              if (id == 1724)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1725)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1726)
            if (id == 1729)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1729)
              if (id == 1727)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1728)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1729)
              if (id == 1730)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1731)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1732)
      if (id == 1757)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1757)
        if (id == 1745)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1745)
          if (id == 1739)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1739)
            if (id == 1736)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1736)
              if (id == 1734)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1734)
                if (id == 1733)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1734)
                if (id == 1735)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1736)
              if (id == 1737)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1738)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1739)
            if (id == 1742)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1742)
              if (id == 1740)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1741)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1742)
              if (id == 1743)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1744)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1745)
          if (id == 1751)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1751)
            if (id == 1748)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1748)
              if (id == 1746)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1747)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1748)
              if (id == 1749)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1750)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1751)
            if (id == 1754)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1754)
              if (id == 1752)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1753)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1754)
              if (id == 1755)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1756)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1757)
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
              else if (id < 1759)
                if (id == 1758)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.NoTime.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1759)
                if (id == 1760)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
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

  val minID = 1683
  val maxID = 1781
}