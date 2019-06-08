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


object BaseEntryReader20 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 1930)
      Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 1930)
      if (id == 1905)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1905)
        if (id == 1893)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1893)
          if (id == 1887)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1887)
            if (id == 1884)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1884)
              if (id == 1882)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1882)
                if (id == 1881)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1882)
                if (id == 1883)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1884)
              if (id == 1885)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1886)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1887)
            if (id == 1890)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1890)
              if (id == 1888)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1889)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1890)
              if (id == 1891)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1892)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1893)
          if (id == 1899)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1899)
            if (id == 1896)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1896)
              if (id == 1894)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1895)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1896)
              if (id == 1897)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1898)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1899)
            if (id == 1902)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1902)
              if (id == 1900)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1901)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1902)
              if (id == 1903)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1904)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1905)
        if (id == 1918)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1918)
          if (id == 1912)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1912)
            if (id == 1909)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1909)
              if (id == 1907)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1907)
                if (id == 1906)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1907)
                if (id == 1908)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1909)
              if (id == 1910)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1911)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1912)
            if (id == 1915)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1915)
              if (id == 1913)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1914)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1915)
              if (id == 1916)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1917)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1918)
          if (id == 1924)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1924)
            if (id == 1921)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1921)
              if (id == 1919)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1920)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1921)
              if (id == 1922)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1923)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1924)
            if (id == 1927)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1927)
              if (id == 1925)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1926)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1927)
              if (id == 1928)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1929)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 1930)
      if (id == 1955)
        Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 1955)
        if (id == 1943)
          Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 1943)
          if (id == 1937)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1937)
            if (id == 1934)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1934)
              if (id == 1932)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1932)
                if (id == 1931)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1932)
                if (id == 1933)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 1934)
              if (id == 1935)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1936)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1937)
            if (id == 1940)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1940)
              if (id == 1938)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1939)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1940)
              if (id == 1941)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1942)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 1943)
          if (id == 1949)
            Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1949)
            if (id == 1946)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1946)
              if (id == 1944)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1945)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1946)
              if (id == 1947)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1948)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 1949)
            if (id == 1952)
              Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1952)
              if (id == 1950)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1951)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 1952)
              if (id == 1953)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 1954)
                Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 1955)
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
              else if (id < 1957)
                if (id == 1956)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 1957)
                if (id == 1958)
                  Some(reader(BaseEntryId.FormatA.KeyUncompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
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

  val minID = 1881
  val maxID = 1979
}