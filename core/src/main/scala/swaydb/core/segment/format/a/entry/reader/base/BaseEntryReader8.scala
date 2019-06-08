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

object BaseEntryReader8 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 742)
      Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 742)
      if (id == 717)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 717)
        if (id == 705)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 705)
          if (id == 699)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 699)
            if (id == 696)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 696)
              if (id == 694)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 694)
                if (id == 693)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 694)
                if (id == 695)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 696)
              if (id == 697)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 698)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 699)
            if (id == 702)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 702)
              if (id == 700)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 701)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 702)
              if (id == 703)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 704)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 705)
          if (id == 711)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 711)
            if (id == 708)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 708)
              if (id == 706)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 707)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 708)
              if (id == 709)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 710)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 711)
            if (id == 714)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 714)
              if (id == 712)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 713)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 714)
              if (id == 715)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 716)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 717)
        if (id == 730)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 730)
          if (id == 724)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 724)
            if (id == 721)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 721)
              if (id == 719)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 719)
                if (id == 718)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 719)
                if (id == 720)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 721)
              if (id == 722)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 723)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 724)
            if (id == 727)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 727)
              if (id == 725)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 726)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 727)
              if (id == 728)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 729)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 730)
          if (id == 736)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 736)
            if (id == 733)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 733)
              if (id == 731)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 732)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 733)
              if (id == 734)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 735)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 736)
            if (id == 739)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 739)
              if (id == 737)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 738)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 739)
              if (id == 740)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 741)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 742)
      if (id == 767)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 767)
        if (id == 755)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 755)
          if (id == 749)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 749)
            if (id == 746)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 746)
              if (id == 744)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 744)
                if (id == 743)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 744)
                if (id == 745)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 746)
              if (id == 747)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 748)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 749)
            if (id == 752)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 752)
              if (id == 750)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 751)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 752)
              if (id == 753)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 754)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 755)
          if (id == 761)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 761)
            if (id == 758)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 758)
              if (id == 756)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 757)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 758)
              if (id == 759)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 760)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 761)
            if (id == 764)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 764)
              if (id == 762)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 763)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 764)
              if (id == 765)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 766)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 767)
        if (id == 780)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 780)
          if (id == 774)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 774)
            if (id == 771)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 771)
              if (id == 769)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 769)
                if (id == 768)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 769)
                if (id == 770)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 771)
              if (id == 772)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 773)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 774)
            if (id == 777)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 777)
              if (id == 775)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 776)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 777)
              if (id == 778)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 779)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 780)
          if (id == 786)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 786)
            if (id == 783)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 783)
              if (id == 781)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 782)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 783)
              if (id == 784)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 785)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 786)
            if (id == 789)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 789)
              if (id == 787)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 788)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 789)
              if (id == 790)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 791)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 693
  val maxID = 791
}