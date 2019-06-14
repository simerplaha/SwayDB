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
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.IO
import swaydb.data.slice.Reader

object BaseEntryReader3 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 862)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 862)
      if (id == 776)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 776)
        if (id == 733)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 733)
          if (id == 711)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 711)
            if (id == 700)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 700)
              if (id == 695)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 695)
                if (id == 692)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 692)
                  if (id == 690)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 691)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 692)
                  if (id == 693)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 694)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 695)
                if (id == 698)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 698)
                  if (id == 696)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 697)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 698)
                  if (id == 699)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 700)
              if (id == 706)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 706)
                if (id == 703)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 703)
                  if (id == 701)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 702)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 703)
                  if (id == 704)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 705)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 706)
                if (id == 709)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 709)
                  if (id == 707)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 708)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 709)
                  if (id == 710)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 711)
            if (id == 722)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 722)
              if (id == 717)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 717)
                if (id == 714)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 714)
                  if (id == 712)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 713)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 714)
                  if (id == 715)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 716)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 717)
                if (id == 720)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 720)
                  if (id == 718)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 719)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 720)
                  if (id == 721)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 722)
              if (id == 728)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 728)
                if (id == 725)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 725)
                  if (id == 723)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 724)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 725)
                  if (id == 726)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 727)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 728)
                if (id == 731)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 731)
                  if (id == 729)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 730)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 731)
                  if (id == 732)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 733)
          if (id == 755)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 755)
            if (id == 744)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 744)
              if (id == 739)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 739)
                if (id == 736)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 736)
                  if (id == 734)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 735)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 736)
                  if (id == 737)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 738)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 739)
                if (id == 742)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 742)
                  if (id == 740)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 741)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 742)
                  if (id == 743)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 744)
              if (id == 750)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 750)
                if (id == 747)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 747)
                  if (id == 745)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 746)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 747)
                  if (id == 748)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 749)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 750)
                if (id == 753)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 753)
                  if (id == 751)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 752)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 753)
                  if (id == 754)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 755)
            if (id == 766)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 766)
              if (id == 761)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 761)
                if (id == 758)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 758)
                  if (id == 756)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 757)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 758)
                  if (id == 759)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 760)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 761)
                if (id == 764)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 764)
                  if (id == 762)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 763)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 764)
                  if (id == 765)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 766)
              if (id == 771)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 771)
                if (id == 769)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 769)
                  if (id == 767)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 768)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 769)
                  if (id == 770)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 771)
                if (id == 774)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 774)
                  if (id == 772)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 773)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 774)
                  if (id == 775)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 776)
        if (id == 819)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 819)
          if (id == 798)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 798)
            if (id == 787)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 787)
              if (id == 782)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 782)
                if (id == 779)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 779)
                  if (id == 777)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 778)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 779)
                  if (id == 780)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 781)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 782)
                if (id == 785)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 785)
                  if (id == 783)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 784)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 785)
                  if (id == 786)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 787)
              if (id == 793)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 793)
                if (id == 790)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 790)
                  if (id == 788)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 789)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 790)
                  if (id == 791)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 792)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 793)
                if (id == 796)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 796)
                  if (id == 794)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 795)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 796)
                  if (id == 797)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 798)
            if (id == 809)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 809)
              if (id == 804)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 804)
                if (id == 801)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 801)
                  if (id == 799)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 800)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 801)
                  if (id == 802)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 803)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 804)
                if (id == 807)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 807)
                  if (id == 805)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 806)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 807)
                  if (id == 808)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 809)
              if (id == 814)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 814)
                if (id == 812)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 812)
                  if (id == 810)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 811)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 812)
                  if (id == 813)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 814)
                if (id == 817)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 817)
                  if (id == 815)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 816)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 817)
                  if (id == 818)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 819)
          if (id == 841)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 841)
            if (id == 830)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 830)
              if (id == 825)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 825)
                if (id == 822)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 822)
                  if (id == 820)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 821)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 822)
                  if (id == 823)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 824)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 825)
                if (id == 828)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 828)
                  if (id == 826)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 827)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 828)
                  if (id == 829)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 830)
              if (id == 836)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 836)
                if (id == 833)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 833)
                  if (id == 831)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 832)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 833)
                  if (id == 834)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 835)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 836)
                if (id == 839)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 839)
                  if (id == 837)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 838)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 839)
                  if (id == 840)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 841)
            if (id == 852)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 852)
              if (id == 847)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 847)
                if (id == 844)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 844)
                  if (id == 842)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 843)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 844)
                  if (id == 845)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 846)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 847)
                if (id == 850)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 850)
                  if (id == 848)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 849)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 850)
                  if (id == 851)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 852)
              if (id == 857)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 857)
                if (id == 855)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 855)
                  if (id == 853)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 854)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 855)
                  if (id == 856)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 857)
                if (id == 860)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 860)
                  if (id == 858)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 859)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 860)
                  if (id == 861)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 862)
      if (id == 949)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 949)
        if (id == 906)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 906)
          if (id == 884)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 884)
            if (id == 873)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 873)
              if (id == 868)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 868)
                if (id == 865)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 865)
                  if (id == 863)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 864)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 865)
                  if (id == 866)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 867)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 868)
                if (id == 871)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 871)
                  if (id == 869)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 870)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 871)
                  if (id == 872)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 873)
              if (id == 879)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 879)
                if (id == 876)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 876)
                  if (id == 874)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 875)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 876)
                  if (id == 877)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 878)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 879)
                if (id == 882)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 882)
                  if (id == 880)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 881)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 882)
                  if (id == 883)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 884)
            if (id == 895)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 895)
              if (id == 890)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 890)
                if (id == 887)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 887)
                  if (id == 885)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 886)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 887)
                  if (id == 888)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 889)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 890)
                if (id == 893)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 893)
                  if (id == 891)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 892)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 893)
                  if (id == 894)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 895)
              if (id == 901)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 901)
                if (id == 898)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 898)
                  if (id == 896)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 897)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 898)
                  if (id == 899)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 900)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 901)
                if (id == 904)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 904)
                  if (id == 902)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 903)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 904)
                  if (id == 905)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 906)
          if (id == 928)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 928)
            if (id == 917)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 917)
              if (id == 912)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 912)
                if (id == 909)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 909)
                  if (id == 907)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 908)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 909)
                  if (id == 910)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 911)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 912)
                if (id == 915)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 915)
                  if (id == 913)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 914)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 915)
                  if (id == 916)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 917)
              if (id == 923)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 923)
                if (id == 920)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 920)
                  if (id == 918)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 919)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 920)
                  if (id == 921)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 922)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 923)
                if (id == 926)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 926)
                  if (id == 924)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 925)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 926)
                  if (id == 927)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 928)
            if (id == 939)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 939)
              if (id == 934)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 934)
                if (id == 931)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 931)
                  if (id == 929)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 930)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 931)
                  if (id == 932)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 933)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 934)
                if (id == 937)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 937)
                  if (id == 935)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 936)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 937)
                  if (id == 938)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 939)
              if (id == 944)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 944)
                if (id == 942)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 942)
                  if (id == 940)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 941)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 942)
                  if (id == 943)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 944)
                if (id == 947)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 947)
                  if (id == 945)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 946)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 947)
                  if (id == 948)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 949)
        if (id == 992)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 992)
          if (id == 971)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 971)
            if (id == 960)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 960)
              if (id == 955)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 955)
                if (id == 952)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 952)
                  if (id == 950)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 951)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 952)
                  if (id == 953)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 954)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 955)
                if (id == 958)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 958)
                  if (id == 956)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 957)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 958)
                  if (id == 959)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 960)
              if (id == 966)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 966)
                if (id == 963)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 963)
                  if (id == 961)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 962)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 963)
                  if (id == 964)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 965)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 966)
                if (id == 969)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 969)
                  if (id == 967)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 968)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 969)
                  if (id == 970)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 971)
            if (id == 982)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 982)
              if (id == 977)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 977)
                if (id == 974)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 974)
                  if (id == 972)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 973)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 974)
                  if (id == 975)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 976)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 977)
                if (id == 980)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 980)
                  if (id == 978)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 979)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 980)
                  if (id == 981)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 982)
              if (id == 987)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 987)
                if (id == 985)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 985)
                  if (id == 983)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 984)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 985)
                  if (id == 986)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 987)
                if (id == 990)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 990)
                  if (id == 988)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 989)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 990)
                  if (id == 991)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 992)
          if (id == 1014)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 1014)
            if (id == 1003)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1003)
              if (id == 998)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 998)
                if (id == 995)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 995)
                  if (id == 993)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 994)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 995)
                  if (id == 996)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 997)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 998)
                if (id == 1001)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1001)
                  if (id == 999)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1000)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1001)
                  if (id == 1002)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1003)
              if (id == 1009)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1009)
                if (id == 1006)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1006)
                  if (id == 1004)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1005)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1006)
                  if (id == 1007)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1008)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1009)
                if (id == 1012)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1012)
                  if (id == 1010)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1011)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1012)
                  if (id == 1013)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 1014)
            if (id == 1025)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 1025)
              if (id == 1020)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1020)
                if (id == 1017)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1017)
                  if (id == 1015)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1016)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1017)
                  if (id == 1018)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1019)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1020)
                if (id == 1023)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1023)
                  if (id == 1021)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1022)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1023)
                  if (id == 1024)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 1025)
              if (id == 1030)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 1030)
                if (id == 1028)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1028)
                  if (id == 1026)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1027)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1028)
                  if (id == 1029)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 1030)
                if (id == 1033)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 1033)
                  if (id == 1031)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 1032)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 1033)
                  if (id == 1034)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 690
  val maxID = 1034
}