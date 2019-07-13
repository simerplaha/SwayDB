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
import swaydb.core.io.reader.BlockReader
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.IO
import swaydb.data.slice.Reader

private[core] object BaseEntryReader3 extends BaseEntryReader {

  def read[T](baseId: Int,
              keyValueId: Int,
              indexReader: Reader,
              valueReader: Option[BlockReader[ValuesBlock]],
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              accessPosition: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (baseId == 862)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
    else if (baseId < 862)
      if (baseId == 776)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
      else if (baseId < 776)
        if (baseId == 733)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 733)
          if (baseId == 711)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 711)
            if (baseId == 700)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 700)
              if (baseId == 695)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 695)
                if (baseId == 692)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 692)
                  if (baseId == 690)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 691)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 692)
                  if (baseId == 693)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 694)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 695)
                if (baseId == 698)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 698)
                  if (baseId == 696)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 697)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 698)
                  if (baseId == 699)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 700)
              if (baseId == 706)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 706)
                if (baseId == 703)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 703)
                  if (baseId == 701)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 702)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 703)
                  if (baseId == 704)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 705)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 706)
                if (baseId == 709)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 709)
                  if (baseId == 707)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 708)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 709)
                  if (baseId == 710)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 711)
            if (baseId == 722)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 722)
              if (baseId == 717)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 717)
                if (baseId == 714)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 714)
                  if (baseId == 712)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 713)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 714)
                  if (baseId == 715)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 716)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 717)
                if (baseId == 720)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 720)
                  if (baseId == 718)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 719)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 720)
                  if (baseId == 721)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 722)
              if (baseId == 728)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 728)
                if (baseId == 725)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 725)
                  if (baseId == 723)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 724)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 725)
                  if (baseId == 726)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 727)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 728)
                if (baseId == 731)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 731)
                  if (baseId == 729)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 730)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 731)
                  if (baseId == 732)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 733)
          if (baseId == 755)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 755)
            if (baseId == 744)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 744)
              if (baseId == 739)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 739)
                if (baseId == 736)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 736)
                  if (baseId == 734)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 735)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 736)
                  if (baseId == 737)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 738)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 739)
                if (baseId == 742)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 742)
                  if (baseId == 740)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 741)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 742)
                  if (baseId == 743)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 744)
              if (baseId == 750)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 750)
                if (baseId == 747)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 747)
                  if (baseId == 745)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 746)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 747)
                  if (baseId == 748)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 749)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 750)
                if (baseId == 753)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 753)
                  if (baseId == 751)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 752)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 753)
                  if (baseId == 754)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 755)
            if (baseId == 766)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 766)
              if (baseId == 761)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 761)
                if (baseId == 758)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 758)
                  if (baseId == 756)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 757)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 758)
                  if (baseId == 759)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 760)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 761)
                if (baseId == 764)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 764)
                  if (baseId == 762)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 763)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 764)
                  if (baseId == 765)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 766)
              if (baseId == 771)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 771)
                if (baseId == 769)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 769)
                  if (baseId == 767)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 768)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 769)
                  if (baseId == 770)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 771)
                if (baseId == 774)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 774)
                  if (baseId == 772)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 773)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 774)
                  if (baseId == 775)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
      else if (baseId > 776)
        if (baseId == 819)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 819)
          if (baseId == 798)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 798)
            if (baseId == 787)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 787)
              if (baseId == 782)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 782)
                if (baseId == 779)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 779)
                  if (baseId == 777)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 778)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 779)
                  if (baseId == 780)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 781)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 782)
                if (baseId == 785)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 785)
                  if (baseId == 783)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 784)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 785)
                  if (baseId == 786)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 787)
              if (baseId == 793)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 793)
                if (baseId == 790)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 790)
                  if (baseId == 788)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 789)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 790)
                  if (baseId == 791)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 792)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 793)
                if (baseId == 796)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 796)
                  if (baseId == 794)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 795)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 796)
                  if (baseId == 797)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 798)
            if (baseId == 809)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 809)
              if (baseId == 804)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 804)
                if (baseId == 801)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 801)
                  if (baseId == 799)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 800)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 801)
                  if (baseId == 802)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 803)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 804)
                if (baseId == 807)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 807)
                  if (baseId == 805)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 806)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 807)
                  if (baseId == 808)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 809)
              if (baseId == 814)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 814)
                if (baseId == 812)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 812)
                  if (baseId == 810)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 811)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 812)
                  if (baseId == 813)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 814)
                if (baseId == 817)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 817)
                  if (baseId == 815)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 816)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 817)
                  if (baseId == 818)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 819)
          if (baseId == 841)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 841)
            if (baseId == 830)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 830)
              if (baseId == 825)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 825)
                if (baseId == 822)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 822)
                  if (baseId == 820)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 821)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 822)
                  if (baseId == 823)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 824)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 825)
                if (baseId == 828)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 828)
                  if (baseId == 826)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 827)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 828)
                  if (baseId == 829)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 830)
              if (baseId == 836)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 836)
                if (baseId == 833)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 833)
                  if (baseId == 831)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 832)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 833)
                  if (baseId == 834)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 835)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 836)
                if (baseId == 839)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 839)
                  if (baseId == 837)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 838)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 839)
                  if (baseId == 840)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 841)
            if (baseId == 852)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 852)
              if (baseId == 847)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 847)
                if (baseId == 844)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 844)
                  if (baseId == 842)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 843)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 844)
                  if (baseId == 845)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 846)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 847)
                if (baseId == 850)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 850)
                  if (baseId == 848)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 849)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 850)
                  if (baseId == 851)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 852)
              if (baseId == 857)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 857)
                if (baseId == 855)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 855)
                  if (baseId == 853)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 854)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 855)
                  if (baseId == 856)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 857)
                if (baseId == 860)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 860)
                  if (baseId == 858)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 859)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 860)
                  if (baseId == 861)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
    else if (baseId > 862)
      if (baseId == 949)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
      else if (baseId < 949)
        if (baseId == 906)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 906)
          if (baseId == 884)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 884)
            if (baseId == 873)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 873)
              if (baseId == 868)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 868)
                if (baseId == 865)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 865)
                  if (baseId == 863)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 864)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 865)
                  if (baseId == 866)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 867)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 868)
                if (baseId == 871)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 871)
                  if (baseId == 869)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 870)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 871)
                  if (baseId == 872)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 873)
              if (baseId == 879)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 879)
                if (baseId == 876)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 876)
                  if (baseId == 874)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 875)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 876)
                  if (baseId == 877)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 878)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 879)
                if (baseId == 882)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 882)
                  if (baseId == 880)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 881)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 882)
                  if (baseId == 883)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 884)
            if (baseId == 895)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 895)
              if (baseId == 890)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 890)
                if (baseId == 887)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 887)
                  if (baseId == 885)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 886)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 887)
                  if (baseId == 888)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 889)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 890)
                if (baseId == 893)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 893)
                  if (baseId == 891)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 892)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 893)
                  if (baseId == 894)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 895)
              if (baseId == 901)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 901)
                if (baseId == 898)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 898)
                  if (baseId == 896)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 897)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 898)
                  if (baseId == 899)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 900)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 901)
                if (baseId == 904)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 904)
                  if (baseId == 902)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 903)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 904)
                  if (baseId == 905)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 906)
          if (baseId == 928)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 928)
            if (baseId == 917)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 917)
              if (baseId == 912)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 912)
                if (baseId == 909)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 909)
                  if (baseId == 907)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 908)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 909)
                  if (baseId == 910)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 911)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 912)
                if (baseId == 915)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 915)
                  if (baseId == 913)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 914)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 915)
                  if (baseId == 916)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 917)
              if (baseId == 923)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 923)
                if (baseId == 920)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 920)
                  if (baseId == 918)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 919)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 920)
                  if (baseId == 921)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 922)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 923)
                if (baseId == 926)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 926)
                  if (baseId == 924)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 925)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 926)
                  if (baseId == 927)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 928)
            if (baseId == 939)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 939)
              if (baseId == 934)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 934)
                if (baseId == 931)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 931)
                  if (baseId == 929)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 930)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 931)
                  if (baseId == 932)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 933)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 934)
                if (baseId == 937)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 937)
                  if (baseId == 935)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 936)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 937)
                  if (baseId == 938)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 939)
              if (baseId == 944)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 944)
                if (baseId == 942)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 942)
                  if (baseId == 940)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 941)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 942)
                  if (baseId == 943)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 944)
                if (baseId == 947)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 947)
                  if (baseId == 945)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 946)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 947)
                  if (baseId == 948)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
      else if (baseId > 949)
        if (baseId == 992)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 992)
          if (baseId == 971)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 971)
            if (baseId == 960)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 960)
              if (baseId == 955)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 955)
                if (baseId == 952)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 952)
                  if (baseId == 950)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 951)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 952)
                  if (baseId == 953)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 954)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 955)
                if (baseId == 958)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 958)
                  if (baseId == 956)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 957)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 958)
                  if (baseId == 959)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 960)
              if (baseId == 966)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 966)
                if (baseId == 963)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 963)
                  if (baseId == 961)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 962)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 963)
                  if (baseId == 964)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 965)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 966)
                if (baseId == 969)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 969)
                  if (baseId == 967)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 968)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 969)
                  if (baseId == 970)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 971)
            if (baseId == 982)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 982)
              if (baseId == 977)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 977)
                if (baseId == 974)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 974)
                  if (baseId == 972)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 973)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 974)
                  if (baseId == 975)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 976)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 977)
                if (baseId == 980)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 980)
                  if (baseId == 978)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 979)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 980)
                  if (baseId == 981)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 982)
              if (baseId == 987)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 987)
                if (baseId == 985)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 985)
                  if (baseId == 983)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 984)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 985)
                  if (baseId == 986)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 987)
                if (baseId == 990)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 990)
                  if (baseId == 988)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 989)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 990)
                  if (baseId == 991)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 992)
          if (baseId == 1014)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 1014)
            if (baseId == 1003)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1003)
              if (baseId == 998)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 998)
                if (baseId == 995)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 995)
                  if (baseId == 993)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 994)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 995)
                  if (baseId == 996)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 997)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 998)
                if (baseId == 1001)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1001)
                  if (baseId == 999)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1000)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1001)
                  if (baseId == 1002)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1003)
              if (baseId == 1009)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1009)
                if (baseId == 1006)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1006)
                  if (baseId == 1004)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1005)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1006)
                  if (baseId == 1007)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1008)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1009)
                if (baseId == 1012)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1012)
                  if (baseId == 1010)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1011)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1012)
                  if (baseId == 1013)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 1014)
            if (baseId == 1025)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 1025)
              if (baseId == 1020)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1020)
                if (baseId == 1017)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1017)
                  if (baseId == 1015)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1016)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1017)
                  if (baseId == 1018)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1019)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1020)
                if (baseId == 1023)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1023)
                  if (baseId == 1021)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1022)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1023)
                  if (baseId == 1024)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 1025)
              if (baseId == 1030)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 1030)
                if (baseId == 1028)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1028)
                  if (baseId == 1026)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1027)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1028)
                  if (baseId == 1029)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 1030)
                if (baseId == 1033)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 1033)
                  if (baseId == 1031)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 1032)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 1033)
                  if (baseId == 1034)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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