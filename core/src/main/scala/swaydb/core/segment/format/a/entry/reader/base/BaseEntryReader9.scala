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

object BaseEntryReader9 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 841)
      Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 841)
      if (id == 816)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 816)
        if (id == 804)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 804)
          if (id == 798)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 798)
            if (id == 795)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 795)
              if (id == 793)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 793)
                if (id == 792)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 793)
                if (id == 794)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 795)
              if (id == 796)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 797)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 798)
            if (id == 801)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 801)
              if (id == 799)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 800)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 801)
              if (id == 802)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 803)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 804)
          if (id == 810)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 810)
            if (id == 807)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 807)
              if (id == 805)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 806)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 807)
              if (id == 808)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 809)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 810)
            if (id == 813)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 813)
              if (id == 811)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 812)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 813)
              if (id == 814)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 815)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 816)
        if (id == 829)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 829)
          if (id == 823)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 823)
            if (id == 820)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 820)
              if (id == 818)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 818)
                if (id == 817)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 818)
                if (id == 819)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 820)
              if (id == 821)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 822)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 823)
            if (id == 826)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 826)
              if (id == 824)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 825)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 826)
              if (id == 827)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 828)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 829)
          if (id == 835)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 835)
            if (id == 832)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 832)
              if (id == 830)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 831)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 832)
              if (id == 833)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 834)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 835)
            if (id == 838)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 838)
              if (id == 836)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 837)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 838)
              if (id == 839)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 840)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 841)
      if (id == 866)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 866)
        if (id == 854)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 854)
          if (id == 848)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 848)
            if (id == 845)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 845)
              if (id == 843)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 843)
                if (id == 842)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 843)
                if (id == 844)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 845)
              if (id == 846)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 847)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 848)
            if (id == 851)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 851)
              if (id == 849)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 850)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 851)
              if (id == 852)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 853)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 854)
          if (id == 860)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 860)
            if (id == 857)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 857)
              if (id == 855)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 856)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 857)
              if (id == 858)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 859)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 860)
            if (id == 863)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 863)
              if (id == 861)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 862)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 863)
              if (id == 864)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 865)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 866)
        if (id == 879)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 879)
          if (id == 873)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 873)
            if (id == 870)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 870)
              if (id == 868)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 868)
                if (id == 867)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 868)
                if (id == 869)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 870)
              if (id == 871)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 872)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 873)
            if (id == 876)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 876)
              if (id == 874)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 875)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 876)
              if (id == 877)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 878)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 879)
          if (id == 885)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 885)
            if (id == 882)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 882)
              if (id == 880)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 881)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 882)
              if (id == 883)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 884)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 885)
            if (id == 888)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 888)
              if (id == 886)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 887)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 888)
              if (id == 889)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 890)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 792
  val maxID = 890
}