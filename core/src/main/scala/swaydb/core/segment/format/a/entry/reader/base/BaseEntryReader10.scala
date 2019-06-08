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

object BaseEntryReader10 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 940)
      Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 940)
      if (id == 915)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 915)
        if (id == 903)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 903)
          if (id == 897)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 897)
            if (id == 894)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 894)
              if (id == 892)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 892)
                if (id == 891)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 892)
                if (id == 893)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 894)
              if (id == 895)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 896)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 897)
            if (id == 900)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 900)
              if (id == 898)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 899)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 900)
              if (id == 901)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 902)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 903)
          if (id == 909)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 909)
            if (id == 906)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 906)
              if (id == 904)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 905)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 906)
              if (id == 907)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 908)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 909)
            if (id == 912)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 912)
              if (id == 910)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 911)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 912)
              if (id == 913)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 914)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 915)
        if (id == 928)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 928)
          if (id == 922)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 922)
            if (id == 919)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 919)
              if (id == 917)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 917)
                if (id == 916)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 917)
                if (id == 918)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 919)
              if (id == 920)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 921)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 922)
            if (id == 925)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 925)
              if (id == 923)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 924)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 925)
              if (id == 926)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 927)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 928)
          if (id == 934)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 934)
            if (id == 931)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 931)
              if (id == 929)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 930)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 931)
              if (id == 932)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 933)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 934)
            if (id == 937)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 937)
              if (id == 935)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 936)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 937)
              if (id == 938)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 939)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 940)
      if (id == 965)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 965)
        if (id == 953)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 953)
          if (id == 947)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 947)
            if (id == 944)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 944)
              if (id == 942)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 942)
                if (id == 941)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 942)
                if (id == 943)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 944)
              if (id == 945)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 946)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 947)
            if (id == 950)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 950)
              if (id == 948)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 949)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 950)
              if (id == 951)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 952)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 953)
          if (id == 959)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 959)
            if (id == 956)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 956)
              if (id == 954)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 955)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 956)
              if (id == 957)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 958)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 959)
            if (id == 962)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 962)
              if (id == 960)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 961)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 962)
              if (id == 963)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 964)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 965)
        if (id == 978)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 978)
          if (id == 972)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 972)
            if (id == 969)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 969)
              if (id == 967)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 967)
                if (id == 966)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 967)
                if (id == 968)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 969)
              if (id == 970)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 971)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 972)
            if (id == 975)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 975)
              if (id == 973)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 974)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 975)
              if (id == 976)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 977)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 978)
          if (id == 984)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 984)
            if (id == 981)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 981)
              if (id == 979)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 980)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 981)
              if (id == 982)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 983)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 984)
            if (id == 987)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 987)
              if (id == 985)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 986)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 987)
              if (id == 988)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 989)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 891
  val maxID = 989
}