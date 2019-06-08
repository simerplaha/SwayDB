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

object BaseEntryReader7 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 643)
      Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 643)
      if (id == 618)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 618)
        if (id == 606)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 606)
          if (id == 600)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 600)
            if (id == 597)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 597)
              if (id == 595)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 595)
                if (id == 594)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 595)
                if (id == 596)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 597)
              if (id == 598)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 599)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 600)
            if (id == 603)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 603)
              if (id == 601)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 602)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 603)
              if (id == 604)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 605)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 606)
          if (id == 612)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 612)
            if (id == 609)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 609)
              if (id == 607)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 608)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 609)
              if (id == 610)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 611)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 612)
            if (id == 615)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 615)
              if (id == 613)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 614)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 615)
              if (id == 616)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 617)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 618)
        if (id == 631)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 631)
          if (id == 625)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 625)
            if (id == 622)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 622)
              if (id == 620)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 620)
                if (id == 619)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 620)
                if (id == 621)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 622)
              if (id == 623)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 624)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 625)
            if (id == 628)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 628)
              if (id == 626)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 627)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 628)
              if (id == 629)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 630)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 631)
          if (id == 637)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 637)
            if (id == 634)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 634)
              if (id == 632)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 633)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 634)
              if (id == 635)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 636)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 637)
            if (id == 640)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 640)
              if (id == 638)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 639)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 640)
              if (id == 641)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 642)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 643)
      if (id == 668)
        Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 668)
        if (id == 656)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 656)
          if (id == 650)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 650)
            if (id == 647)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 647)
              if (id == 645)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 645)
                if (id == 644)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 645)
                if (id == 646)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 647)
              if (id == 648)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 649)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 650)
            if (id == 653)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 653)
              if (id == 651)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 652)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 653)
              if (id == 654)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 655)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 656)
          if (id == 662)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 662)
            if (id == 659)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 659)
              if (id == 657)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 658)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 659)
              if (id == 660)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 661)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 662)
            if (id == 665)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 665)
              if (id == 663)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 664)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 665)
              if (id == 666)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 667)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 668)
        if (id == 681)
          Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 681)
          if (id == 675)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 675)
            if (id == 672)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 672)
              if (id == 670)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 670)
                if (id == 669)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 670)
                if (id == 671)
                  Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 672)
              if (id == 673)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 674)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 675)
            if (id == 678)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 678)
              if (id == 676)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 677)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 678)
              if (id == 679)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 680)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 681)
          if (id == 687)
            Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 687)
            if (id == 684)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 684)
              if (id == 682)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 683)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 684)
              if (id == 685)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 686)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 687)
            if (id == 690)
              Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 690)
              if (id == 688)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 689)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 690)
              if (id == 691)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 692)
                Some(reader(BaseEntryId.FormatA.KeyFullyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 594
  val maxID = 692
}