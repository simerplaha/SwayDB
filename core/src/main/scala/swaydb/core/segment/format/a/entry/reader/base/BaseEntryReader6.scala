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

object BaseEntryReader6 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 544)
      Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 544)
      if (id == 519)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 519)
        if (id == 507)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 507)
          if (id == 501)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 501)
            if (id == 498)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 498)
              if (id == 496)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 496)
                if (id == 495)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 496)
                if (id == 497)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 498)
              if (id == 499)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 500)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 501)
            if (id == 504)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 504)
              if (id == 502)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 503)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 504)
              if (id == 505)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 506)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 507)
          if (id == 513)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 513)
            if (id == 510)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 510)
              if (id == 508)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 509)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 510)
              if (id == 511)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 512)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 513)
            if (id == 516)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 516)
              if (id == 514)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 515)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 516)
              if (id == 517)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 518)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 519)
        if (id == 532)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 532)
          if (id == 526)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 526)
            if (id == 523)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 523)
              if (id == 521)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 521)
                if (id == 520)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 521)
                if (id == 522)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 523)
              if (id == 524)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 525)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 526)
            if (id == 529)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 529)
              if (id == 527)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 528)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 529)
              if (id == 530)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 531)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 532)
          if (id == 538)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 538)
            if (id == 535)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 535)
              if (id == 533)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 534)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 535)
              if (id == 536)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 537)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 538)
            if (id == 541)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 541)
              if (id == 539)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 540)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 541)
              if (id == 542)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 543)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 544)
      if (id == 569)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 569)
        if (id == 557)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 557)
          if (id == 551)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 551)
            if (id == 548)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 548)
              if (id == 546)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 546)
                if (id == 545)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 546)
                if (id == 547)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 548)
              if (id == 549)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 550)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 551)
            if (id == 554)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 554)
              if (id == 552)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 553)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 554)
              if (id == 555)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 556)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 557)
          if (id == 563)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 563)
            if (id == 560)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 560)
              if (id == 558)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 559)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 560)
              if (id == 561)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 562)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 563)
            if (id == 566)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 566)
              if (id == 564)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 565)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 566)
              if (id == 567)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 568)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 569)
        if (id == 582)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 582)
          if (id == 576)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 576)
            if (id == 573)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 573)
              if (id == 571)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 571)
                if (id == 570)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 571)
                if (id == 572)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 573)
              if (id == 574)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 575)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 576)
            if (id == 579)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 579)
              if (id == 577)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 578)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 579)
              if (id == 580)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 581)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 582)
          if (id == 588)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 588)
            if (id == 585)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 585)
              if (id == 583)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 584)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 585)
              if (id == 586)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 587)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 588)
            if (id == 591)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 591)
              if (id == 589)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 590)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 591)
              if (id == 592)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 593)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 495
  val maxID = 593
}