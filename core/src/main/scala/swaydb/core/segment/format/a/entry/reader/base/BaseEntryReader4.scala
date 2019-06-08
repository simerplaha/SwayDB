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

object BaseEntryReader4 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 346)
      Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 346)
      if (id == 321)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 321)
        if (id == 309)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 309)
          if (id == 303)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 303)
            if (id == 300)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 300)
              if (id == 298)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 298)
                if (id == 297)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 298)
                if (id == 299)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 300)
              if (id == 301)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 302)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 303)
            if (id == 306)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 306)
              if (id == 304)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 305)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 306)
              if (id == 307)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 308)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 309)
          if (id == 315)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 315)
            if (id == 312)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 312)
              if (id == 310)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 311)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 312)
              if (id == 313)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 314)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 315)
            if (id == 318)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 318)
              if (id == 316)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 317)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 318)
              if (id == 319)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 320)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 321)
        if (id == 334)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 334)
          if (id == 328)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 328)
            if (id == 325)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 325)
              if (id == 323)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 323)
                if (id == 322)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 323)
                if (id == 324)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 325)
              if (id == 326)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 327)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 328)
            if (id == 331)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 331)
              if (id == 329)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 330)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 331)
              if (id == 332)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 333)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 334)
          if (id == 340)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 340)
            if (id == 337)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 337)
              if (id == 335)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 336)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 337)
              if (id == 338)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 339)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 340)
            if (id == 343)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 343)
              if (id == 341)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 342)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 343)
              if (id == 344)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 345)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 346)
      if (id == 371)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 371)
        if (id == 359)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 359)
          if (id == 353)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 353)
            if (id == 350)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 350)
              if (id == 348)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 348)
                if (id == 347)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 348)
                if (id == 349)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 350)
              if (id == 351)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 352)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 353)
            if (id == 356)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 356)
              if (id == 354)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 355)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 356)
              if (id == 357)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 358)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 359)
          if (id == 365)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 365)
            if (id == 362)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 362)
              if (id == 360)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 361)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 362)
              if (id == 363)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 364)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 365)
            if (id == 368)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 368)
              if (id == 366)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 367)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 368)
              if (id == 369)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 370)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 371)
        if (id == 384)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 384)
          if (id == 378)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 378)
            if (id == 375)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 375)
              if (id == 373)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 373)
                if (id == 372)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 373)
                if (id == 374)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 375)
              if (id == 376)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 377)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 378)
            if (id == 381)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 381)
              if (id == 379)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 380)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 381)
              if (id == 382)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 383)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 384)
          if (id == 390)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 390)
            if (id == 387)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 387)
              if (id == 385)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 386)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 387)
              if (id == 388)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 389)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 390)
            if (id == 393)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 393)
              if (id == 391)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 392)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 393)
              if (id == 394)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 395)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 297
  val maxID = 395
}