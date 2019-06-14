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

object BaseEntryReader2 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 517)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 517)
      if (id == 431)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 431)
        if (id == 388)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 388)
          if (id == 366)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 366)
            if (id == 355)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 355)
              if (id == 350)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 350)
                if (id == 347)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 347)
                  if (id == 345)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 346)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 347)
                  if (id == 348)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 349)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 350)
                if (id == 353)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 353)
                  if (id == 351)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 352)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 353)
                  if (id == 354)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 355)
              if (id == 361)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 361)
                if (id == 358)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 358)
                  if (id == 356)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 357)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 358)
                  if (id == 359)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 360)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 361)
                if (id == 364)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 364)
                  if (id == 362)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 363)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 364)
                  if (id == 365)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 366)
            if (id == 377)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 377)
              if (id == 372)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 372)
                if (id == 369)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 369)
                  if (id == 367)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 368)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 369)
                  if (id == 370)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 371)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 372)
                if (id == 375)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 375)
                  if (id == 373)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 374)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 375)
                  if (id == 376)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 377)
              if (id == 383)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 383)
                if (id == 380)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 380)
                  if (id == 378)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 379)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 380)
                  if (id == 381)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 382)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 383)
                if (id == 386)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 386)
                  if (id == 384)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 385)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 386)
                  if (id == 387)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 388)
          if (id == 410)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 410)
            if (id == 399)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 399)
              if (id == 394)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 394)
                if (id == 391)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 391)
                  if (id == 389)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 390)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 391)
                  if (id == 392)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 393)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 394)
                if (id == 397)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 397)
                  if (id == 395)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 396)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 397)
                  if (id == 398)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 399)
              if (id == 405)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 405)
                if (id == 402)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 402)
                  if (id == 400)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 401)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 402)
                  if (id == 403)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 404)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 405)
                if (id == 408)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 408)
                  if (id == 406)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 407)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 408)
                  if (id == 409)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 410)
            if (id == 421)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 421)
              if (id == 416)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 416)
                if (id == 413)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 413)
                  if (id == 411)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 412)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 413)
                  if (id == 414)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 415)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 416)
                if (id == 419)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 419)
                  if (id == 417)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 418)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 419)
                  if (id == 420)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 421)
              if (id == 426)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 426)
                if (id == 424)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 424)
                  if (id == 422)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 423)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 424)
                  if (id == 425)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 426)
                if (id == 429)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 429)
                  if (id == 427)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 428)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 429)
                  if (id == 430)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 431)
        if (id == 474)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 474)
          if (id == 453)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 453)
            if (id == 442)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 442)
              if (id == 437)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 437)
                if (id == 434)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 434)
                  if (id == 432)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 433)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 434)
                  if (id == 435)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 436)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 437)
                if (id == 440)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 440)
                  if (id == 438)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 439)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 440)
                  if (id == 441)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 442)
              if (id == 448)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 448)
                if (id == 445)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 445)
                  if (id == 443)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 444)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 445)
                  if (id == 446)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 447)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 448)
                if (id == 451)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 451)
                  if (id == 449)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 450)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 451)
                  if (id == 452)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 453)
            if (id == 464)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 464)
              if (id == 459)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 459)
                if (id == 456)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 456)
                  if (id == 454)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 455)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 456)
                  if (id == 457)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 458)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 459)
                if (id == 462)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 462)
                  if (id == 460)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 461)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 462)
                  if (id == 463)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 464)
              if (id == 469)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 469)
                if (id == 467)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 467)
                  if (id == 465)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 466)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 467)
                  if (id == 468)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 469)
                if (id == 472)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 472)
                  if (id == 470)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 471)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 472)
                  if (id == 473)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 474)
          if (id == 496)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 496)
            if (id == 485)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 485)
              if (id == 480)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 480)
                if (id == 477)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 477)
                  if (id == 475)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 476)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 477)
                  if (id == 478)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 479)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 480)
                if (id == 483)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 483)
                  if (id == 481)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 482)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 483)
                  if (id == 484)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 485)
              if (id == 491)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 491)
                if (id == 488)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 488)
                  if (id == 486)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 487)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 488)
                  if (id == 489)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 490)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 491)
                if (id == 494)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 494)
                  if (id == 492)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 493)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 494)
                  if (id == 495)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 496)
            if (id == 507)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 507)
              if (id == 502)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 502)
                if (id == 499)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 499)
                  if (id == 497)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 498)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 499)
                  if (id == 500)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 501)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 502)
                if (id == 505)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 505)
                  if (id == 503)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 504)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 505)
                  if (id == 506)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 507)
              if (id == 512)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 512)
                if (id == 510)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 510)
                  if (id == 508)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 509)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 510)
                  if (id == 511)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 512)
                if (id == 515)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 515)
                  if (id == 513)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 514)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 515)
                  if (id == 516)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 517)
      if (id == 604)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 604)
        if (id == 561)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 561)
          if (id == 539)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 539)
            if (id == 528)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 528)
              if (id == 523)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 523)
                if (id == 520)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 520)
                  if (id == 518)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 519)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 520)
                  if (id == 521)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 522)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 523)
                if (id == 526)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 526)
                  if (id == 524)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 525)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 526)
                  if (id == 527)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 528)
              if (id == 534)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 534)
                if (id == 531)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 531)
                  if (id == 529)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 530)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 531)
                  if (id == 532)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 533)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 534)
                if (id == 537)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 537)
                  if (id == 535)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 536)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 537)
                  if (id == 538)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 539)
            if (id == 550)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 550)
              if (id == 545)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 545)
                if (id == 542)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 542)
                  if (id == 540)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 541)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 542)
                  if (id == 543)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 544)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 545)
                if (id == 548)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 548)
                  if (id == 546)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 547)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 548)
                  if (id == 549)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 550)
              if (id == 556)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 556)
                if (id == 553)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 553)
                  if (id == 551)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 552)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 553)
                  if (id == 554)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 555)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 556)
                if (id == 559)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 559)
                  if (id == 557)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 558)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 559)
                  if (id == 560)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 561)
          if (id == 583)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 583)
            if (id == 572)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 572)
              if (id == 567)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 567)
                if (id == 564)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 564)
                  if (id == 562)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 563)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 564)
                  if (id == 565)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 566)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 567)
                if (id == 570)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 570)
                  if (id == 568)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 569)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 570)
                  if (id == 571)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 572)
              if (id == 578)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 578)
                if (id == 575)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 575)
                  if (id == 573)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 574)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 575)
                  if (id == 576)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 577)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 578)
                if (id == 581)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 581)
                  if (id == 579)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 580)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 581)
                  if (id == 582)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 583)
            if (id == 594)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 594)
              if (id == 589)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 589)
                if (id == 586)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 586)
                  if (id == 584)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 585)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 586)
                  if (id == 587)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 588)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 589)
                if (id == 592)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 592)
                  if (id == 590)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 591)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 592)
                  if (id == 593)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 594)
              if (id == 599)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 599)
                if (id == 597)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 597)
                  if (id == 595)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 596)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 597)
                  if (id == 598)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 599)
                if (id == 602)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 602)
                  if (id == 600)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 601)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 602)
                  if (id == 603)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
      else if (id > 604)
        if (id == 647)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 647)
          if (id == 626)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 626)
            if (id == 615)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 615)
              if (id == 610)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 610)
                if (id == 607)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 607)
                  if (id == 605)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 606)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 607)
                  if (id == 608)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 609)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 610)
                if (id == 613)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 613)
                  if (id == 611)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 612)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 613)
                  if (id == 614)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 615)
              if (id == 621)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 621)
                if (id == 618)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 618)
                  if (id == 616)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 617)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 618)
                  if (id == 619)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 620)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 621)
                if (id == 624)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 624)
                  if (id == 622)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 623)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 624)
                  if (id == 625)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 626)
            if (id == 637)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 637)
              if (id == 632)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 632)
                if (id == 629)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 629)
                  if (id == 627)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 628)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 629)
                  if (id == 630)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 631)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 632)
                if (id == 635)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 635)
                  if (id == 633)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 634)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 635)
                  if (id == 636)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 637)
              if (id == 642)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 642)
                if (id == 640)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 640)
                  if (id == 638)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 639)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 640)
                  if (id == 641)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 642)
                if (id == 645)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 645)
                  if (id == 643)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 644)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 645)
                  if (id == 646)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
        else if (id > 647)
          if (id == 669)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 669)
            if (id == 658)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 658)
              if (id == 653)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 653)
                if (id == 650)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 650)
                  if (id == 648)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 649)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 650)
                  if (id == 651)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 652)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 653)
                if (id == 656)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 656)
                  if (id == 654)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 655)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 656)
                  if (id == 657)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 658)
              if (id == 664)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 664)
                if (id == 661)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 661)
                  if (id == 659)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 660)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 661)
                  if (id == 662)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 663)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 664)
                if (id == 667)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 667)
                  if (id == 665)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 666)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 667)
                  if (id == 668)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (id > 669)
            if (id == 680)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 680)
              if (id == 675)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 675)
                if (id == 672)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 672)
                  if (id == 670)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 671)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 672)
                  if (id == 673)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 674)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 675)
                if (id == 678)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 678)
                  if (id == 676)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 677)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 678)
                  if (id == 679)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (id > 680)
              if (id == 685)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 685)
                if (id == 683)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 683)
                  if (id == 681)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 682)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 683)
                  if (id == 684)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else
                  None
              else if (id > 685)
                if (id == 688)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else if (id < 688)
                  if (id == 686)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else if (id == 687)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                  else
                    None
                else if (id > 688)
                  if (id == 689)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 345
  val maxID = 689
}