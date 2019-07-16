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
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.IO
import swaydb.data.slice.Reader

private[core] object BaseEntryReader2 extends BaseEntryReader {

  def read[T](baseId: Int,
              keyValueId: Int,
              indexReader: Reader,
              valueReader: Option[UnblockedReader[ValuesBlock]],
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              accessPosition: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (baseId == 517)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
    else if (baseId < 517)
      if (baseId == 431)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
      else if (baseId < 431)
        if (baseId == 388)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 388)
          if (baseId == 366)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 366)
            if (baseId == 355)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 355)
              if (baseId == 350)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 350)
                if (baseId == 347)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 347)
                  if (baseId == 345)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 346)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 347)
                  if (baseId == 348)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 349)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 350)
                if (baseId == 353)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 353)
                  if (baseId == 351)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 352)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 353)
                  if (baseId == 354)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 355)
              if (baseId == 361)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 361)
                if (baseId == 358)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 358)
                  if (baseId == 356)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 357)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 358)
                  if (baseId == 359)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 360)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 361)
                if (baseId == 364)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 364)
                  if (baseId == 362)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 363)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 364)
                  if (baseId == 365)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 366)
            if (baseId == 377)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 377)
              if (baseId == 372)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 372)
                if (baseId == 369)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 369)
                  if (baseId == 367)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 368)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 369)
                  if (baseId == 370)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 371)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 372)
                if (baseId == 375)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 375)
                  if (baseId == 373)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 374)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 375)
                  if (baseId == 376)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 377)
              if (baseId == 383)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 383)
                if (baseId == 380)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 380)
                  if (baseId == 378)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 379)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 380)
                  if (baseId == 381)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 382)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 383)
                if (baseId == 386)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 386)
                  if (baseId == 384)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 385)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 386)
                  if (baseId == 387)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 388)
          if (baseId == 410)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 410)
            if (baseId == 399)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 399)
              if (baseId == 394)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 394)
                if (baseId == 391)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 391)
                  if (baseId == 389)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 390)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 391)
                  if (baseId == 392)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 393)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 394)
                if (baseId == 397)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 397)
                  if (baseId == 395)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 396)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 397)
                  if (baseId == 398)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 399)
              if (baseId == 405)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 405)
                if (baseId == 402)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 402)
                  if (baseId == 400)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 401)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 402)
                  if (baseId == 403)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 404)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 405)
                if (baseId == 408)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 408)
                  if (baseId == 406)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 407)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 408)
                  if (baseId == 409)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 410)
            if (baseId == 421)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 421)
              if (baseId == 416)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 416)
                if (baseId == 413)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 413)
                  if (baseId == 411)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 412)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 413)
                  if (baseId == 414)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 415)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 416)
                if (baseId == 419)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 419)
                  if (baseId == 417)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 418)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 419)
                  if (baseId == 420)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 421)
              if (baseId == 426)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 426)
                if (baseId == 424)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 424)
                  if (baseId == 422)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 423)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 424)
                  if (baseId == 425)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 426)
                if (baseId == 429)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 429)
                  if (baseId == 427)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 428)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 429)
                  if (baseId == 430)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
      else if (baseId > 431)
        if (baseId == 474)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 474)
          if (baseId == 453)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 453)
            if (baseId == 442)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 442)
              if (baseId == 437)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 437)
                if (baseId == 434)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 434)
                  if (baseId == 432)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 433)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 434)
                  if (baseId == 435)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 436)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 437)
                if (baseId == 440)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 440)
                  if (baseId == 438)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 439)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 440)
                  if (baseId == 441)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 442)
              if (baseId == 448)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 448)
                if (baseId == 445)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 445)
                  if (baseId == 443)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 444)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 445)
                  if (baseId == 446)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 447)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 448)
                if (baseId == 451)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 451)
                  if (baseId == 449)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 450)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 451)
                  if (baseId == 452)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 453)
            if (baseId == 464)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 464)
              if (baseId == 459)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 459)
                if (baseId == 456)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 456)
                  if (baseId == 454)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 455)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 456)
                  if (baseId == 457)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 458)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 459)
                if (baseId == 462)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 462)
                  if (baseId == 460)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 461)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 462)
                  if (baseId == 463)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 464)
              if (baseId == 469)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 469)
                if (baseId == 467)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 467)
                  if (baseId == 465)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 466)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 467)
                  if (baseId == 468)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 469)
                if (baseId == 472)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 472)
                  if (baseId == 470)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 471)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 472)
                  if (baseId == 473)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 474)
          if (baseId == 496)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 496)
            if (baseId == 485)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 485)
              if (baseId == 480)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 480)
                if (baseId == 477)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 477)
                  if (baseId == 475)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 476)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 477)
                  if (baseId == 478)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 479)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 480)
                if (baseId == 483)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 483)
                  if (baseId == 481)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 482)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 483)
                  if (baseId == 484)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 485)
              if (baseId == 491)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 491)
                if (baseId == 488)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 488)
                  if (baseId == 486)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 487)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 488)
                  if (baseId == 489)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 490)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 491)
                if (baseId == 494)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 494)
                  if (baseId == 492)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 493)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 494)
                  if (baseId == 495)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 496)
            if (baseId == 507)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 507)
              if (baseId == 502)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 502)
                if (baseId == 499)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 499)
                  if (baseId == 497)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 498)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 499)
                  if (baseId == 500)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 501)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 502)
                if (baseId == 505)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 505)
                  if (baseId == 503)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 504)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 505)
                  if (baseId == 506)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 507)
              if (baseId == 512)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 512)
                if (baseId == 510)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 510)
                  if (baseId == 508)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 509)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 510)
                  if (baseId == 511)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 512)
                if (baseId == 515)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 515)
                  if (baseId == 513)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 514)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 515)
                  if (baseId == 516)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
    else if (baseId > 517)
      if (baseId == 604)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
      else if (baseId < 604)
        if (baseId == 561)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 561)
          if (baseId == 539)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 539)
            if (baseId == 528)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 528)
              if (baseId == 523)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 523)
                if (baseId == 520)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 520)
                  if (baseId == 518)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 519)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 520)
                  if (baseId == 521)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 522)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 523)
                if (baseId == 526)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 526)
                  if (baseId == 524)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 525)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 526)
                  if (baseId == 527)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 528)
              if (baseId == 534)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 534)
                if (baseId == 531)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 531)
                  if (baseId == 529)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 530)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 531)
                  if (baseId == 532)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 533)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 534)
                if (baseId == 537)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 537)
                  if (baseId == 535)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 536)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 537)
                  if (baseId == 538)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 539)
            if (baseId == 550)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 550)
              if (baseId == 545)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 545)
                if (baseId == 542)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 542)
                  if (baseId == 540)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 541)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 542)
                  if (baseId == 543)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 544)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 545)
                if (baseId == 548)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 548)
                  if (baseId == 546)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 547)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 548)
                  if (baseId == 549)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 550)
              if (baseId == 556)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 556)
                if (baseId == 553)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 553)
                  if (baseId == 551)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 552)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 553)
                  if (baseId == 554)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 555)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 556)
                if (baseId == 559)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 559)
                  if (baseId == 557)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 558)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 559)
                  if (baseId == 560)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 561)
          if (baseId == 583)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 583)
            if (baseId == 572)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 572)
              if (baseId == 567)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 567)
                if (baseId == 564)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 564)
                  if (baseId == 562)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 563)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 564)
                  if (baseId == 565)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 566)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 567)
                if (baseId == 570)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 570)
                  if (baseId == 568)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 569)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 570)
                  if (baseId == 571)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 572)
              if (baseId == 578)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 578)
                if (baseId == 575)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 575)
                  if (baseId == 573)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 574)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 575)
                  if (baseId == 576)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 577)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 578)
                if (baseId == 581)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 581)
                  if (baseId == 579)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 580)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 581)
                  if (baseId == 582)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 583)
            if (baseId == 594)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 594)
              if (baseId == 589)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 589)
                if (baseId == 586)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 586)
                  if (baseId == 584)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 585)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 586)
                  if (baseId == 587)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 588)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 589)
                if (baseId == 592)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 592)
                  if (baseId == 590)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 591)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 592)
                  if (baseId == 593)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 594)
              if (baseId == 599)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 599)
                if (baseId == 597)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 597)
                  if (baseId == 595)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 596)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 597)
                  if (baseId == 598)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 599)
                if (baseId == 602)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 602)
                  if (baseId == 600)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 601)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 602)
                  if (baseId == 603)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
      else if (baseId > 604)
        if (baseId == 647)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId < 647)
          if (baseId == 626)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 626)
            if (baseId == 615)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 615)
              if (baseId == 610)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 610)
                if (baseId == 607)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 607)
                  if (baseId == 605)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 606)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 607)
                  if (baseId == 608)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 609)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 610)
                if (baseId == 613)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 613)
                  if (baseId == 611)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 612)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 613)
                  if (baseId == 614)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 615)
              if (baseId == 621)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 621)
                if (baseId == 618)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 618)
                  if (baseId == 616)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 617)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 618)
                  if (baseId == 619)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 620)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 621)
                if (baseId == 624)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 624)
                  if (baseId == 622)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 623)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 624)
                  if (baseId == 625)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 626)
            if (baseId == 637)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 637)
              if (baseId == 632)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 632)
                if (baseId == 629)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 629)
                  if (baseId == 627)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 628)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 629)
                  if (baseId == 630)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 631)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 632)
                if (baseId == 635)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 635)
                  if (baseId == 633)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 634)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 635)
                  if (baseId == 636)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 637)
              if (baseId == 642)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 642)
                if (baseId == 640)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 640)
                  if (baseId == 638)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 639)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 640)
                  if (baseId == 641)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 642)
                if (baseId == 645)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 645)
                  if (baseId == 643)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 644)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 645)
                  if (baseId == 646)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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
        else if (baseId > 647)
          if (baseId == 669)
            Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
          else if (baseId < 669)
            if (baseId == 658)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 658)
              if (baseId == 653)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 653)
                if (baseId == 650)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 650)
                  if (baseId == 648)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 649)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 650)
                  if (baseId == 651)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 652)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 653)
                if (baseId == 656)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 656)
                  if (baseId == 654)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 655)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 656)
                  if (baseId == 657)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 658)
              if (baseId == 664)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 664)
                if (baseId == 661)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 661)
                  if (baseId == 659)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 660)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 661)
                  if (baseId == 662)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 663)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 664)
                if (baseId == 667)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 667)
                  if (baseId == 665)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 666)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 667)
                  if (baseId == 668)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else
              None
          else if (baseId > 669)
            if (baseId == 680)
              Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
            else if (baseId < 680)
              if (baseId == 675)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 675)
                if (baseId == 672)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 672)
                  if (baseId == 670)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 671)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 672)
                  if (baseId == 673)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 674)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 675)
                if (baseId == 678)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 678)
                  if (baseId == 676)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 677)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 678)
                  if (baseId == 679)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else
                None
            else if (baseId > 680)
              if (baseId == 685)
                Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
              else if (baseId < 685)
                if (baseId == 683)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 683)
                  if (baseId == 681)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 682)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 683)
                  if (baseId == 684)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else
                  None
              else if (baseId > 685)
                if (baseId == 688)
                  Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                else if (baseId < 688)
                  if (baseId == 686)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else if (baseId == 687)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
                  else
                    None
                else if (baseId > 688)
                  if (baseId == 689)
                    Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
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