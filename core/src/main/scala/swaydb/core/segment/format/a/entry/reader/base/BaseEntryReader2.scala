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
import swaydb.data.slice.ReaderBase

private[core] object BaseEntryReader2 extends BaseEntryReader {

  def read[T](baseId: Int,
              keyValueId: Int,
              sortedIndexAccessPosition: Int,
              keyInfo: Option[Either[Int, Persistent.Partial.Key]],
              indexReader: ReaderBase,
              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent.Partial],
              reader: EntryReader[T]): T =
  //GENERATED CONDITIONS
    if (baseId == 517)
      reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
    else if (baseId < 517)
      if (baseId == 431)
        reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
      else if (baseId < 431)
        if (baseId == 388)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
        else if (baseId < 388)
          if (baseId == 366)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
          else if (baseId < 366)
            if (baseId == 355)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 355)
              if (baseId == 350)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 350)
                if (baseId == 347)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 347)
                  if (baseId == 345)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 346)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 347)
                  if (baseId == 348)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 349)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 350)
                if (baseId == 353)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 353)
                  if (baseId == 351)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 352)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 353)
                  if (baseId == 354)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 355)
              if (baseId == 361)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 361)
                if (baseId == 358)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 358)
                  if (baseId == 356)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 357)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 358)
                  if (baseId == 359)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 360)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 361)
                if (baseId == 364)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 364)
                  if (baseId == 362)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 363)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 364)
                  if (baseId == 365)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 366)
            if (baseId == 377)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 377)
              if (baseId == 372)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 372)
                if (baseId == 369)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 369)
                  if (baseId == 367)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 368)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 369)
                  if (baseId == 370)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 371)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 372)
                if (baseId == 375)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 375)
                  if (baseId == 373)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 374)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 375)
                  if (baseId == 376)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 377)
              if (baseId == 383)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 383)
                if (baseId == 380)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 380)
                  if (baseId == 378)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 379)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 380)
                  if (baseId == 381)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 382)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 383)
                if (baseId == 386)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 386)
                  if (baseId == 384)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 385)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 386)
                  if (baseId == 387)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 388)
          if (baseId == 410)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
          else if (baseId < 410)
            if (baseId == 399)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 399)
              if (baseId == 394)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 394)
                if (baseId == 391)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 391)
                  if (baseId == 389)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 390)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 391)
                  if (baseId == 392)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 393)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 394)
                if (baseId == 397)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 397)
                  if (baseId == 395)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 396)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 397)
                  if (baseId == 398)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 399)
              if (baseId == 405)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 405)
                if (baseId == 402)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 402)
                  if (baseId == 400)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 401)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 402)
                  if (baseId == 403)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 404)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 405)
                if (baseId == 408)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 408)
                  if (baseId == 406)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 407)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 408)
                  if (baseId == 409)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 410)
            if (baseId == 421)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 421)
              if (baseId == 416)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 416)
                if (baseId == 413)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 413)
                  if (baseId == 411)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 412)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 413)
                  if (baseId == 414)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 415)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 416)
                if (baseId == 419)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 419)
                  if (baseId == 417)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 418)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 419)
                  if (baseId == 420)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 421)
              if (baseId == 426)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 426)
                if (baseId == 424)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 424)
                  if (baseId == 422)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 423)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 424)
                  if (baseId == 425)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 426)
                if (baseId == 429)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 429)
                  if (baseId == 427)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 428)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 429)
                  if (baseId == 430)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else if (baseId > 431)
        if (baseId == 474)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
        else if (baseId < 474)
          if (baseId == 453)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
          else if (baseId < 453)
            if (baseId == 442)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 442)
              if (baseId == 437)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 437)
                if (baseId == 434)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 434)
                  if (baseId == 432)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 433)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 434)
                  if (baseId == 435)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 436)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 437)
                if (baseId == 440)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 440)
                  if (baseId == 438)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 439)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 440)
                  if (baseId == 441)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 442)
              if (baseId == 448)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 448)
                if (baseId == 445)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 445)
                  if (baseId == 443)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.NoValue.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 444)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 445)
                  if (baseId == 446)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 447)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 448)
                if (baseId == 451)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 451)
                  if (baseId == 449)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 450)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 451)
                  if (baseId == 452)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 453)
            if (baseId == 464)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 464)
              if (baseId == 459)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 459)
                if (baseId == 456)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 456)
                  if (baseId == 454)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 455)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 456)
                  if (baseId == 457)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 458)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 459)
                if (baseId == 462)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 462)
                  if (baseId == 460)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 461)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 462)
                  if (baseId == 463)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 464)
              if (baseId == 469)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 469)
                if (baseId == 467)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 467)
                  if (baseId == 465)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 466)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 467)
                  if (baseId == 468)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 469)
                if (baseId == 472)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 472)
                  if (baseId == 470)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 471)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 472)
                  if (baseId == 473)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 474)
          if (baseId == 496)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
          else if (baseId < 496)
            if (baseId == 485)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 485)
              if (baseId == 480)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 480)
                if (baseId == 477)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 477)
                  if (baseId == 475)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 476)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 477)
                  if (baseId == 478)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 479)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 480)
                if (baseId == 483)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 483)
                  if (baseId == 481)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 482)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 483)
                  if (baseId == 484)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 485)
              if (baseId == 491)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 491)
                if (baseId == 488)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 488)
                  if (baseId == 486)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 487)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 488)
                  if (baseId == 489)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 490)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 491)
                if (baseId == 494)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 494)
                  if (baseId == 492)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 493)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 494)
                  if (baseId == 495)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 496)
            if (baseId == 507)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 507)
              if (baseId == 502)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 502)
                if (baseId == 499)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 499)
                  if (baseId == 497)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 498)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 499)
                  if (baseId == 500)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 501)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 502)
                if (baseId == 505)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 505)
                  if (baseId == 503)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 504)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 505)
                  if (baseId == 506)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 507)
              if (baseId == 512)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 512)
                if (baseId == 510)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 510)
                  if (baseId == 508)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 509)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 510)
                  if (baseId == 511)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 512)
                if (baseId == 515)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 515)
                  if (baseId == 513)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 514)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 515)
                  if (baseId == 516)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else
        throw swaydb.Exception.InvalidKeyValueId(baseId)
    else if (baseId > 517)
      if (baseId == 604)
        reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
      else if (baseId < 604)
        if (baseId == 561)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
        else if (baseId < 561)
          if (baseId == 539)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
          else if (baseId < 539)
            if (baseId == 528)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 528)
              if (baseId == 523)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 523)
                if (baseId == 520)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 520)
                  if (baseId == 518)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 519)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 520)
                  if (baseId == 521)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 522)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetTwoCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 523)
                if (baseId == 526)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 526)
                  if (baseId == 524)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 525)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 526)
                  if (baseId == 527)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 528)
              if (baseId == 534)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 534)
                if (baseId == 531)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 531)
                  if (baseId == 529)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 530)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 531)
                  if (baseId == 532)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 533)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 534)
                if (baseId == 537)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 537)
                  if (baseId == 535)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 536)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 537)
                  if (baseId == 538)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 539)
            if (baseId == 550)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 550)
              if (baseId == 545)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 545)
                if (baseId == 542)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 542)
                  if (baseId == 540)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 541)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 542)
                  if (baseId == 543)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 544)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 545)
                if (baseId == 548)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 548)
                  if (baseId == 546)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 547)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 548)
                  if (baseId == 549)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 550)
              if (baseId == 556)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 556)
                if (baseId == 553)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 553)
                  if (baseId == 551)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 552)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 553)
                  if (baseId == 554)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 555)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 556)
                if (baseId == 559)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 559)
                  if (baseId == 557)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 558)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 559)
                  if (baseId == 560)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 561)
          if (baseId == 583)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
          else if (baseId < 583)
            if (baseId == 572)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 572)
              if (baseId == 567)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 567)
                if (baseId == 564)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 564)
                  if (baseId == 562)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 563)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetThreeCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 564)
                  if (baseId == 565)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 566)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 567)
                if (baseId == 570)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 570)
                  if (baseId == 568)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 569)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 570)
                  if (baseId == 571)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 572)
              if (baseId == 578)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 578)
                if (baseId == 575)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 575)
                  if (baseId == 573)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 574)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 575)
                  if (baseId == 576)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 577)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 578)
                if (baseId == 581)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 581)
                  if (baseId == 579)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 580)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 581)
                  if (baseId == 582)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 583)
            if (baseId == 594)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 594)
              if (baseId == 589)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 589)
                if (baseId == 586)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 586)
                  if (baseId == 584)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 585)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 586)
                  if (baseId == 587)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 588)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 589)
                if (baseId == 592)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 592)
                  if (baseId == 590)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 591)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 592)
                  if (baseId == 593)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 594)
              if (baseId == 599)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 599)
                if (baseId == 597)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 597)
                  if (baseId == 595)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 596)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 597)
                  if (baseId == 598)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 599)
                if (baseId == 602)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 602)
                  if (baseId == 600)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 601)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 602)
                  if (baseId == 603)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetFullyCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else if (baseId > 604)
        if (baseId == 647)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
        else if (baseId < 647)
          if (baseId == 626)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
          else if (baseId < 626)
            if (baseId == 615)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 615)
              if (baseId == 610)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 610)
                if (baseId == 607)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 607)
                  if (baseId == 605)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 606)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 607)
                  if (baseId == 608)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 609)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 610)
                if (baseId == 613)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 613)
                  if (baseId == 611)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 612)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 613)
                  if (baseId == 614)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 615)
              if (baseId == 621)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 621)
                if (baseId == 618)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 618)
                  if (baseId == 616)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 617)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 618)
                  if (baseId == 619)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 620)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 621)
                if (baseId == 624)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 624)
                  if (baseId == 622)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 623)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 624)
                  if (baseId == 625)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 626)
            if (baseId == 637)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 637)
              if (baseId == 632)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 632)
                if (baseId == 629)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 629)
                  if (baseId == 627)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 628)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 629)
                  if (baseId == 630)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 631)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 632)
                if (baseId == 635)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 635)
                  if (baseId == 633)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 634)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 635)
                  if (baseId == 636)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 637)
              if (baseId == 642)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 642)
                if (baseId == 640)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 640)
                  if (baseId == 638)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 639)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 640)
                  if (baseId == 641)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 642)
                if (baseId == 645)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 645)
                  if (baseId == 643)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimePartiallyCompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 644)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 645)
                  if (baseId == 646)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else if (baseId > 647)
          if (baseId == 669)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
          else if (baseId < 669)
            if (baseId == 658)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 658)
              if (baseId == 653)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 653)
                if (baseId == 650)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 650)
                  if (baseId == 648)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 649)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 650)
                  if (baseId == 651)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 652)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 653)
                if (baseId == 656)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 656)
                  if (baseId == 654)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 655)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 656)
                  if (baseId == 657)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 658)
              if (baseId == 664)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 664)
                if (baseId == 661)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 661)
                  if (baseId == 659)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 660)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 661)
                  if (baseId == 662)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 663)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 664)
                if (baseId == 667)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 667)
                  if (baseId == 665)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 666)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 667)
                  if (baseId == 668)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else if (baseId > 669)
            if (baseId == 680)
              reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
            else if (baseId < 680)
              if (baseId == 675)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 675)
                if (baseId == 672)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 672)
                  if (baseId == 670)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 671)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 672)
                  if (baseId == 673)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 674)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 675)
                if (baseId == 678)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 678)
                  if (baseId == 676)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 677)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 678)
                  if (baseId == 679)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else if (baseId > 680)
              if (baseId == 685)
                reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
              else if (baseId < 685)
                if (baseId == 683)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 683)
                  if (baseId == 681)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 682)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 683)
                  if (baseId == 684)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else if (baseId > 685)
                if (baseId == 688)
                  reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                else if (baseId < 688)
                  if (baseId == 686)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else if (baseId == 687)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else if (baseId > 688)
                  if (baseId == 689)
                    reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, keyValueId, sortedIndexAccessPosition, keyInfo, indexReader, valuesReader, indexOffset, nextIndexOffset, nextIndexSize, previous)
                  else
                    throw swaydb.Exception.InvalidKeyValueId(baseId)
                else
                  throw swaydb.Exception.InvalidKeyValueId(baseId)
              else
                throw swaydb.Exception.InvalidKeyValueId(baseId)
            else
              throw swaydb.Exception.InvalidKeyValueId(baseId)
          else
            throw swaydb.Exception.InvalidKeyValueId(baseId)
        else
          throw swaydb.Exception.InvalidKeyValueId(baseId)
      else
        throw swaydb.Exception.InvalidKeyValueId(baseId)
    else
      throw swaydb.Exception.InvalidKeyValueId(baseId)

  val minID = 345
  val maxID = 689
}