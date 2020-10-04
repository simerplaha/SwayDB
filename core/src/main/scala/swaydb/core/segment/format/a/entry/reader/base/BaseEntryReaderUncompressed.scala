/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.entry.reader.base

import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.BaseEntryApplier

private[core] object BaseEntryReaderUncompressed extends BaseEntryReader {

  def read[T](baseId: Int,
              reader: BaseEntryApplier[T]): T =
  //GENERATED CONDITIONS
    if (baseId == 95)
      reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed)
    else if (baseId < 95)
      if (baseId == 23)
        reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline)
      else if (baseId < 23)
        if (baseId == 14)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.NoDeadline)
        else if (baseId < 14)
          if (baseId == 13)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline)
          else
            throw swaydb.Exception.InvalidBaseId(baseId)
        else if (baseId > 14)
          if (baseId == 19)
            reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline)
          else
            throw swaydb.Exception.InvalidBaseId(baseId)
        else
          throw swaydb.Exception.InvalidBaseId(baseId)
      else if (baseId > 23)
        if (baseId == 24)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.NoDeadline)
        else if (baseId == 29)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline)
        else
          throw swaydb.Exception.InvalidBaseId(baseId)
      else
        throw swaydb.Exception.InvalidBaseId(baseId)
    else if (baseId > 95)
      if (baseId == 141)
        reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed)
      else if (baseId < 141)
        if (baseId == 96)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineUncompressed)
        else if (baseId == 121)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed)
        else
          throw swaydb.Exception.InvalidBaseId(baseId)
      else if (baseId > 141)
        if (baseId == 142)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineUncompressed)
        else if (baseId == 167)
          reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueFullyCompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed)
        else
          throw swaydb.Exception.InvalidBaseId(baseId)
      else
        throw swaydb.Exception.InvalidBaseId(baseId)
    else
      throw swaydb.Exception.InvalidBaseId(baseId)

  val minID = 13
  val maxID = 167
}
