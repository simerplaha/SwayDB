/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.entry.reader.base

import swaydb.core.segment.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.entry.reader.BaseEntryApplier

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
