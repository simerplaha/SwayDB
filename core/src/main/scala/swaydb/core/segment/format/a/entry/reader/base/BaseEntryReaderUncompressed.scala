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
import swaydb.core.segment.format.a.block.reader.DecompressedBlockReader
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.IO
import swaydb.data.slice.Reader

private[core] object BaseEntryReaderUncompressed extends BaseEntryReader {

  def read[T](baseId: Int,
              keyValueId: Int,
              indexReader: Reader,
              valueReader: Option[DecompressedBlockReader[ValuesBlock]],
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              accessPosition: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (baseId == 1110)
      Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
    else if (baseId < 1110)
      if (baseId == 660)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
      else if (baseId < 660)
        if (baseId == 650)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else if (baseId == 659)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else
          None
      else if (baseId > 660)
        if (baseId == 669)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.NoTime.NoValue.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else
          None
      else
        None
    else if (baseId > 1110)
      if (baseId == 1120)
        Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.NoDeadline, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
      else if (baseId < 1120)
        if (baseId == 1119)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else
          None
      else if (baseId > 1120)
        if (baseId == 1129)
          Some(reader(BaseEntryIdFormatA.FormatA1.KeyStart.TimeUncompressed.NoValue.DeadlineUncompressed, keyValueId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, accessPosition, previous))
        else
          None
      else
        None
    else
      None

  val minID = 650
  val maxID = 1129
}
