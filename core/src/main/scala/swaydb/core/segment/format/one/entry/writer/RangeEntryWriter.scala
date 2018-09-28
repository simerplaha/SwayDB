/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.one.entry.writer

import swaydb.core.data.Transient
import swaydb.core.segment.format.one.entry.id.{RangeKeyFullyCompressedEntryId, RangeKeyPartiallyCompressedEntryId, RangeKeyUncompressedEntryId}
import swaydb.core.util.Bytes._
import swaydb.data.slice.Slice

object RangeEntryWriter {

  /**
    * @return indexEntry, valueBytes, valueOffsetBytes, nextValuesOffsetPosition
    */
  def write(current: Transient.Range,
            compressDuplicateValues: Boolean): (Slice[Byte], Option[Slice[Byte]], Int, Int) =
    current.previous flatMap {
      previous =>
        compress(key = current.fullKey, previous = previous, minimumCommonBytes = 2) map {
          case (_, remainingBytes) if remainingBytes.isEmpty =>
            val (indexBytes, valueBytes, valueStartOffset, valueEndOffset) =
              ValueWriter.write(
                current = current,
                compressDuplicateValues = compressDuplicateValues,
                id = RangeKeyFullyCompressedEntryId.KeyFullyCompressed,
                plusSize = sizeOf(current.fullKey.size) //write the size of keys that were compressed.
              )

//            assert(indexBytes.isFull, s"indexSlice is not full actual: ${indexBytes.written} - expected: ${indexBytes.size}")
//            valueBytes foreach (valueBytes => assert(valueBytes.isFull, s"valueBytes is not full actual: ${valueBytes.written} - expected: ${valueBytes.size}"))
            (indexBytes.addIntUnsigned(current.fullKey.size), valueBytes, valueStartOffset, valueEndOffset)

          case (commonBytes, remainingBytes) =>
            val (indexBytes, valueBytes, valueStartOffset, valueEndOffset) =
              ValueWriter.write(
                current = current,
                compressDuplicateValues = compressDuplicateValues,
                id = RangeKeyPartiallyCompressedEntryId.KeyPartiallyCompressed,
                plusSize = sizeOf(commonBytes) + remainingBytes.size //write the size of keys compressed and also the uncompressed Bytes
              )
            (indexBytes.addIntUnsigned(commonBytes).addAll(remainingBytes), valueBytes, valueStartOffset, valueEndOffset)
        }
    } getOrElse {
      //no common prefixes or no previous write without compression
      val (indexBytes, valueBytes, valueStartOffset, valueEndOffset) =
        ValueWriter.write(
          current = current,
          compressDuplicateValues = compressDuplicateValues,
          id = RangeKeyUncompressedEntryId.KeyUncompressed,
          plusSize = current.fullKey.size //write key bytes.
        )
      (indexBytes.addAll(current.fullKey), valueBytes, valueStartOffset, valueEndOffset)
    }
}