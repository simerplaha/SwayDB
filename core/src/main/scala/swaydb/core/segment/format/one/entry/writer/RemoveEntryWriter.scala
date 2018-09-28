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
import swaydb.core.segment.format.one.entry.id.RemoveEntryId
import swaydb.core.util.Bytes._
import swaydb.data.slice.Slice

object RemoveEntryWriter {

  def write(keyValue: Transient.Remove): Slice[Byte] =
    keyValue.previous flatMap {
      previous =>
        compress(key = keyValue.key, previous = previous, minimumCommonBytes = 2) map {
          case (_, remainingBytes) if remainingBytes.isEmpty =>
            val indexBytes =
              DeadlineWriter.write(
                current = keyValue,
                getDeadlineId = RemoveEntryId.KeyFullyCompressed.NoValue,
                plusSize = sizeOf(keyValue.key.size)
              ).addIntUnsigned(keyValue.key.size)

            assert(indexBytes.isFull, s"indexSlice is not full actual: ${indexBytes.written} - expected: ${indexBytes.size}")
            indexBytes

          case (commonBytes, remainingBytes) =>
            val indexBytes =
              DeadlineWriter.write(
                current = keyValue,
                getDeadlineId = RemoveEntryId.KeyPartiallyCompressed.NoValue,
                plusSize = sizeOf(commonBytes) + remainingBytes.size //write the size of keys compressed and also the uncompressed Bytes
              ).addIntUnsigned(commonBytes)
                .addAll(remainingBytes)
            assert(indexBytes.isFull, s"indexSlice is not full actual: ${indexBytes.written} - expected: ${indexBytes.size}")
            indexBytes
        }
    } getOrElse {
      //no common prefixes or no previous write without compression
      val indexBytes =
        DeadlineWriter.write(
          current = keyValue,
          getDeadlineId = RemoveEntryId.KeyUncompressed.NoValue,
          plusSize = keyValue.key.size //write key bytes
        ).addAll(keyValue.key)
      assert(indexBytes.isFull, s"indexSlice is not full actual: ${indexBytes.written} - expected: ${indexBytes.size}")
      indexBytes
    }

}
