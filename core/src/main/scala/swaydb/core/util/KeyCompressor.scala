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

package swaydb.core.util

import swaydb.IO
import swaydb.core.data.{Memory, MemoryOption}
import swaydb.data.MaxKey
import swaydb.data.slice.Slice


private[core] object KeyCompressor {

  /**
   * @return (minKey, maxKey, fullKey)
   */
  def compress(head: MemoryOption,
               last: Memory): (Slice[Byte], MaxKey[Slice[Byte]], Slice[Byte]) =
    (head, last) match {
      case (keyValue: Memory, fixed: Memory.Fixed) =>
        val fullKey = Bytes.compressJoin(keyValue.key, fixed.key, 0.toByte)
        (keyValue.key, MaxKey.Fixed(fixed.key), fullKey)

      case (keyValue: Memory, range: Memory.Range) =>
        val maxKey = Bytes.compressJoin(range.fromKey, range.toKey)
        val fullKey = Bytes.compressJoin(keyValue.key, maxKey, 1.toByte)
        (keyValue.key, MaxKey.Range(range.fromKey, range.toKey), fullKey)

      case (Memory.Null, fixed: Memory.Fixed) =>
        (fixed.key, MaxKey.Fixed(fixed.key), fixed.key append 2.toByte)

      case (Memory.Null, range: Memory.Range) =>
        val mergedKey = Bytes.compressJoin(range.fromKey, range.toKey, 3.toByte)
        (range.fromKey, MaxKey.Range(range.fromKey, range.toKey), mergedKey)
    }

  def decompress(key: Slice[Byte]): (Slice[Byte], MaxKey[Slice[Byte]]) =
    key.lastOption match {
      case Some(byte) =>
        if (byte == 0) {
          val (minKey, maxKey) = Bytes.decompressJoin(key.dropRight(1))
          (minKey, MaxKey.Fixed(maxKey))
        } else if (byte == 1) {
          val (minKey, rangeMaxKey) = Bytes.decompressJoin(key.dropRight(1))
          val (fromKey, toKey) = Bytes.decompressJoin(rangeMaxKey)
          (minKey, MaxKey.Range(fromKey, toKey))
        } else if (byte == 2) {
          val keyWithoutId = key.dropRight(1)
          (keyWithoutId, MaxKey.Fixed(keyWithoutId))
        } else if (byte == 3) {
          val (minKey, maxKey) = Bytes.decompressJoin(key.dropRight(1))
          (minKey, MaxKey.Range(minKey, maxKey))
        } else {
          throw IO.throwable(s"Invalid byte: $byte")
        }

      case None =>
        throw IO.throwable("Key is empty")
    }
}
