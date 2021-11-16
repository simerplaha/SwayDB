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

package swaydb.core.util

import swaydb.core.data.{Memory, MemoryOption}
import swaydb.slice.MaxKey
import swaydb.slice.Slice


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
          throw new Exception(s"Invalid byte: $byte")
        }

      case None =>
        throw new Exception("Key is empty")
    }
}
