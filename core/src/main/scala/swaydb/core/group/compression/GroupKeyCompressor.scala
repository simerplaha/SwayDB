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

package swaydb.core.group.compression

import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.core.data.Transient
import swaydb.core.util.Bytes
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

private[core] object GroupKeyCompressor {

  /**
   * @return (minKey, maxKey, fullKey)
   */
  def compress(head: Option[Transient],
               last: Transient): (Slice[Byte], MaxKey[Slice[Byte]], Slice[Byte]) =
    (head, last) match {
      case (Some(keyValue), fixed: Transient.Fixed) =>
        val fullKey = Bytes.compressJoin(keyValue.key, fixed.key, 0.toByte)
        (keyValue.key, MaxKey.Fixed(fixed.key), fullKey)

      case (Some(keyValue), range: Transient.Range) =>
        val maxKey = Bytes.compressJoin(range.fromKey, range.toKey)
        val fullKey = Bytes.compressJoin(keyValue.key, maxKey, 1.toByte)
        (keyValue.key, MaxKey.Range(range.fromKey, range.toKey), fullKey)

      case (Some(keyValue), group: Transient.Group) =>
        group.maxKey match {
          case fixed @ MaxKey.Fixed(maxKey) =>
            val fullKey = Bytes.compressJoin(keyValue.key, maxKey, 0.toByte)
            (keyValue.key, fixed, fullKey)

          case maxKeyRange @ MaxKey.Range(fromKey, maxKey) =>
            val maxKeyCompressed = Bytes.compressJoin(fromKey, maxKey)
            val fullKey = Bytes.compressJoin(keyValue.key, maxKeyCompressed, 1.toByte)
            (keyValue.key, maxKeyRange, fullKey)
        }

      case (None, fixed: Transient.Fixed) =>
        (fixed.key, MaxKey.Fixed(fixed.key), fixed.key append 2.toByte)

      case (None, range: Transient.Range) =>
        val mergedKey = Bytes.compressJoin(range.fromKey, range.toKey, 3.toByte)
        (range.fromKey, MaxKey.Range(range.fromKey, range.toKey), mergedKey)

      case (None, group: Transient.Group) =>
        (group.minKey, group.maxKey, group.mergedKey)
    }

  def decompress(key: Slice[Byte]): IO[swaydb.Error.Segment, (Slice[Byte], MaxKey[Slice[Byte]])] =
    key.lastOption map {
      case 0 =>
        Bytes.decompressJoin(key.dropRight(1)) map {
          case (minKey, maxKey) =>
            (minKey, MaxKey.Fixed(maxKey))
        }

      case 1 =>
        Bytes.decompressJoin(key.dropRight(1)) flatMap {
          case (minKey, rangeMaxKey) =>
            Bytes.decompressJoin(rangeMaxKey) map {
              case (fromKey, toKey) =>
                (minKey, MaxKey.Range(fromKey, toKey))
            }
        }

      case 2 =>
        val keyWithoutId = key.dropRight(1)
        IO.Right(keyWithoutId, MaxKey.Fixed(keyWithoutId))

      case 3 =>
        Bytes.decompressJoin(key.dropRight(1)) map {
          case (minKey, maxKey) =>
            (minKey, MaxKey.Range(minKey, maxKey))
        }
    } getOrElse {
      IO.failed(GroupCompressorFailure.GroupKeyIsEmpty)
    }
}
