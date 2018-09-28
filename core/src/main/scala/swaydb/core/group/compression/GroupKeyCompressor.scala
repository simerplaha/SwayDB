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

package swaydb.core.group.compression

import swaydb.core.data.KeyValue
import swaydb.core.util.Bytes
import swaydb.data.segment.MaxKey
import swaydb.data.segment.MaxKey.{Fixed, Range}
import swaydb.data.slice.Slice

import scala.util.{Failure, Success, Try}

private[core] object GroupKeyCompressor {

  /**
    * @return (minKey, maxKey, fullKey)
    */
  def compress(head: Option[KeyValue.WriteOnly],
               last: KeyValue.WriteOnly): (Slice[Byte], MaxKey, Slice[Byte]) =
    (head, last) match {
      case (Some(keyValue), fixed: KeyValue.WriteOnly.Fixed) =>
        val fullKey = Bytes.compressJoin(keyValue.key, fixed.key, 0.toByte)
        (keyValue.key, MaxKey.Fixed(fixed.key), fullKey)

      case (Some(keyValue), range: KeyValue.WriteOnly.Range) =>
        val maxKey = Bytes.compressJoin(range.fromKey, range.toKey)
        val fullKey = Bytes.compressJoin(keyValue.key, maxKey, 1.toByte)
        (keyValue.key, MaxKey.Range(range.fromKey, range.toKey), fullKey)

      case (Some(keyValue), group: KeyValue.WriteOnly.Group) =>
        group.maxKey match {
          case fixed @ Fixed(maxKey) =>
            val fullKey = Bytes.compressJoin(keyValue.key, maxKey, 0.toByte)
            (keyValue.key, fixed, fullKey)

          case maxKeyRange @ Range(fromKey, maxKey) =>
            val maxKeyCompressed = Bytes.compressJoin(fromKey, maxKey)
            val fullKey = Bytes.compressJoin(keyValue.key, maxKeyCompressed, 1.toByte)
            (keyValue.key, maxKeyRange, fullKey)
        }

      case (None, fixed: KeyValue.WriteOnly.Fixed) =>
        (fixed.key, MaxKey.Fixed(fixed.key), fixed.key append 2.toByte)

      case (None, range: KeyValue.WriteOnly.Range) =>
        val fullKey = Bytes.compressJoin(range.fromKey, range.toKey, 3.toByte)
        (range.fromKey, MaxKey.Range(range.fromKey, range.toKey), fullKey)

      case (None, group: KeyValue.WriteOnly.Group) =>
        (group.minKey, group.maxKey, group.fullKey)
    }

  def decompress(key: Slice[Byte]): Try[(Slice[Byte], MaxKey)] =
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
        Success(keyWithoutId, MaxKey.Fixed(keyWithoutId))

      case 3 =>
        Bytes.decompressJoin(key.dropRight(1)) map {
          case (minKey, maxKey) =>
            (minKey, MaxKey.Range(minKey, maxKey))
        }

    } getOrElse {
      Failure(GroupCompressorFailure.GroupKeyIsEmpty)
    }

}
