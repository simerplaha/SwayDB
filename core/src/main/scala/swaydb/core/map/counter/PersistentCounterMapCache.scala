/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.map.counter

import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.data.slice.Slice

object PersistentCounterMapCache {
  implicit def builder =
    new MapCacheBuilder[PersistentCounterMapCache] {
      override def create(): PersistentCounterMapCache =
        new PersistentCounterMapCache
    }
}

class PersistentCounterMapCache extends MapCache[Slice[Byte], Slice[Byte]] {
  var entryOrNull: MapEntry[Slice[Byte], Slice[Byte]] = _

  override def writeAtomic(entry: MapEntry[Slice[Byte], Slice[Byte]]): Unit =
    this.entryOrNull = entry

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Slice[Byte]]): Unit =
    writeAtomic(entry)

  override def iterator: Iterator[(Slice[Byte], Slice[Byte])] =
    entryOrNull match {
      case null =>
        Iterator.empty

      case MapEntry.Put(key, value) =>
        Iterator((key, value))

      case entry: MapEntry[Slice[Byte], Slice[Byte]] =>
        entry.entries.iterator map {
          case MapEntry.Put(key, value) =>
            (key, value)
        }
    }

  override def isEmpty: Boolean =
    entryOrNull == null

  override def maxKeyValueCount: Int =
    if (entryOrNull == null)
      0
    else
      1

}
