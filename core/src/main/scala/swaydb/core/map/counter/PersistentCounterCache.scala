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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.map.counter

import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.data.slice.Slice

object PersistentCounterCache {
  implicit def builder =
    new MapCacheBuilder[PersistentCounterCache] {
      override def create(): PersistentCounterCache =
        new PersistentCounterCache
    }
}

class PersistentCounterCache extends MapCache[Slice[Byte], Slice[Byte]] {
  var entryOrNull: MapEntry[Slice[Byte], Slice[Byte]] = _

  override def write(entry: MapEntry[Slice[Byte], Slice[Byte]]): Unit =
    this.entryOrNull = entry

  override def asScala: Iterable[(Slice[Byte], Slice[Byte])] =
    entryOrNull match {
      case null =>
        Seq.empty

      case MapEntry.Put(key, value) =>
        Seq((key, value))

      case entry: MapEntry[Slice[Byte], Slice[Byte]] =>
        entry.entries map {
          case MapEntry.Put(key, value) =>
            (key, value)
        }
    }

  override def isEmpty: Boolean =
    entryOrNull == null

  override def size: Int =
    if (entryOrNull == null)
      0
    else
      1
}
