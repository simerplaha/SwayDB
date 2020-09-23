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

package swaydb.core.map.applied

import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}


object AppliedFunctionsCache {
  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]]) =
    new MapCacheBuilder[AppliedFunctionsCache] {
      override def create(): AppliedFunctionsCache =
        AppliedFunctionsCache(SkipList.concurrent(Slice.Null, Slice.Null))
    }
}

case class AppliedFunctionsCache(skipList: SkipListConcurrent[SliceOption[Byte], Slice.Null.type, Slice[Byte], Slice.Null.type]) extends MapCache[Slice[Byte], Slice.Null.type] {
  override def write(entry: MapEntry[Slice[Byte], Slice.Null.type]): Unit =
    entry applyTo skipList

  override def asScala: Iterable[(Slice[Byte], Slice.Null.type)] =
    skipList.asScala

  override def isEmpty: Boolean =
    skipList.isEmpty

  override def size: Int =
    skipList.size
}
