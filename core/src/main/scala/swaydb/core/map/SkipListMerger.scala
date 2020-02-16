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
 */

package swaydb.core.map

import swaydb.core.function.FunctionStore
import swaydb.core.util.skiplist.ConcurrentSkipList
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for SkipListMerge of type [${K}, ${V}]")
trait SkipListMerger[OK, OV, K <: OK, V <: OV] {

  def insert(insertKey: K,
             insertValue: V,
             skipList: ConcurrentSkipList[OK, OV, K, V])(implicit keyOrder: KeyOrder[K],
                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                         functionStore: FunctionStore): Unit

  def insert(entry: MapEntry[K, V],
             skipList: ConcurrentSkipList[OK, OV, K, V])(implicit keyOrder: KeyOrder[K],
                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                         functionStore: FunctionStore): Unit
}
