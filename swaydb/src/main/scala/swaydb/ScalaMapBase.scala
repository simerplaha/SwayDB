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

package swaydb

import scala.collection.mutable

protected abstract class ScalaMapBase[K, V, F](db: SwayMap[K, V, F, Bag.Less]) extends mutable.Map[K, V] {

  override def get(key: K): Option[V] =
    db.get(key)

  override def iterator: Iterator[(K, V)] =
    db.iterator(Bag.less)

  override def isEmpty: Boolean =
    db.isEmpty

  override def headOption: Option[(K, V)] =
    db.headOption

  override def lastOption: Option[(K, V)] =
    db.lastOption

  override def keySet: mutable.Set[K] =
    db.keySet

  override def contains(key: K): Boolean =
    db.contains(key)

  override def last: (K, V) =
    db.lastOption.get

  override def head: (K, V) =
    db.headOption.get

  override def clear(): Unit =
    db.clear()

}
