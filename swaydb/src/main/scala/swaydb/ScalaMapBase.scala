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

package swaydb

import scala.collection.mutable

protected abstract class ScalaMapBase[K, V](db: SetMapT[K, V, Glass]) extends mutable.Map[K, V] {

  override def get(key: K): Option[V] =
    db.get(key)

  override def iterator: Iterator[(K, V)] =
    db.iterator(Bag.glass)

  override def isEmpty: Boolean =
    db.isEmpty

  override def headOption: Option[(K, V)] =
    db.head

  override def lastOption: Option[(K, V)] =
    db.last

  override def keySet: mutable.Set[K] =
    db.keySet

  override def contains(key: K): Boolean =
    db.contains(key)

  override def last: (K, V) =
    db.last.get

  override def head: (K, V) =
    db.head.get

  override def clear(): Unit =
    db.clearKeyValues()

}
