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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb

import scala.collection.mutable

protected abstract class ScalaSetBase[A, F](db: Set[A, F, Bag.Less]) extends mutable.Set[A] {

  override def contains(elem: A): Boolean =
    db.contains(elem)

  override def iterator: Iterator[A] =
    db.iterator(Bag.less)

  override def isEmpty: Boolean =
    db.isEmpty

  override def headOption: Option[A] =
    db.head

  override def lastOption: Option[A] =
    db.last

  override def last: A =
    db.last.get

  override def head: A =
    db.head.get

  override def clear(): Unit =
    db.clear()

}

protected abstract class ScalaSetBaseFromMap[A](db: SetMapT[A, _, Bag.Less]) extends mutable.Set[A] {

  override def contains(elem: A): Boolean =
    db.contains(elem)

  override def iterator: Iterator[A] =
    db.iterator(Bag.less).map(_._1)

  override def isEmpty: Boolean =
    db.isEmpty

  override def headOption: Option[A] =
    db.head.map(_._1)

  override def lastOption: Option[A] =
    db.last.map(_._1)

  override def last: A =
    lastOption.get

  override def head: A =
    headOption.get

  override def clear(): Unit =
    db.clearKeyValues()

}
