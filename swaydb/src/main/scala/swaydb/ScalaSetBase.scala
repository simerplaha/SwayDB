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

protected abstract class ScalaSetBase[A, F](db: Set[A, F, Bag.Less]) extends mutable.Set[A] {

  override def contains(elem: A): Boolean =
    db.contains(elem)

  override def iterator: Iterator[A] =
    db.iterator(Bag.bagless)

  override def isEmpty: Boolean =
    db.isEmpty

  override def headOption: Option[A] =
    db.headOption

  override def lastOption: Option[A] =
    db.lastOption

  override def last: A =
    db.lastOption.get

  override def head: A =
    db.headOption.get

  override def clear(): Unit =
    db.clear()

}
