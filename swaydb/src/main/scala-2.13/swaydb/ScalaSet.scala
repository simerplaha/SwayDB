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

import scala.collection.compat.IterableOnce
import scala.collection.mutable

/**
 * Scala collections are blocking and requires an IO Set from SwayDB to build a Set.
 */
private[swaydb] object ScalaSet {

  def apply[A, F](db: Set[A, F, Bag.Less]): mutable.Set[A] =
    new ScalaSetBase[A, F](db) {
      override def addOne(elem: A): this.type = {
        db.add(elem)
        this
      }

      override def subtractOne(elem: A): this.type = {
        db.remove(elem)
        this
      }

      override def subtractAll(xs: IterableOnce[A]): this.type = {
        db.remove(xs.iterator)
        this
      }

      override def addAll(xs: IterableOnce[A]): this.type = {
        db.add(xs.iterator)
        this
      }
    }

  def apply[A, V, F](db: SwayMap[A, V, F, Bag.Less], nullValue: V): mutable.Set[A] =
    new ScalaSetBaseFromMap[A, F](db) {
      override def addOne(elem: A): this.type = {
        db.put(elem, nullValue)
        this
      }

      override def subtractOne(elem: A): this.type = {
        db.remove(elem)
        this
      }

      override def subtractAll(xs: IterableOnce[A]): this.type = {
        db.remove(xs.iterator)
        this
      }

      override def addAll(xs: IterableOnce[A]): this.type = {
        db.put(xs.iterator.map((_, nullValue)))
        this
      }
    }
}
