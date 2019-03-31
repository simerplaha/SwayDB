/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha) 
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

package swaydb

import scala.collection.{TraversableOnce, mutable}
import swaydb.data.IO

/**
  * Scala collections are blocking and requires an IO Set from SwayDB to build a Set.
  */
private[swaydb] object ScalaSet {

  def apply[A](db: Set[A, IO]): mutable.Set[A] =
    new mutable.Set[A] {

      override def contains(elem: A): Boolean =
        db.contains(elem).get

      override def iterator: Iterator[A] =
        new Iterator[A] {
          var nextOne: A = _

          override def hasNext: Boolean =
            if (nextOne == null)
              db.headOption.get exists {
                some =>
                  nextOne = some
                  true
              }
            else
              db.next(nextOne).get exists {
                some =>
                  nextOne = some
                  true
              }

          override def next(): A =
            nextOne
        }

      override def isEmpty: Boolean =
        db.isEmpty.get

      override def nonEmpty: Boolean =
        !isEmpty

      override def headOption: Option[A] =
        db.headOption.get

      override def lastOption: Option[A] =
        db.lastOption.get

      override def +=(elem: A): this.type = {
        db.add(elem).get
        this
      }

      override def -=(elem: A): this.type = {
        db.remove(elem).get
        this
      }

      override def --=(xs: TraversableOnce[A]): this.type = {
        db.remove(xs.toIterable).get
        this
      }

      override def ++=(xs: TraversableOnce[A]): this.type = {
        db.add(xs.toIterable).get
        this
      }

      override def clear(): Unit =
        db.clear().get
    }
}
