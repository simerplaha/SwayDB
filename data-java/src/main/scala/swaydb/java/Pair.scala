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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.java

import scala.compat.java8.DurationConverters._

object Pair {

  implicit class PairImplicit[K](keyVal: Pair[K, java.time.Duration]) {
    def toScala: (K, scala.concurrent.duration.Deadline) =
      (keyVal.left, keyVal.right.toScala.fromNow)
  }

  implicit class TupleImplicits[L, R](tuple: (L, R)) {
    def toPair: Pair[L, R] =
      Pair(tuple._1, tuple._2)
  }

  def apply[L, R](left: L, right: R): Pair[L, R] =
    new Pair(left, right)

  def create[L, R](left: L, right: R): Pair[L, R] =
    new Pair(left, right)

  def create[I](leftAndRight: I): Pair[I, I] =
    new Pair(leftAndRight, leftAndRight)
}

class Pair[+L, +R](val left: L, val right: R) {
  def toTuple: (L, R) =
    (left, right)

  def toKeyVal =
    KeyVal(left, right)

  override def equals(obj: Any): Boolean =
    toTuple.equals(obj)

  override def hashCode(): Int =
    toTuple.hashCode()
}
