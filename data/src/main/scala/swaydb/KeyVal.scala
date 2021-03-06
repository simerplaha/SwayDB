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

object KeyVal {
  def apply[K, V](keyVal: (K, V)): KeyVal[K, V] =
    new KeyVal(keyVal._1, keyVal._2)

  def of[K, V](key: K, value: V): KeyVal[K, V] =
    new KeyVal(key, value)

  def of[T](keyAndVal: T): KeyVal[T, T] =
    new KeyVal(keyAndVal, keyAndVal)

  implicit class TupleImplicits[K, V](tuple: (K, V)) {
    @inline final def asKeyVal: KeyVal[K, V] =
      KeyVal(tuple._1, tuple._2)

    @inline final def asPair: Pair[K, V] =
      Pair(tuple._1, tuple._2)
  }
}

case class KeyVal[+K, +V](key: K, value: V) extends Pair(key, value) {
  override def equals(other: Any): Boolean =
    other match {
      case other: KeyVal[K, V] =>
        left == other.left && right == other.right

      case _ =>
        false
    }

  override def toString: String =
    s"KeyVal(key = $key, value = $value)"
}
