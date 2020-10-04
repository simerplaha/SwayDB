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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util

object NullOps {

  def find[A >: Null](array: Array[A], p: A => Boolean): A = {
    var i = 0
    while (i < array.length) {
      val item = array(i)
      if (p(item))
        return item
      else
        i += 1
    }

    null
  }

  @inline final def map[A, T >: Null](value: A, f: A => T): T =
    if (value == null)
      null
    else
      f(value)

  @inline final def foreach[A](value: A, f: A => Unit): Unit =
    if (value != null)
      f(value)

  @inline final def foldLeft[A, T](initial: T)(value: A, f: (T, A) => T): T =
    if (value == null)
      initial
    else
      f(initial, value)

  @inline final def getOrElse[A](value: A, or: => A): A =
    if (value == null)
      or
    else
      value

  @inline final def forall[A](item: A, condition: A => Boolean): Boolean =
    item == null || condition(item)

  @inline final def tryOrNull[T >: Null](f: => T): T =
    try
      f
    catch {
      case _: Exception =>
        null
    }
}
