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

package swaydb.core.util

object NullOps {

  implicit class ArrayImplicits[A](array: Array[A]) {
    final def findNullable(p: A => Boolean): A = {
      var i = 0
      while (i < array.length - 1) {
        val item = array(i)
        if (p(item))
          return item
        else
          i += 1
      }

      null.asInstanceOf[A]
    }
  }

  implicit class NullImplicits[A](value: A) {
    def isNull: Boolean =
      value == null

    def isNotNull: Boolean =
      value != null

    def mapNotNull[T >: Null](f: A => T): T =
      if (value == null)
        null
      else
        f(value)
  }

}
