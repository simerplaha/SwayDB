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

package swaydb.core.util.series

object SeriesVolatile {

  def apply[T >: Null](limit: Int): SeriesVolatile[T] =
    new SeriesVolatile[T](Array.fill[Item[T]](limit)(new Item[T](null)))

}


class SeriesVolatile[T >: Null](array: Array[Item[T]]) extends Series[T] {
  override def getOrNull(index: Int): T =
    array(index).value

  def set(index: Int, item: T): Unit =
    array(index).value = item

  override def length: Int =
    array.length

  def lastOrNull: T =
    array(array.length - 1).value

  def headOrNull: T =
    array(0).value

  override def iterator: Iterator[T] =
    array
      .iterator
      .map(_.value)

  override def isConcurrent: Boolean =
    true
}
