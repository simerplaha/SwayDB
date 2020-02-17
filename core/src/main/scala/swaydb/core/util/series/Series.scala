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

package swaydb.core.util.series

import scala.reflect.ClassTag

private[swaydb] trait Series[T] extends Iterable[T] {
  def getOrNull(index: Int): T
  def set(index: Int, item: T): Unit
  def length: Int
  def isVolatile: Boolean
}

private[swaydb] object Series {

  def volatile[T >: Null](limit: Int): Series[T] =
    new SeriesVolatile[T](Array.fill[Item[T]](limit)(new Item[T](null)))

  def basic[T: ClassTag](limit: Int): Series[T] =
    new SeriesBasic[T](new Array[T](limit))

  def navigableVolatile[K, V](maxLevelItems: Int): SeriesNavigable[K, V] =
    new SeriesNavigable[K, V](Series.volatile(maxLevelItems))
}
