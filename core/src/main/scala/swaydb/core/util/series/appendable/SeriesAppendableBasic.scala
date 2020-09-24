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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.util.series.appendable

import scala.reflect.ClassTag

private[series] object SeriesAppendableBasic {

  def apply[T >: Null : ClassTag](limit: Int): SeriesAppendableBasic[T] =
    new SeriesAppendableBasic[T](Array.fill[T](limit)(null))

}

private[series] class SeriesAppendableBasic[T >: Null] private(array: Array[T]) extends SeriesAppendable[T] { self =>
  //Not volatile because series do not allow concurrent writes only concurrent reads.
  private var writePosition = 0

  def get(index: Int): T =
    if (index >= writePosition)
      throw new ArrayIndexOutOfBoundsException(index)
    else
      array(index)

  def add(item: T): Unit = {
    array(writePosition) = item
    writePosition += 1
  }

  def length: Int =
    writePosition

  def innerArrayLength =
    array.length

  def lastOrNull: T =
    if (writePosition == 0)
      null
    else
      array(writePosition - 1)

  def headOrNull: T =
    if (writePosition == 0)
      null
    else
      array(0)

  def isFull =
    array.length == writePosition

  def iterator: Iterator[T] =
    array
      .iterator
      .take(writePosition)
}
