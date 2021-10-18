/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.util.series

object SeriesVolatile {

  def apply[T >: Null](limit: Int): SeriesVolatile[T] =
    new SeriesVolatile[T](Array.fill[VolatileValue[T]](limit)(new VolatileValue[T](null)))

}

class SeriesVolatile[T >: Null](array: Array[VolatileValue[T]]) extends Series[T] {
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
