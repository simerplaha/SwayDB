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

package swaydb.series.appendable

import swaydb.series.VolatileValue

private[series] object SeriesAppendableVolatile {

  def apply[T >: Null](limit: Int): SeriesAppendableVolatile[T] = {
    val nullFill = Array.fill[VolatileValue[T]](limit)(new VolatileValue[T](null))
    new SeriesAppendableVolatile[T](nullFill)
  }

}

private[series] class SeriesAppendableVolatile[T >: Null] private(array: Array[VolatileValue[T]]) extends SeriesAppendable[T] { self =>
  //Not volatile because series do not allow concurrent writes only concurrent reads.
  @volatile private var writePosition = 0

  def get(index: Int): T =
    if (index >= writePosition)
      throw new ArrayIndexOutOfBoundsException(index)
    else
      array(index).value

  def add(item: T): Unit = {
    array(writePosition).value = item
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
      array(writePosition - 1).value

  def headOrNull: T =
    if (writePosition == 0)
      null
    else
      array(0).value

  def isFull =
    array.length == writePosition

  def iterator: Iterator[T] =
    iterator(0)

  override def iterator(from: Int): Iterator[T] =
    new Iterator[T] {

      var position: Int = from

      override def hasNext: Boolean =
        position < writePosition

      override def next(): T = {
        val item = self.array(position).value
        position += 1
        item
      }
    }
}
