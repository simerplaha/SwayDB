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

import java.util.concurrent.atomic.AtomicReferenceArray

object SeriesAtomic {

  def apply[T](limit: Int): SeriesAtomic[T] =
    new SeriesAtomic[T](new AtomicReferenceArray[T](limit))

}

class SeriesAtomic[T](array: AtomicReferenceArray[T]) extends Series[T] {
  override def getOrNull(index: Int): T =
    array.get(index)

  def set(index: Int, item: T): Unit =
    array.set(index, item)

  override def length: Int =
    array.length()

  override def iterator: Iterator[T] =
    new Iterator[T] {
      val innerIterator = (0 until array.length()).iterator

      override def hasNext: Boolean =
        innerIterator.hasNext

      override def next(): T =
        array.get(innerIterator.next())
    }

  override def isConcurrent: Boolean =
    true
}
