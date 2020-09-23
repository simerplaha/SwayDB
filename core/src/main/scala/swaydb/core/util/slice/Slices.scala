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

package swaydb.core.util.slice

import java.util

import swaydb.data.slice.Slice

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object Slices {

  @inline def apply[T: ClassTag](lengthPerSlice: Int): Slices[T] = {
    val sizePerSlice = lengthPerSlice max 1 //size cannot be 0
    val list = new util.ArrayList[Slice[T]](1)
    list add Slice.of[T](sizePerSlice)

    new Slices(
      list = list,
      lengthPerSlice = sizePerSlice
    )
  }

}

class Slices[T: ClassTag](list: util.ArrayList[Slice[T]], lengthPerSlice: Int) extends Iterable[T] {

  private var _size = 0

  def get(index: Int): T =
    if (index < lengthPerSlice) {
      val array = list.get(0)
      array(index)
    } else {
      val listIndex = index / lengthPerSlice
      val indexInArray = index - (lengthPerSlice * listIndex)

      val array = list.get(listIndex)
      array(indexInArray)
    }

  def add(item: T): Unit = {
    val last = list.get(list.size() - 1)
    if (last.isFull)
      list.add(Slice.of[T](lengthPerSlice).add(item))
    else
      last.add(item)

    _size += 1
  }

  override def size: Int =
    _size

  override def iterator: Iterator[T] =
    new Iterator[T] {
      val iterators =
        list
          .iterator()
          .asScala
          .flatMap(_.iterator)

      override def hasNext: Boolean = iterators.hasNext
      override def next(): T = iterators.next()
    }
}
