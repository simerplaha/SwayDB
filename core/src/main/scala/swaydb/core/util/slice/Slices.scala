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

import swaydb.data.slice.Slice

import scala.reflect.ClassTag

object Slices {

  @inline def apply[T: ClassTag](lengthPerSlice: Int): Slices[T] = {
    val sizePerSlice = lengthPerSlice max 1 //size cannot be 0
    val list = Slice.of[Slice[T]](2)
    list add Slice.of[T](sizePerSlice)

    new Slices(
      slices = list,
      lengthPerSlice = sizePerSlice
    )
  }

  def empty[T: ClassTag]: Slices[T] =
    apply[T](0)

}

class Slices[T: ClassTag](@volatile private var slices: Slice[Slice[T]], lengthPerSlice: Int) {

  private var _size = 0

  def get(index: Int): T =
    if (index < lengthPerSlice) {
      val array = slices.get(0)
      array(index)
    } else {
      val listIndex = index / lengthPerSlice
      val indexInArray = index - (lengthPerSlice * listIndex)

      val array = slices.get(listIndex)
      array(indexInArray)
    }

  def add(item: T): Unit = {
    val last = slices.get(slices.size - 1)
    if (last.isFull) {
      val newSlice = Slice.of[T](lengthPerSlice).add(item)

      if (slices.isFull) {
        val newSlices = Slice.of[Slice[T]](slices.size + 1)
        slices.foreach(newSlices.add)
        newSlices add newSlice
        slices = newSlices
      } else {
        slices.add(newSlice)
      }
    } else {
      last.add(item)
    }

    _size += 1
  }

  def lastOrNull: T =
    slices.get(slices.size - 1).lastOrNull

  def headOrNull: T =
    slices.get(0).headOrNull

  def size: Int =
    _size

  def findReverse[R >: T](from: Int, result: R)(f: T => Boolean): R = {
    var start = from

    while (start >= 0) {
      val next = get(start)
      if (f(next))
        return next

      start -= 1
    }

    result
  }

  def find[R >: T](from: Int, result: R)(f: T => Boolean): R = {
    var start = from

    while (start < _size) {
      val next = get(start)
      if (f(next))
        return next

      start += 1
    }

    result
  }

  def foreach(from: Int)(f: T => Unit): Unit = {
    var start = from

    while (start < _size) {
      f(get(start))
      start += 1
    }
  }

  def iteratorFlatten: Iterator[T] =
    new Iterator[T] {
      val iterators =
        slices
          .iterator
          .flatMap(_.iterator)

      override def hasNext: Boolean = iterators.hasNext
      override def next(): T = iterators.next()
    }
}
