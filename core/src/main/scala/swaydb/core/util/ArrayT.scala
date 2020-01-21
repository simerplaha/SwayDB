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

package swaydb.core.util

import java.util.concurrent.atomic.AtomicReferenceArray

import scala.reflect.ClassTag

sealed trait ArrayT[T] extends Iterable[T] {
  def getOrNull(index: Int): T
  def set(index: Int, item: T): Unit
  def length: Int
}

object ArrayT {

  def atomic[T](limit: Int): ArrayT[T] =
    new Atomic[T](new AtomicReferenceArray[T](limit))

  def basic[T: ClassTag](limit: Int): ArrayT[T] =
    new Basic[T](new Array[T](limit))

  class Atomic[T](array: AtomicReferenceArray[T]) extends ArrayT[T] {
    override def getOrNull(index: Int): T =
      array.get(index)

    override def set(index: Int, item: T): Unit =
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
  }

  class Basic[T](array: Array[T]) extends ArrayT[T] {
    override def getOrNull(index: Int): T =
      array(index)

    override def set(index: Int, item: T): Unit =
      array(index) = item

    override def length: Int =
      array.length

    override def iterator: Iterator[T] =
      array.iterator
  }
}
