/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.slice

/**
  * Slices provides iteration on two or more Slices without requiring to copy.
  */
object Slices {
  def apply[T](left: Slicer[T],
               right: Slicer[T]): Slices[T] =
    new Slices(left, right)
}

class Slices[T](left: Slicer[T],
                right: Slicer[T]) extends Slicer[T] {

  override def isFull: Boolean =
    left.isFull && right.isFull

  override def unslice(): Slicer[T] =
    new Slices(left.unslice(), right.unslice())

  override def iterator: Iterator[T] = new Iterator[T] {
    private val leftIterator = left.iterator
    private val rightIterator = right.iterator

    override def hasNext: Boolean =
      leftIterator.hasNext || rightIterator.hasNext

    override def next(): T =
      if (leftIterator.hasNext)
        leftIterator.next()
      else
        rightIterator.next()
  }
}