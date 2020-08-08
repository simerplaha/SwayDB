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

package swaydb.core.util

import swaydb.data.slice.Slice

/**
 * Mutable data type to hold the state of currently being merged key-values and provides functions
 * to mutate it's state.
 *
 * This cannot be immutable as it will add a lot to GC workload.
 *
 * A Segment can easily have over 100,000 key-values to merge and an immutable
 * version of this class would create the same number of of [[DropList]] instances in-memory.
 */
private[core] sealed trait DropList[H >: Null <: T, T >: Null] {

  def headOrNull: T

  def dropHead(): DropList[H, T]

  def dropPrepend(head: H): DropList[H, T]

  def depth: Int

  def size: Int

  def isEmpty: Boolean

  def iterator: Iterator[T]
}

private[core] object DropList {

  @inline final def empty[H >: Null <: T, T >: Null] =
    new Single[H, T](0, null, null, Iterator.empty)

  @inline final def apply[H >: Null <: T, T >: Null](keyValues: Slice[T]): DropList[H, T] =
    new Single[H, T](keyValues.size, null, null, keyValues.iterator)

  @inline final def apply[H >: Null <: T, T >: Null](size: Int, keyValues: Iterator[T]): DropList[H, T] =
    new Single[H, T](size, null, null, keyValues)

  @inline final def single[H >: Null <: T, T >: Null](size: Int, tailHead: T, keyValues: Iterator[T]): DropList.Single[H, T] =
    new Single[H, T](size, null, tailHead, keyValues)

  implicit class DropListImplicit[H >: Null <: T, T >: Null](left: DropList[H, T]) {
    @inline final def append(right: DropList[H, T]): DropList[H, T] =
      if (left.isEmpty)
        right
      else if (right.isEmpty)
        left
      else
        new Multiple(left, right)
  }

  class Single[H >: Null <: T, T >: Null](var size: Int,
                                          private var headRangeOrNull: H,
                                          private var tailHead: T,
                                          private var tailKeyValues: Iterator[T]) extends DropList[H, T] {

    override val depth: Int = 1

    override def headOrNull: T =
      if (headRangeOrNull == null)
        if (tailHead == null) {
          if (tailKeyValues.hasNext)
            tailHead = tailKeyValues.next()
          tailHead
        } else {
          tailHead
        }
      else
        headRangeOrNull

    def dropHead(): DropList.Single[H, T] = {
      if (headRangeOrNull != null) {
        headRangeOrNull = null
        size -= 1
      } else if (tailHead != null) {
        tailHead = null
        size -= 1
      } else if (tailKeyValues.hasNext) {
        tailKeyValues = tailKeyValues.drop(1)
        size -= 1
      }

      this
    }

    def dropPrepend(head: H): DropList.Single[H, T] =
      if (headRangeOrNull != null) {
        headRangeOrNull = head
        this
      } else if (tailHead != null) {
        tailHead = head
        this
      } else {
        tailKeyValues = tailKeyValues.drop(1)
        tailHead = head
        this
      }

    override def isEmpty: Boolean =
      size <= 0

    override def iterator: Iterator[T] =
      new Iterator[T] {
        private var headDone = false
        private var placeHolderDone = false

        override def hasNext: Boolean =
          (!headDone && headRangeOrNull != null) || (!placeHolderDone && tailHead != null) || tailKeyValues.hasNext

        override def next(): T =
          if (!headDone && headRangeOrNull != null) {
            headDone = true
            headRangeOrNull
          } else if (!placeHolderDone && tailHead != null) {
            placeHolderDone = true
            tailHead
          } else {
            tailKeyValues.next()
          }
      }
  }

  private[core] class Multiple[H >: Null <: T, T >: Null](private var left: DropList[H, T],
                                                          right: DropList[H, T]) extends DropList[H, T] {

    override def dropHead(): DropList[H, T] =
      (left.isEmpty, right.isEmpty) match {
        case (true, true) =>
          DropList.empty
        case (true, false) =>
          right.dropHead()
        case (false, true) =>
          left.dropHead()
        case (false, false) =>
          left = left.dropHead()
          this
      }

    override def dropPrepend(head: H): DropList[H, T] =
      (left.isEmpty, right.isEmpty) match {
        case (true, true) =>
          new Single[H, T](1, head, null, Iterator.empty)
        case (true, false) =>
          right.dropPrepend(head)
        case (false, true) =>
          left.dropPrepend(head)
        case (false, false) =>
          left = left.dropPrepend(head)
          this
      }

    override def headOrNull: T =
      if (left.headOrNull == null)
        right.headOrNull
      else
        left.headOrNull

    override def iterator: Iterator[T] =
      new Iterator[T] {
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

    override def isEmpty: Boolean =
      size <= 0

    override def depth: Int =
      left.depth + right.depth

    override def size =
      left.size + right.size
  }
}
