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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
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
 * version of this class would create the same number of of [[DropIterator]] instances in-memory.
 */
private[core] sealed trait DropIterator[H >: Null <: T, T >: Null] {

  def headOrNull: T

  def dropHead(): DropIterator[H, T]

  def dropPrepend(head: H): DropIterator[H, T]

  def depth: Int

  def size: Int

  def isEmpty: Boolean

  def iterator: Iterator[T]
}

private[core] object DropIterator {

  @inline final def empty[H >: Null <: T, T >: Null] =
    new Single[H, T](0, null, null, Iterator.empty)

  @inline final def apply[H >: Null <: T, T >: Null](keyValues: Slice[T]): DropIterator.Single[H, T] =
    new Single[H, T](keyValues.size, null, null, keyValues.iterator)

  @inline final def apply[H >: Null <: T, T >: Null](size: Int, keyValues: Iterator[T]): DropIterator.Single[H, T] =
    new Single[H, T](size, null, null, keyValues)

  implicit class DropIteratorImplicit[H >: Null <: T, T >: Null](left: DropIterator[H, T]) {
    @inline final def append(right: DropIterator[H, T]): DropIterator[H, T] =
      if (left.isEmpty)
        right
      else if (right.isEmpty)
        left
      else
        new Multiple(left, right)
  }

  class Single[H >: Null <: T, T >: Null] private[DropIterator](var size: Int,
                                                                private var headRangeOrNull: H,
                                                                private var tailHead: T,
                                                                private var tailKeyValues: Iterator[T]) extends DropIterator[H, T] {

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

    def dropHead(): DropIterator.Single[H, T] = {
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

    def dropHeadDuplicate(): DropIterator.Single[H, T] = {
      this.headOrNull //ensure that head is fetched.

      val (left, right) = tailKeyValues.duplicate
      this.tailKeyValues = left

      val duplicated = DropIterator[H, T](size = size - 1, keyValues = right)

      if (this.headRangeOrNull != null)
        duplicated.tailHead = this.tailHead

      duplicated
    }

    def duplicate(): (Single[H, T], Single[H, T]) = {
      val (left, right) = tailKeyValues.duplicate

      val leftIterator = DropIterator[H, T](size = size, keyValues = left)
      leftIterator.headRangeOrNull = this.headRangeOrNull
      leftIterator.tailHead = this.tailHead

      val rightIterator = DropIterator[H, T](size = size, keyValues = right)
      rightIterator.headRangeOrNull = this.headRangeOrNull
      rightIterator.tailHead = this.tailHead

      (leftIterator, rightIterator)
    }

    def dropPrepend(head: H): DropIterator.Single[H, T] =
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

  private[core] class Multiple[H >: Null <: T, T >: Null] private[DropIterator](private var left: DropIterator[H, T],
                                                                                right: DropIterator[H, T]) extends DropIterator[H, T] {

    override def dropHead(): DropIterator[H, T] =
      (left.isEmpty, right.isEmpty) match {
        case (true, true) =>
          DropIterator.empty
        case (true, false) =>
          right.dropHead()
        case (false, true) =>
          left.dropHead()
        case (false, false) =>
          left = left.dropHead()
          this
      }

    override def dropPrepend(head: H): DropIterator[H, T] =
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
