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

package swaydb.core.util

import scala.collection.compat._

/**
 * Mutable data type to hold the state of currently being merged key-values and provides functions
 * to mutate it's state.
 *
 * This cannot be immutable as it will add a lot to GC workload.
 *
 * A Segment can easily have over 100,000 key-values to merge and an immutable
 * version of this class would create the same number of [[DropIterator]] instances in-memory.
 */
private[core] sealed trait DropIterator[H >: Null <: T, T >: Null] {

  def headOrNull: T

  def dropHead(): DropIterator[H, T]

  def dropPrepend(head: H): DropIterator[H, T]

  def depth: Int

  def isEmpty: Boolean

  def iterator: Iterator[T]
}

private[core] object DropIterator {

  @inline final def empty[H >: Null <: T, T >: Null] =
    new Flat[H, T](null, null, Iterator.empty)

  @inline final def apply[H >: Null <: T, T >: Null](keyValues: IterableOnce[T]): DropIterator.Flat[H, T] =
    new Flat[H, T](null, null, keyValues.iterator)

  implicit class DropIteratorImplicit[H >: Null <: T, T >: Null](left: DropIterator[H, T]) {
    @inline final def append(right: DropIterator[H, T]): DropIterator[H, T] =
      if (left.isEmpty)
        right
      else if (right.isEmpty)
        left
      else
        new Nest(left, right)
  }

  class Flat[H >: Null <: T, T >: Null] private[DropIterator](private var headRangeOrNull: H,
                                                              private var tailHeadOrNull: T,
                                                              private var tailKeyValues: Iterator[T]) extends DropIterator[H, T] {

    override val depth: Int = 1

    override def headOrNull: T =
      if (headRangeOrNull == null)
        if (tailHeadOrNull == null) {
          if (tailKeyValues.hasNext)
            tailHeadOrNull = tailKeyValues.next()
          tailHeadOrNull
        } else {
          tailHeadOrNull
        }
      else
        headRangeOrNull

    def dropHead(): DropIterator.Flat[H, T] = {
      if (headRangeOrNull != null)
        headRangeOrNull = null
      else if (tailHeadOrNull != null)
        tailHeadOrNull = null
      else if (tailKeyValues.hasNext)
        tailKeyValues = tailKeyValues.drop(1)

      this
    }

    def dropHeadDuplicate(): DropIterator.Flat[H, T] = {
      this.headOrNull //ensure that head is fetched.

      val (left, right) = tailKeyValues.duplicate
      this.tailKeyValues = left

      val duplicated = DropIterator[H, T](keyValues = right)

      if (this.headRangeOrNull != null)
        duplicated.tailHeadOrNull = this.tailHeadOrNull

      duplicated
    }

    def duplicate(): (Flat[H, T], Flat[H, T]) = {
      val (left, right) = tailKeyValues.duplicate

      val leftIterator = DropIterator[H, T](keyValues = left)
      leftIterator.headRangeOrNull = this.headRangeOrNull
      leftIterator.tailHeadOrNull = this.tailHeadOrNull

      val rightIterator = DropIterator[H, T](keyValues = right)
      rightIterator.headRangeOrNull = this.headRangeOrNull
      rightIterator.tailHeadOrNull = this.tailHeadOrNull

      (leftIterator, rightIterator)
    }

    def dropPrepend(head: H): DropIterator.Flat[H, T] =
      if (headRangeOrNull != null) {
        headRangeOrNull = head
        this
      } else if (tailHeadOrNull != null) {
        tailHeadOrNull = head
        this
      } else {
        tailKeyValues = tailKeyValues.drop(1)
        tailHeadOrNull = head
        this
      }

    override def isEmpty: Boolean =
      iterator.isEmpty

    override def iterator: Iterator[T] =
      new Iterator[T] {
        private var headDone = false
        private var placeHolderDone = false

        override def hasNext: Boolean =
          (!headDone && headRangeOrNull != null) || (!placeHolderDone && tailHeadOrNull != null) || tailKeyValues.hasNext

        override def next(): T =
          if (!headDone && headRangeOrNull != null) {
            headDone = true
            headRangeOrNull
          } else if (!placeHolderDone && tailHeadOrNull != null) {
            placeHolderDone = true
            tailHeadOrNull
          } else {
            tailKeyValues.next()
          }
      }
  }

  private[core] class Nest[H >: Null <: T, T >: Null] private[DropIterator](private var left: DropIterator[H, T],
                                                                            right: DropIterator[H, T]) extends DropIterator[H, T] {

    override def dropHead(): DropIterator[H, T] =
      if (left.isEmpty && right.isEmpty) {
        DropIterator.empty
      } else if (left.isEmpty && !right.isEmpty) {
        right.dropHead()
      } else if (!left.isEmpty && right.isEmpty) {
        left.dropHead()
      } else {
        left = left.dropHead()
        this
      }

    override def dropPrepend(head: H): DropIterator[H, T] =
      if (left.isEmpty && right.isEmpty) {
        new Flat[H, T](head, null, Iterator.empty)
      } else if (left.isEmpty && !right.isEmpty) {
        right.dropPrepend(head)
      } else if (!left.isEmpty && right.isEmpty) {
        left.dropPrepend(head)
      } else {
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
      left.iterator.isEmpty || right.iterator.isEmpty

    override def depth: Int =
      left.depth + right.depth
  }
}
