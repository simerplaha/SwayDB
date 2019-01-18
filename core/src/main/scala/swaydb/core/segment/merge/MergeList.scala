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

package swaydb.core.segment.merge

/**
  * Mutable data type to hold the state of currently being merged key-values and provides functions
  * to mutate it's state.
  *
  * This cannot be immutable as it will add a lot to GC workload.
  *
  * A Segment can easily have over 100,000 key-values to merge and an immutable
  * version of this class would create the same number of of MergeList instances in-memory.
  */
private[core] sealed trait MergeList[H <: T, T] extends Iterable[T] {

  def headOption: Option[T]

  def nextOption: Option[T]

  def dropHead(): MergeList[H, T]

  def dropPrepend(head: H): MergeList[H, T]

  def depth: Int

  override def size: Int
}

private[core] object MergeList {

  def empty[H <: T, T] =
    new Single[H, T](Option.empty[H], Iterable.empty[T])

  def apply[H <: T, T](keyValues: Iterable[T]): MergeList[H, T] =
    new Single[H, T](Option.empty[H], keyValues)

  implicit class MergeListImplicit[H <: T, T](left: MergeList[H, T]) {
    def append(right: MergeList[H, T]): MergeList[H, T] =
      if (left.isEmpty)
        right
      else if (right.isEmpty)
        left
      else
        new Multiple(left, right)
  }
}

private[core] class Single[H <: T, T](private var headRange: Option[H],
                                      private var tailKeyValues: Iterable[T]) extends MergeList[H, T] {

  override val depth: Int = 1

  override def headOption: Option[T] =
    headRange orElse tailKeyValues.headOption

  override def nextOption: Option[T] =
    if (headRange.isDefined)
      tailKeyValues.headOption
    else
      tailKeyValues.drop(1).headOption

  def dropHead(): MergeList[H, T] = {
    if (headRange.isDefined) {
      headRange = None
      this
    } else {
      tailKeyValues = tailKeyValues.drop(1)
      this
    }
  }

  def dropPrepend(head: H): MergeList[H, T] =
    if (headRange.isDefined) {
      headRange = Some(head)
      this
    } else {
      tailKeyValues = tailKeyValues.drop(1)
      headRange = Some(head)
      this
    }

  override def iterator = new Iterator[T] {
    private var headDone = false
    private val sliceIterator = tailKeyValues.iterator

    override def hasNext: Boolean =
      (!headDone && headRange.isDefined) || sliceIterator.hasNext

    override def next(): T =
      if (!headDone && headRange.isDefined) {
        headDone = true
        headRange.get
      } else {
        sliceIterator.next()
      }
  }

  override def size =
    tailKeyValues.size + (if (headRange.isDefined) 1 else 0)

}

private[core] class Multiple[H <: T, T](private var left: MergeList[H, T],
                                        right: MergeList[H, T]) extends MergeList[H, T] {

  override def dropHead(): MergeList[H, T] =
    (left.isEmpty, right.isEmpty) match {
      case (true, true) =>
        MergeList.empty
      case (true, false) =>
        right.dropHead()
      case (false, true) =>
        left.dropHead()
      case (false, false) =>
        left = left.dropHead()
        this
    }

  override def dropPrepend(head: H): MergeList[H, T] =
    (left.isEmpty, right.isEmpty) match {
      case (true, true) =>
        new Single[H, T](Some(head), Iterable.empty[T])
      case (true, false) =>
        right.dropPrepend(head)
      case (false, true) =>
        left.dropPrepend(head)
      case (false, false) =>
        left = left.dropPrepend(head)
        this
    }

  override def headOption: Option[T] =
    left.headOption orElse right.headOption

  override def nextOption: Option[T] =
    left.nextOption orElse right.nextOption

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

  override def depth: Int =
    left.depth + right.depth

  override def size =
    left.size + right.size

}
