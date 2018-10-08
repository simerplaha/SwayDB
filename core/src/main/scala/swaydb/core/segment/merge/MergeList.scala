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

import swaydb.core.data.{KeyValue, Memory}
import swaydb.data.slice.Slice

/**
  * Mutable data type to hold the state of currently being merged key-values and provides functions
  * to mutate it's state.
  *
  * This cannot be immutable as it will add a lot to GC workload.
  *
  * A Segment can easily have over 100,000 key-values to merge and an immutable
  * version of this class would create the same number of of MergeList instances in-memory.
  */
private[core] sealed trait MergeList extends Iterable[KeyValue.ReadOnly] {

  def headOption: Option[KeyValue.ReadOnly]

  def dropHead(): MergeList

  def dropPrepend(head: Memory.Range): MergeList

  def depth: Int

  override def size: Int
}

private[core] object MergeList {

  val empty =
    new Single(None, Slice.empty[KeyValue.ReadOnly])

  def apply(keyValues: Slice[KeyValue.ReadOnly]): MergeList =
    new Single(None, keyValues)

  implicit class MergeListImplicit(left: MergeList) {
    def append(right: MergeList): MergeList =
      if (left.isEmpty)
        right
      else if (right.isEmpty)
        left
      else
        new Multiple(left, right)
  }
}

private[core] class Single(private var headRange: Option[Memory.Range],
                           private var tailKeyValues: Slice[KeyValue.ReadOnly]) extends MergeList {

  override val depth: Int = 1

  override def headOption: Option[KeyValue.ReadOnly] =
    headRange orElse tailKeyValues.headOption

  def dropHead(): MergeList = {
    if (headRange.isDefined) {
      headRange = None
      this
    } else {
      tailKeyValues = tailKeyValues.drop(1)
      this
    }
  }

  def dropPrepend(head: Memory.Range): MergeList =
    if (headRange.isDefined) {
      headRange = Some(head)
      this
    } else {
      tailKeyValues = tailKeyValues.drop(1)
      headRange = Some(head)
      this
    }

  override def iterator = new Iterator[KeyValue.ReadOnly] {
    private var headDone = false
    private val sliceIterator = tailKeyValues.iterator

    override def hasNext: Boolean =
      (!headDone && headRange.isDefined) || sliceIterator.hasNext

    override def next(): KeyValue.ReadOnly =
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

private[core] class Multiple(private var left: MergeList,
                             right: MergeList) extends MergeList {

  override def dropHead(): MergeList =
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

  override def dropPrepend(head: Memory.Range): MergeList =
    (left.isEmpty, right.isEmpty) match {
      case (true, true) =>
        new Single(Some(head), Slice.empty[KeyValue.ReadOnly])
      case (true, false) =>
        right.dropPrepend(head)
      case (false, true) =>
        left.dropPrepend(head)
      case (false, false) =>
        left = left.dropPrepend(head)
        this
    }

  override def iterator: Iterator[KeyValue.ReadOnly] = new Iterator[KeyValue.ReadOnly] {
    private val leftIterator = left.iterator
    private val rightIterator = right.iterator

    override def hasNext: Boolean =
      leftIterator.hasNext || rightIterator.hasNext

    override def next(): KeyValue.ReadOnly =
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