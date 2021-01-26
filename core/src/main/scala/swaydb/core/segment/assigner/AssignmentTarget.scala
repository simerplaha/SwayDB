/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.segment.assigner

import swaydb.core.segment.Segment
import swaydb.core.segment.ref.SegmentRef
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

import scala.annotation.implicitNotFound

/**
 * [[AssignmentTarget]] implements functions that [[Assigner]]
 * uses to assign [[Assignable]] to target [[A]] types.
 *
 * Currently the following type classes allow [[Assigner]] to assign
 * [[Assignable]] to [[Segment]] and [[SegmentRef]].
 */
@implicitNotFound("Type class implementation not found for AssignmentTarget of type ${A}")
sealed trait AssignmentTarget[-A] {
  def minKey(segment: A): Slice[Byte]
  def maxKey(segment: A): MaxKey[Slice[Byte]]
}

object AssignmentTarget {

  implicit class SegmentTypeImplicits[A](target: A) {
    @inline def minKey(implicit targetType: AssignmentTarget[A]) =
      targetType.minKey(target)

    @inline def maxKey(implicit targetType: AssignmentTarget[A]) =
      targetType.maxKey(target)
  }

  implicit object SegmentTarget extends AssignmentTarget[Segment] {
    override def minKey(segment: Segment): Slice[Byte] =
      segment.minKey

    override def maxKey(segment: Segment): MaxKey[Slice[Byte]] =
      segment.maxKey
  }

  implicit object SegmentRefTarget extends AssignmentTarget[SegmentRef] {
    override def minKey(ref: SegmentRef): Slice[Byte] =
      ref.minKey

    override def maxKey(ref: SegmentRef): MaxKey[Slice[Byte]] =
      ref.maxKey
  }
}
