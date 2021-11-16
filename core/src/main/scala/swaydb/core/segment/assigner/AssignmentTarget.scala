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

package swaydb.core.segment.assigner

import swaydb.core.segment.Segment
import swaydb.core.segment.ref.SegmentRef
import swaydb.slice.MaxKey
import swaydb.slice.Slice

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
