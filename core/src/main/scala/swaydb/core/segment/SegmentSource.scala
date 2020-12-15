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

package swaydb.core.segment

import swaydb.core.data.KeyValue
import swaydb.core.segment.assigner.{Assignable, SegmentAssigner}
import swaydb.core.segment.ref.SegmentRef
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

import scala.annotation.implicitNotFound

/**
 * [[SegmentSource]] implements functions that [[SegmentAssigner]]
 * uses to assign [[Assignable]] to target [[A]] types.
 *
 * Currently the following type classes allow [[SegmentAssigner]] to assign
 * [[Assignable]] to [[Segment]] and [[SegmentRef]].
 */
@implicitNotFound("Type class implementation not found for SegmentType of type ${A}")
sealed trait SegmentSource[A] {
  def minKey(segment: A): Slice[Byte]
  def maxKey(segment: A): MaxKey[Slice[Byte]]
  def segmentSize(segment: A): Int
  def hasNonPut(segment: A): Boolean
  def getKeyValueCount(segment: A): Int
  def iterator(segment: A): Iterator[KeyValue]
}

object SegmentSource {

  implicit class SegmentTypeImplicits[A](target: A) {
    @inline def minKey(implicit targetType: SegmentSource[A]) =
      targetType.minKey(target)

    @inline def maxKey(implicit targetType: SegmentSource[A]) =
      targetType.maxKey(target)

    @inline def segmentSize(implicit targetType: SegmentSource[A]) =
      targetType.segmentSize(target)

    @inline def hasNonPut(implicit targetType: SegmentSource[A]) =
      targetType.hasNonPut(target)

    @inline def getKeyValueCount()(implicit targetType: SegmentSource[A]) =
      targetType.getKeyValueCount(target)

    @inline def iterator()(implicit targetType: SegmentSource[A]) =
      targetType.iterator(target)
  }

  implicit object SegmentTarget extends SegmentSource[Segment] {
    override def minKey(segment: Segment): Slice[Byte] =
      segment.minKey

    override def maxKey(segment: Segment): MaxKey[Slice[Byte]] =
      segment.maxKey

    override def segmentSize(segment: Segment): Int =
      segment.segmentSize

    override def hasNonPut(segment: Segment): Boolean =
      segment.hasNonPut

    override def getKeyValueCount(segment: Segment): Int =
      segment.getKeyValueCount()

    override def iterator(segment: Segment): Iterator[KeyValue] =
      segment.iterator()
  }

  implicit object SegmentRefTarget extends SegmentSource[SegmentRef] {
    override def minKey(ref: SegmentRef): Slice[Byte] =
      ref.minKey

    override def maxKey(ref: SegmentRef): MaxKey[Slice[Byte]] =
      ref.maxKey

    override def segmentSize(ref: SegmentRef): Int =
      ref.segmentSize

    override def hasNonPut(ref: SegmentRef): Boolean =
      ref.hasNonPut

    override def getKeyValueCount(ref: SegmentRef): Int =
      ref.getKeyValueCount()

    override def iterator(ref: SegmentRef): Iterator[KeyValue] =
      ref.iterator()
  }
}
