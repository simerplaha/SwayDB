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

package swaydb.core.segment.defrag

import swaydb.core.data.KeyValue
import swaydb.core.segment.Segment
import swaydb.core.segment.ref.SegmentRef
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for DefragSource of type ${A}")
sealed trait DefragSource[-A] {
  def minKey(segment: A): Slice[Byte]
  def maxKey(segment: A): MaxKey[Slice[Byte]]
  def segmentSize(segment: A): Int
  def keyValueCount(segment: A): Int
  def hasUpdateOrRangeOrExpired(segment: A): Boolean
  def iterator(segment: A, inOneSeek: Boolean): Iterator[KeyValue]
}

object DefragSource {

  implicit class SegmentTypeImplicits[A](target: A) {
    @inline def minKey(implicit targetType: DefragSource[A]) =
      targetType.minKey(target)

    @inline def maxKey(implicit targetType: DefragSource[A]) =
      targetType.maxKey(target)

    @inline def segmentSize(implicit targetType: DefragSource[A]) =
      targetType.segmentSize(target)

    @inline def keyValueCount(implicit targetType: DefragSource[A]) =
      targetType.keyValueCount(target)

    @inline def hasUpdateOrRangeOrExpired(implicit targetType: DefragSource[A]) =
      targetType.hasUpdateOrRangeOrExpired(target)

    @inline def iterator(inOneSeek: Boolean)(implicit targetType: DefragSource[A]) =
      targetType.iterator(target, inOneSeek)
  }

  implicit object SegmentTarget extends DefragSource[Segment] {
    override def minKey(segment: Segment): Slice[Byte] =
      segment.minKey

    override def maxKey(segment: Segment): MaxKey[Slice[Byte]] =
      segment.maxKey

    override def segmentSize(segment: Segment): Int =
      segment.segmentSize

    override def keyValueCount(segment: Segment): Int =
      segment.keyValueCount

    override def iterator(segment: Segment, inOneSeek: Boolean): Iterator[KeyValue] =
      segment.iterator(inOneSeek)

    override def hasUpdateOrRangeOrExpired(segment: Segment): Boolean =
      segment.hasUpdateOrRangeOrExpired()
  }

  implicit object SegmentRefTarget extends DefragSource[SegmentRef] {
    override def minKey(ref: SegmentRef): Slice[Byte] =
      ref.minKey

    override def maxKey(ref: SegmentRef): MaxKey[Slice[Byte]] =
      ref.maxKey

    override def segmentSize(ref: SegmentRef): Int =
      ref.segmentSize

    override def keyValueCount(ref: SegmentRef): Int =
      ref.keyValueCount

    override def iterator(ref: SegmentRef, inOneSeek: Boolean): Iterator[KeyValue] =
      ref.iterator(inOneSeek)

    override def hasUpdateOrRangeOrExpired(ref: SegmentRef): Boolean =
      ref.hasUpdateOrRangeOrExpired()
  }
}
