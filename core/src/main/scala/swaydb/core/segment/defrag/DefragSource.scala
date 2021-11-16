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

package swaydb.core.segment.defrag

import swaydb.core.data.KeyValue
import swaydb.core.segment.Segment
import swaydb.core.segment.ref.SegmentRef
import swaydb.data.MaxKey
import swaydb.slice.Slice

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
