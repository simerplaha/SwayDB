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

package swaydb.core.level.compaction.task

import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.Assignable

/**
 * Provides segmentSize implementation for different source data types.
 *
 * Source data type can be any type that can be submitted to a [[swaydb.core.level.Level]]
 * for merge.
 *
 * [[A]] cannot be a contravariant type because [[CompactionDataType.TooSmallCollectionCompactionDataType]] is a
 * super type of [[Segment]].
 */
trait CompactionDataType[A] {
  @inline def segmentSize(segment: A): Int
}

protected object CompactionDataType {

  implicit class SelectorTypeImplicits[A](target: A) {
    @inline def segmentSize(implicit targetType: CompactionDataType[A]) =
      targetType.segmentSize(target)
  }

  implicit object SegmentCompactionDataType extends CompactionDataType[Segment] {
    @inline override def segmentSize(segment: Segment): Int =
      segment.segmentSize
  }

  /**
   * SegmentSize being 1 for all Collection types will ensure that all
   * collections are compacted
   */
  implicit object TooSmallCollectionCompactionDataType extends CompactionDataType[Assignable.Collection] {
    @inline override def segmentSize(segment: Assignable.Collection): Int =
      1
  }
}
