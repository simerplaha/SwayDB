/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
