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

package swaydb.core.level.compaction.selector

import swaydb.core.segment.Segment

/**
 * Type that can be selected for compaction. Currently
 * this can either be files that are managed by [[swaydb.core.level.zero.LevelZero]]
 * and [[swaydb.core.level.Level]] which are [[swaydb.core.map.Map]] and [[Segment]]
 * respectively.
 */
sealed trait SelectorDataType[-A] {
  @inline def segmentSize(segment: A): Int
}

object SelectorDataType {

  implicit class SelectorDataTypeImplicits[A](target: A) {
    @inline def segmentSize(implicit targetType: SelectorDataType[A]) =
      targetType.segmentSize(target)
  }

  implicit object SegmentSelectorDataType extends SelectorDataType[Segment] {
    @inline override def segmentSize(segment: Segment): Int =
      segment.segmentSize
  }
}
