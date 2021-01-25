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

package swaydb.core.level

import swaydb.core.data.Memory
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, SegmentAssignment}

import scala.collection.mutable.ListBuffer

case class LevelAssignment(newKeyValues: Iterable[Assignable],
                           removeDeletedRecords: Boolean,
                           assignments: Iterable[SegmentAssignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]) {

  /**
   * Casting is temporary solution. Level should be typed to either persistent or in-memory so this type-casting is not required.
   */
  def castToPersistentAssignments: Iterable[SegmentAssignment[Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]] =
    assignments.asInstanceOf[Iterable[SegmentAssignment[Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]]

  def castToMemoryAssignments: Iterable[SegmentAssignment[Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]] =
    assignments.asInstanceOf[Iterable[SegmentAssignment[Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]]
}