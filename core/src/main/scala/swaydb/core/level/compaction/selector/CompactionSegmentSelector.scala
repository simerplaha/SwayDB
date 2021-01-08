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

package swaydb.core.level.compaction.selector

import swaydb.Aggregator
import swaydb.core.level.Level
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, SegmentAssigner, SegmentAssignment}
import swaydb.data.NonEmptyList
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case object CompactionSegmentSelector {

  @inline def select(source: Level,
                     nextLevels: NonEmptyList[Level],
                     sourceOverflow: Long): Iterable[CompactionTask.Task] = {
    implicit val keyOrder: KeyOrder[Slice[Byte]] = source.keyOrder

    val tasks = ListBuffer.empty[CompactionTask.Task]

    select(
      sourceSegments = source.segments(),
      targetFirstLevel = nextLevels.head,
      nextLevels = nextLevels,
      sourceOverflow = sourceOverflow,
      tasks = tasks
    )

    tasks
  }

  @tailrec
  private def select(sourceSegments: Iterable[Segment],
                     targetFirstLevel: Level,
                     nextLevels: NonEmptyList[Level],
                     sourceOverflow: Long,
                     tasks: ListBuffer[CompactionTask.Task])(implicit keyOrder: KeyOrder[Slice[Byte]]): Unit = {
    implicit val segmentOrder: Ordering[Segment] = keyOrder.on[Segment](_.minKey)

    //convert Segments to key-values
    val (segmentKeyValues, segmentMapIndex) =
      CompactionSelectorCommon.toKeyValues(sourceSegments)

    implicit val creator: Aggregator.Creator[Assignable, mutable.SortedSet[Segment]] =
      CompactionSelectorCommon.aggregatorCreator(segmentOrder, segmentMapIndex)

    val nextLevel = nextLevels.head

    //performing assignment and scoring and fetch Segments that can be merged and segments that can be copied.
    val (segmentsToMerge, segmentsToCopy) =
      if (nextLevel.isNonEmpty()) { //if next Level is not empty then do assignment
        val assignments: ListBuffer[SegmentAssignment[mutable.SortedSet[Segment], mutable.SortedSet[Segment], Segment]] =
          SegmentAssigner.assignUnsafeGaps[mutable.SortedSet[Segment], mutable.SortedSet[Segment], Segment](
            assignablesCount = segmentKeyValues.size,
            assignables = segmentKeyValues.iterator,
            segments = nextLevel.segments().iterator
          )

        val scoredAssignments =
          CompactionSelectorCommon.scoreAssignments(assignments)(CompactionAssignmentScorer.segmentScorer)

        CompactionSelectorCommon.finaliseSegmentsToCompact(
          sourceOverflow = sourceOverflow,
          scoredAssignments = scoredAssignments
        )
      } else {
        //else segments can be copied.
        val segmentsToCopy =
          CompactionSelectorCommon.fillOverflow(
            overflow = sourceOverflow,
            segments = sourceSegments
          )

        (Segment.emptyIterable, segmentsToCopy)
      }

    //if there are Segments to merge then create a task.
    if (segmentsToMerge.nonEmpty)
      tasks +=
        CompactionTask.Task(
          segments = segmentsToMerge,
          targetLevel = nextLevel
        )

    //if segmentToCopy is not empty then try to finding a
    //target Level where these Segments can be merged.
    //if no segments overlap then copied it into the first level.
    if (segmentsToCopy.nonEmpty)
      if (nextLevels.tail.nonEmpty) {
        select(
          sourceSegments = segmentsToCopy,
          targetFirstLevel = targetFirstLevel,
          nextLevels = NonEmptyList(nextLevels.tail.head, nextLevels.tail.drop(1)),
          sourceOverflow = Long.MaxValue, //all Segments should be merged.
          tasks = tasks
        )
      } else {
        val oldTaskIndex = tasks.indexWhere(_.targetLevel == targetFirstLevel)

        if (oldTaskIndex < 0) {
          tasks +=
            CompactionTask.Task(
              segments = segmentsToCopy,
              targetLevel = nextLevel
            )
        } else {
          val oldTask = tasks(oldTaskIndex)

          val newSegments = mutable.SortedSet.empty[Segment](segmentOrder)
          newSegments ++= oldTask.segments
          newSegments ++= segmentsToCopy

          val updatedTask = oldTask.copy(segments = newSegments)
          tasks.update(oldTaskIndex, updatedTask)
        }
      }
  }
}
