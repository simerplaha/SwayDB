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

import swaydb.Aggregator
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{Memory, Time, Value}
import swaydb.core.level.compaction.selector.SelectorDataType._
import swaydb.core.segment.assigner.{Assignable, SegmentAssignment}
import swaydb.core.util.Collections._
import swaydb.data.MaxKey
import swaydb.data.slice.{Slice, SliceOption}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

protected case object CompactionSelectorCommon {

  /**
   * Converts [[A]] to key-values and returns a map
   * of [[A]]s which can be used to get the [[A]].
   */
  def toKeyValues[A <: Assignable.Collection](segments: Iterable[A]): (ListBuffer[Memory], collection.Map[Int, A]) = {
    val segmentKeyValues = ListBuffer.empty[Memory]
    val segmentIndex = mutable.Map.empty[Int, A]
    var index = 0

    segments foreach {
      segment =>
        val indexBytes = Slice.writeUnsignedInt[Byte](index)
        segmentIndex.put(index, segment)

        if (segment.keyValueCount == 1)
          segment.maxKey match {
            case MaxKey.Fixed(maxKey) =>
              segmentKeyValues += Memory.Put(maxKey, indexBytes, None, Time.empty)

            case MaxKey.Range(fromKey, maxKey) =>
              segmentKeyValues += Memory.Range(fromKey, maxKey, FromValue.Null, Value.Update(indexBytes, None, Time.empty))
          }
        else
          segment.maxKey match {
            case MaxKey.Fixed(maxKey) =>
              segmentKeyValues += Memory.Range(segment.key, maxKey, FromValue.Null, Value.Update(indexBytes, None, Time.empty))
              segmentKeyValues += Memory.Put(maxKey, indexBytes, None, Time.empty)

            case MaxKey.Range(_, maxKey) =>
              segmentKeyValues += Memory.Range(segment.key, maxKey, FromValue.Null, Value.Update(indexBytes, None, Time.empty))
          }

        index += 1
    }

    (segmentKeyValues, segmentIndex)
  }

  /**
   * [[toKeyValues]] converts [[Assignable.Collection]] to [[Memory]] types
   * which are then used for assignment but currently [[Memory]] types do not
   * support custom value types are required a byte [[SliceOption]].
   *
   * So temporarily we cast [[swaydb.core.segment.Segment]] so that after assignments we can
   * fetch the Segment.
   */
  def aggregatorCreator[A](implicit ordering: Ordering[A],
                           index: collection.Map[Int, A]): Aggregator.Creator[Assignable, mutable.SortedSet[A]] =
    () =>
      new Aggregator[Assignable, mutable.SortedSet[A]] {
        val segments = mutable.SortedSet.empty[A]

        override def add(item: Assignable): Unit =
          item match {
            case Memory.Put(_, value, _, _) =>
              segments += index(value.getC.readUnsignedInt())

            case Memory.Range(_, _, _, Value.Update(value, _, _)) =>
              segments += index(value.getC.readUnsignedInt())

            case assignable =>
              throw new Exception(s"Invalid item. ${assignable.getClass.getName}")
          }

        override def result: mutable.SortedSet[A] =
          segments
      }

  def scoreAssignments[A <: Assignable.Collection](assignment: ListBuffer[SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], A]])(implicit scorer: Ordering[SegmentAssignment[Iterable[A], Iterable[A], Iterable[A]]]) = {
    var previousAssignmentOrNull: SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], A] = null

    val groupedAssignments = ListBuffer.empty[SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], ListBuffer[A]]]

    assignment foreach {
      nextAssignment =>
        val spreads =
          previousAssignmentOrNull != null && {
            //check if previous assignment spreads onto next assignment.
            val previousAssignmentsLast =
              previousAssignmentOrNull.tailGap.result.lastOption
                .orElse(previousAssignmentOrNull.midOverlap.result.lastOption)
                .orElse(previousAssignmentOrNull.headGap.result.lastOption)

            val nextAssignmentsFirst =
              nextAssignment.headGap.result.lastOption
                .orElse(nextAssignment.midOverlap.result.lastOption)
                .orElse(nextAssignment.tailGap.result.lastOption)

            previousAssignmentsLast == nextAssignmentsFirst
          }

        if (spreads) {
          groupedAssignments.last.headGap addAll nextAssignment.headGap.result
          groupedAssignments.last.midOverlap addAll nextAssignment.midOverlap.result
          groupedAssignments.last.tailGap addAll nextAssignment.tailGap.result
        } else {
          groupedAssignments +=
            SegmentAssignment(
              segment = ListBuffer(nextAssignment.segment),
              headGap = nextAssignment.headGap,
              midOverlap = nextAssignment.midOverlap,
              tailGap = nextAssignment.tailGap
            )
        }

        previousAssignmentOrNull = nextAssignment
    }

    groupedAssignments.sorted(scorer)
  }

  def finaliseSegmentsToCompact[A, B](sourceOverflow: Long,
                                      scoredAssignments: Iterable[SegmentAssignment[Iterable[A], Iterable[A], B]])(implicit segmentOrdering: Ordering[A],
                                                                                                                   selectorType: SelectorDataType[A]): (Iterable[A], Iterable[A]) = {
    var sizeTaken = 0L

    val segmentsToMerge = mutable.SortedSet.empty[A]
    val segmentsToCopy = mutable.SortedSet.empty[A]

    scoredAssignments
      .foreachBreak {
        assignment =>
          segmentsToCopy ++= assignment.headGap.result
          segmentsToCopy ++= assignment.tailGap.result

          if (assignment.midOverlap.result.nonEmpty) {
            val midSize = assignment.midOverlap.result.foldLeft(0)(_ + _.segmentSize)
            sizeTaken += midSize
            sizeTaken >= sourceOverflow
          } else {
            true
          }
      }

    val copyable =
      fillOverflow(
        overflow = sourceOverflow - sizeTaken,
        segments = segmentsToCopy
      )

    (segmentsToMerge, copyable)
  }

  def fillOverflow[A](overflow: Long, segments: Iterable[A])(implicit selectorType: SelectorDataType[A]): Iterable[A] = {
    var remainingOverflow = overflow

    if (remainingOverflow > 0)
      segments takeWhile {
        segment =>
          remainingOverflow -= segment.segmentSize
          remainingOverflow >= 0
      }
    else
      Iterable.empty
  }
}
