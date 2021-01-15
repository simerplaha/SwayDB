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

package swaydb.core.level.compaction.task

import swaydb.Aggregator
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{Memory, Time, Value}
import swaydb.core.level.Level
import swaydb.core.level.compaction.task.CompactionDataType._
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, SegmentAssigner, SegmentAssignment}
import swaydb.core.util.Collections._
import swaydb.data.order.KeyOrder
import swaydb.data.{MaxKey, NonEmptyList}
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec
import scala.collection.compat.IterableOnce
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Builds optimal compaction tasks to perform that meet the configured
 * [[swaydb.data.compaction.Throttle]] criteria.
 */
protected case object CompactionTasker {

  def run[A <: Assignable.Collection](data: Iterable[A],
                                      lowerLevels: NonEmptyList[Level],
                                      dataOverflow: Long)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                          dataType: CompactionDataType[A]): Iterable[CompactionTask.Task[A]] = {

    val tasks = ListBuffer.empty[CompactionTask.Task[A]]

    buildTasks(
      data = data,
      targetFirstLevel = lowerLevels.head,
      lowerLevels = lowerLevels,
      dataOverflow = dataOverflow,
      tasks = tasks
    )

    tasks
  }

  /**
   * Assigns the input data to levels that have overlapping key-values.
   * If there were no overlapping segments then it assigns data
   * to first encountered non-overlapping Level for copying.
   *
   * @param data             New data which requires merging/assignment to an optimal
   *                         level.
   * @param targetFirstLevel First level which remains fixed throughout the recursion.
   * @param lowerLevels      Remaining levels.
   * @param dataOverflow     Sets the total size of data to compact. This is dictated
   *                         by the [[swaydb.data.compaction.Throttle]] configuration.
   * @param tasks            Final [[CompactionTask.Task]] that will contain optimal
   *                         assignments.
   * @tparam A the type of input data.
   */
  @tailrec
  private def buildTasks[A <: Assignable.Collection](data: Iterable[A],
                                                     targetFirstLevel: Level,
                                                     lowerLevels: NonEmptyList[Level],
                                                     dataOverflow: Long,
                                                     tasks: ListBuffer[CompactionTask.Task[A]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                dataType: CompactionDataType[A]): Unit = {
    implicit val segmentOrder: Ordering[A] = keyOrder.on[A](_.key)

    //convert Segments to key-values for SegmentAssigner.
    val (segmentKeyValues, segmentMapIndex) =
      CompactionTasker.toKeyValues(data)

    //build an aggregator such that all Assignable assignments are converted back to
    //the input data type. i.e. if input data type was Segment, mutable.SortedSet[A] will be
    //mutable.SortedSet[Segment]

    implicit val creator: Aggregator.Creator[Assignable, mutable.SortedSet[A]] =
      CompactionTasker.aggregatorCreator(segmentOrder, segmentMapIndex)

    //get the next level from remaining levels.
    val nextLevel = lowerLevels.head

    //performing assignment and scoring and fetch Segments that can be merged and segments that can be copied.
    val (segmentsToMerge, segmentsToCopy) =
      if (nextLevel.isNonEmpty()) { //if next Level is not empty then do assignment
        val assignments: ListBuffer[SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], Segment]] =
          SegmentAssigner.assignUnsafeGaps[mutable.SortedSet[A], mutable.SortedSet[A], Segment](
            keyValues = segmentKeyValues.iterator,
            segments = nextLevel.segments().iterator
          )

        //score the assignments
        val scoredAssignments =
          CompactionTasker.scoreAndGroupAssignments[A, Segment](assignments)

        //finalise Segments to compact.
        CompactionTasker.finaliseSegmentsToCompact(
          dataOverflow = dataOverflow,
          scoredAssignments = scoredAssignments
        )
      } else { //else segments can be copied.
        val segmentsToCopy =
          CompactionTasker.fillOverflow(
            overflow = dataOverflow,
            data = data
          )

        (Iterable.empty, segmentsToCopy)
      }

    //if there are Segments to merge then create a task.
    if (segmentsToMerge.nonEmpty)
      tasks +=
        CompactionTask.Task(
          data = segmentsToMerge,
          targetLevel = nextLevel
        )

    //if segmentToCopy is not empty then try to finding a
    //target Level where these Segments can be merged.
    //if no segments overlap then copied it into the first level.
    if (segmentsToCopy.nonEmpty)
      if (lowerLevels.tail.nonEmpty) {
        //there there are lower level then try assigning the copyable Segments to
        //lower level to find a segments that can be merged with this copyable data.
        buildTasks[A](
          data = segmentsToCopy,
          targetFirstLevel = targetFirstLevel,
          lowerLevels = NonEmptyList(lowerLevels.tail.head, lowerLevels.tail.drop(1)),
          dataOverflow = Long.MaxValue, //all Segments should be merged to set overflow to be maximum.
          tasks = tasks
        )
      } else { //there were not lower levels
        //find the first level that this data could've be copied into
        //and assign this data to that level.
        val oldTaskIndex = tasks.indexWhere(_.targetLevel == targetFirstLevel)

        if (oldTaskIndex < 0) {
          tasks +=
            CompactionTask.Task(
              data = segmentsToCopy,
              targetLevel = nextLevel
            )
        } else {
          val oldTask = tasks(oldTaskIndex)

          val newSegments = mutable.SortedSet.empty[A](segmentOrder)
          newSegments ++= oldTask.data
          newSegments ++= segmentsToCopy

          val updatedTask = oldTask.copy(data = newSegments)
          tasks.update(oldTaskIndex, updatedTask)
        }
      }
  }

  /**
   * Converts [[Assignable.Collection]] type to [[Memory]] key-values types
   * which can then be used by [[SegmentAssigner]] for assignments.
   *
   * Each [[Memory]] stores a unique Int value which can be read to fetch the
   * input data type from the returned [[collection.Map]].
   */
  def toKeyValues[A <: Assignable.Collection](data: IterableOnce[A]): (ListBuffer[Memory], collection.Map[Int, A]) = {
    val segmentKeyValues = ListBuffer.empty[Memory]
    val segmentIndex = mutable.Map.empty[Int, A]
    var index = 0

    data foreach {
      segment =>
        val indexBytes = Slice.writeUnsignedInt[Byte](index)
        segmentIndex.put(index, segment)

        if (segment.keyValueCount == 1)
          segment.maxKey match {
            case MaxKey.Fixed(maxKey: Slice[Byte]) =>
              segmentKeyValues += Memory.Put(maxKey, indexBytes, None, Time.empty)

            case MaxKey.Range(fromKey: Slice[Byte], maxKey: Slice[Byte]) =>
              segmentKeyValues += Memory.Range(fromKey, maxKey, FromValue.Null, Value.Update(indexBytes, None, Time.empty))
          }
        else
          segment.maxKey match {
            case MaxKey.Fixed(maxKey: Slice[Byte]) =>
              segmentKeyValues += Memory.Range(segment.key, maxKey, FromValue.Null, Value.Update(indexBytes, None, Time.empty))
              segmentKeyValues += Memory.Put(maxKey, indexBytes, None, Time.empty)

            case MaxKey.Range(_, maxKey: Slice[Byte]) =>
              segmentKeyValues += Memory.Range(segment.key, maxKey, FromValue.Null, Value.Update(indexBytes, None, Time.empty))
          }

        index += 1
    }

    (segmentKeyValues, segmentIndex)
  }

  /**
   * An aggregator which converts [[SegmentAssigner]]'s assignments back to the original
   * input data type.
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

  /**
   * @tparam A the type of input data
   * @tparam B the type of target data to which the the [[A]]'s would be assigned to.
   *           This would be most be [[Segment]].
   * @return optimal assignments based on scoring defined by [[CompactionAssignmentScorer]].
   */
  def scoreAndGroupAssignments[A <: Assignable.Collection, B](assignments: ListBuffer[SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], B]])(implicit inputDataType: CompactionDataType[A],
                                                                                                                                                         targetDataType: CompactionDataType[B]): ListBuffer[SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], ListBuffer[B]]] = {
    var previousAssignmentOrNull: SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], B] = null

    val groupedAssignments = ListBuffer.empty[SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], ListBuffer[B]]]

    assignments foreach {
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

    groupedAssignments.sorted(CompactionAssignmentScorer.scorer[A, B])
  }

  /**
   * Takes enough segments to control the data overflow.
   * Prioritise segments that can be merged (overlapping segments) over
   * segments that can be copied.
   */
  def finaliseSegmentsToCompact[A, B](dataOverflow: Long,
                                      scoredAssignments: Iterable[SegmentAssignment[Iterable[A], Iterable[A], B]])(implicit segmentOrdering: Ordering[A],
                                                                                                                   dataType: CompactionDataType[A]): (Iterable[A], Iterable[A]) = {
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
            sizeTaken >= dataOverflow
          } else {
            true
          }
      }

    val copyable =
      fillOverflow(
        overflow = dataOverflow - sizeTaken,
        data = segmentsToCopy
      )

    (segmentsToMerge, copyable)
  }

  def fillOverflow[A](overflow: Long, data: Iterable[A])(implicit dataType: CompactionDataType[A]): Iterable[A] = {
    var remainingOverflow = overflow

    if (remainingOverflow > 0)
      data takeWhile {
        segment =>
          remainingOverflow -= segment.segmentSize
          remainingOverflow >= 0
      }
    else
      Iterable.empty
  }
}
