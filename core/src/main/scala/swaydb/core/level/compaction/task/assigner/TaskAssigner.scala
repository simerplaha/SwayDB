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

package swaydb.core.level.compaction.task.assigner

import swaydb.Aggregator
import swaydb.core.data.Value.FromValue
import swaydb.core.data.{Memory, Time, Value}
import swaydb.core.level.compaction.task.CompactionDataType._
import swaydb.core.level.compaction.task.{CompactionDataType, CompactionTask}
import swaydb.core.level.{Level, LevelAssignment}
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, SegmentAssigner, SegmentAssignment, SegmentAssignmentResult}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.{MaxKey, NonEmptyList}

import scala.annotation.tailrec
import scala.collection.compat.IterableOnce
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

/**
 * Builds optimal compaction tasks to perform that meet the configured
 * [[swaydb.data.compaction.Throttle]] criteria.
 */
protected case object TaskAssigner {

  /**
   * Re-assigns tasks to target Level but this time it allows
   * expanding input data to handle cases where key-values spread
   * to multiple Segments.
   */
  def assignExpand[A <: Assignable.Collection](tasks: Iterable[CompactionTask.Task[A]],
                                               removeDeletedRecords: Boolean)(implicit ec: ExecutionContext): Future[Iterable[LevelAssignment]] =
    Future.traverse(tasks) {
      task =>
        Future {
          task.target.assign(
            newKeyValues = task.data,
            targetSegments = task.target.segments(),
            removeDeletedRecords = removeDeletedRecords
          )
        }
    }

  /**
   * Assigns input data by looking at the edge (head & last) key-values i.e. without reading the Segment's content.
   */
  def assignQuick[A <: Assignable.Collection](data: Iterable[A],
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
      toKeyValues(data)

    //build an aggregator such that all Assignable assignments are converted back to
    //the input data type. i.e. if input data type was Segment, mutable.SortedSet[A] will be
    //mutable.SortedSet[Segment]

    implicit val creator: Aggregator.Creator[Assignable, mutable.SortedSet[A]] =
      aggregatorCreator(segmentOrder, segmentMapIndex)

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

        //group the assignments that spread
        val groupedAssignments =
          groupAssignmentsForScoring[A, Segment, SegmentAssignment[mutable.SortedSet[A], mutable.SortedSet[A], Segment]](assignments)

        //score assignments.
        val scoredAssignments =
          groupedAssignments.sorted(AssignmentScorer.scorer[A, Segment]())

        //finalise Segments to compact.
        finaliseSegmentsToCompact(
          dataOverflow = dataOverflow,
          scoredAssignments = scoredAssignments
        )
      } else { //else segments can be copied.
        val segmentsToCopy =
          fillOverflow(
            overflow = dataOverflow,
            data = data
          )

        (mutable.SortedSet.empty[A], segmentsToCopy)
      }

    //if there are Segments to merge then create a task.
    if (segmentsToMerge.nonEmpty)
      tasks +=
        CompactionTask.Task(
          data = segmentsToMerge,
          target = nextLevel
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
        val oldTaskIndex = tasks.indexWhere(_.target == targetFirstLevel)

        if (oldTaskIndex < 0) {
          tasks +=
            CompactionTask.Task(
              data = segmentsToCopy,
              target = targetFirstLevel
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
   * Handles cases when a segment can be assigned to multiple target segments (one to many).
   * For those cases those assignments are grouped to form a single [[SegmentAssignment]] that's why
   * resulting type of [[SegmentAssignment]] is a ListBuffer[[B]].
   *
   * IMPORTANT - The resulting assignments should only be used for scoring because on merging two assignments
   * their gaps are simply grouped together so that scoring can be applied for gaps vs overlaps.
   * Eg: if assignment1 has tailGap which overlaps with next Segment's headGap then on more two assignments
   * the tailGap is no-more a Gap but should become an overlapping assignment. But currently this is not
   * account in the logic below therefore should only be used for scoring.
   *
   * @note Mutates the input assignments.
   * @tparam A the type of input data
   * @tparam B the type of target data to which the the [[A]]'s would be assigned to.
   *           This would be most be [[Segment]].
   * @tparam C the type of assignments.
   * @return groups assignments that spread onto multiple Segments.
   */
  def groupAssignmentsForScoring[A, B, C <: SegmentAssignment.Result[mutable.SortedSet[A], mutable.SortedSet[A], B]](assignments: Iterable[C])(implicit inputDataType: CompactionDataType[A],
                                                                                                                                               targetDataType: CompactionDataType[B]): ListBuffer[SegmentAssignment.Result[mutable.SortedSet[A], mutable.SortedSet[A], ListBuffer[B]]] = {
    var previousAssignmentOrNull: SegmentAssignment.Result[mutable.SortedSet[A], mutable.SortedSet[A], B] = null

    val groupedAssignments = ListBuffer.empty[SegmentAssignment.Result[mutable.SortedSet[A], mutable.SortedSet[A], ListBuffer[B]]]

    assignments foreach {
      nextAssignment =>
        val spreads =
          previousAssignmentOrNull != null && {
            //check if previous assignment spreads onto next assignment.
            val previousAssignmentsLast =
              previousAssignmentOrNull.tailGapResult.lastOption
                .orElse(previousAssignmentOrNull.midOverlapResult.lastOption)
                .orElse(previousAssignmentOrNull.headGapResult.lastOption)

            val nextAssignmentsFirst =
              nextAssignment.headGapResult.lastOption
                .orElse(nextAssignment.midOverlapResult.lastOption)
                .orElse(nextAssignment.tailGapResult.lastOption)

            previousAssignmentsLast == nextAssignmentsFirst
          }

        //if the assignment spreads onto the next target segments them merge the assignments into a single assignment for scoring.
        if (spreads) {
          groupedAssignments.last.headGapResult ++= nextAssignment.headGapResult
          groupedAssignments.last.midOverlapResult ++= nextAssignment.midOverlapResult
          groupedAssignments.last.tailGapResult ++= nextAssignment.tailGapResult
        } else {
          groupedAssignments +=
            SegmentAssignmentResult(
              segment = ListBuffer(nextAssignment.segment),
              headGapResult = nextAssignment.headGapResult,
              midOverlapResult = nextAssignment.midOverlapResult,
              tailGapResult = nextAssignment.tailGapResult
            )
        }

        previousAssignmentOrNull = nextAssignment
    }

    groupedAssignments
  }

  /**
   * Takes enough segments to control the data overflow.
   * Prioritise segments that can be merged (overlapping segments) over
   * segments that can be copied.
   */
  def finaliseSegmentsToCompact[A, B](dataOverflow: Long,
                                      scoredAssignments: Iterable[SegmentAssignment.Result[Iterable[A], Iterable[A], B]])(implicit segmentOrdering: Ordering[A],
                                                                                                                          dataType: CompactionDataType[A]): (scala.collection.SortedSet[A], scala.collection.SortedSet[A]) =
    if (dataOverflow <= 0) {
      (mutable.SortedSet.empty, mutable.SortedSet.empty)
    } else {
      var midOverlapTaken = 0L

      //segments that are overlapping and require merging
      val segmentsToMerge = mutable.SortedSet.empty[A]
      //segments that can be copied
      val segmentsToCopy = mutable.SortedSet.empty[A]

      val it = scoredAssignments.iterator
      var break = false //break if overflow is collected.
      while (!break && it.hasNext) {
        val assignment = it.next()
        //collect segments that can be copied
        segmentsToCopy ++= assignment.headGapResult
        segmentsToCopy ++= assignment.tailGapResult

        //all midOverlaps should be merged because they
        //have the same target Segment.
        val midOverlapIterator = assignment.midOverlapResult.iterator
        while (midOverlapIterator.hasNext) {
          val overlap = midOverlapIterator.next()
          segmentsToMerge += overlap
          midOverlapTaken += overlap.segmentSize
        }

        break = midOverlapTaken >= dataOverflow
      }

      val copyable =
        fillOverflow(
          overflow = dataOverflow - midOverlapTaken,
          data = segmentsToCopy
        )

      (segmentsToMerge, copyable)
    }

  def fillOverflow[A](overflow: Long, data: Iterable[A])(implicit dataType: CompactionDataType[A],
                                                         segmentOrdering: Ordering[A]): scala.collection.SortedSet[A] = {
    var remainingOverflow = overflow

    if (remainingOverflow > 0) {
      val fill = mutable.SortedSet.empty[A]
      val it = data.iterator
      var break = false
      while (!break && it.hasNext) {
        val next = it.next()
        fill += next
        remainingOverflow -= next.segmentSize
        break = remainingOverflow <= 0
      }
      fill
    } else {
      mutable.SortedSet.empty
    }
  }
}
