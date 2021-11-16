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

package swaydb.core.level.compaction.task.assigner

import swaydb.core.data.Value.FromValue
import swaydb.core.data.{Memory, Time, Value}
import swaydb.core.level.Level
import swaydb.core.level.compaction.task.CompactionDataType._
import swaydb.core.level.compaction.task.{CompactionDataType, CompactionTask}
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, Assigner, Assignment, AssignmentResult}
import swaydb.slice.MaxKey
import swaydb.data.compaction.PushStrategy
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.utils.{Aggregator, NonEmptyList}

import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Builds optimal compaction tasks to perform that meet the configured
 * [[swaydb.data.compaction.LevelThrottle]] criteria.
 */
protected case object TaskAssigner {

  /**
   * Quick assigns input data by looking at the edge (head & last) key-values i.e. without reading the Segment's content.
   */
  def assignQuick[A <: Assignable.Collection](data: Iterable[A],
                                              lowerLevels: NonEmptyList[Level],
                                              dataOverflow: Long,
                                              pushStrategy: PushStrategy)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                          dataType: CompactionDataType[A]): Iterable[CompactionTask.Task[A]] = {
    val tasks = ListBuffer.empty[CompactionTask.Task[A]]

    buildTasks(
      data = data,
      copyToLevel = lowerLevels.head,
      lowerLevels = lowerLevels,
      dataOverflow = dataOverflow,
      pushStrategy = pushStrategy,
      tasks = tasks
    )

    tasks
  }

  /**
   * Assigns the input data to levels that have overlapping key-values.
   * If there were no overlapping segments then it assigns data
   * to first encountered non-overlapping Level for copying.
   *
   * @param data         New data which requires merging/assignment to an optimal
   *                     level.
   * @param copyToLevel  Level to with data can be copied to.
   * @param lowerLevels  Remaining levels.
   * @param dataOverflow Sets the total size of data to compact. This is dictated
   *                     by the [[swaydb.data.compaction.LevelThrottle]] configuration.
   * @param tasks        Final [[CompactionTask.Task]] that will contain optimal
   *                     assignments.
   * @tparam A the type of input data.
   */
  @tailrec
  private def buildTasks[A <: Assignable.Collection](data: Iterable[A],
                                                     copyToLevel: Level,
                                                     lowerLevels: NonEmptyList[Level],
                                                     dataOverflow: Long,
                                                     pushStrategy: PushStrategy,
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
    val thisLevel = lowerLevels.head

    //performing assignment and scoring and fetch Segments that can be merged and segments that can be copied.
    val (segmentsToMerge, segmentsToCopy) =
      if (thisLevel.isNonEmpty()) { //if next Level is not empty then do assignment
        val assignments: ListBuffer[Assignment[mutable.SortedSet[A], mutable.SortedSet[A], Segment]] =
          Assigner.assignUnsafeGaps[mutable.SortedSet[A], mutable.SortedSet[A], Segment](
            keyValues = segmentKeyValues.iterator,
            segments = thisLevel.segments().iterator,
            initialiseIteratorsInOneSeek = false
          )

        //group the assignments that spread
        val groupedAssignments =
          groupAssignmentsForScoring[A, Segment, Assignment[mutable.SortedSet[A], mutable.SortedSet[A], Segment]](assignments)

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
          target = thisLevel
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
          copyToLevel = if (pushStrategy.immediately) thisLevel else copyToLevel,
          lowerLevels = NonEmptyList(lowerLevels.tail.head, lowerLevels.tail.drop(1)),
          dataOverflow = Long.MaxValue, //all Segments should be merged to set overflow to be maximum.
          pushStrategy = pushStrategy,
          tasks = tasks
        )
      } else { //there were not lower levels
        //find the first level that this data could've be copied into
        //and assign this data to that level.

        val copyableLevel =
          if (pushStrategy.immediately) //segment can be copied into this Level so write to this Level
            thisLevel
          else
            copyToLevel

        val oldTaskIndex = tasks.indexWhere(_.target == copyableLevel)

        if (oldTaskIndex < 0) {
          tasks +=
            CompactionTask.Task(
              data = segmentsToCopy,
              target = copyableLevel
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
   * which can then be used by [[Assigner]] for assignments.
   *
   * Each [[Memory]] stores a unique Int value which can be read to fetch the
   * input data type from the returned [[collection.Map]].
   */
  def toKeyValues[A <: Assignable.Collection](data: IterableOnce[A]): (ListBuffer[Memory], collection.Map[Int, A]) = {
    val segmentKeyValues = ListBuffer.empty[Memory]
    val segmentIndex = mutable.Map.empty[Int, A]
    var index = 0

    val dataIterator = data.iterator

    while (dataIterator.hasNext) {
      val segment = dataIterator.next()
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
   * An aggregator which converts [[Assigner]]'s assignments back to the original
   * input data type.
   */
  def aggregatorCreator[A](implicit ordering: Ordering[A],
                           index: collection.Map[Int, A]): Aggregator.Creator[Assignable, mutable.SortedSet[A]] =
    () =>
      new Aggregator[Assignable, mutable.SortedSet[A]] {
        val segments = mutable.SortedSet.empty[A]

        override def addOne(item: Assignable): this.type =
          item match {
            case Memory.Put(_, value, _, _) =>
              segments += index(value.getC.readUnsignedInt())
              this

            case Memory.Range(_, _, _, Value.Update(value, _, _)) =>
              segments += index(value.getC.readUnsignedInt())
              this

            case assignable =>
              throw new Exception(s"Invalid item. ${assignable.getClass.getName}")
          }

        override def addAll(items: IterableOnce[Assignable]): this.type = {
          items foreach this.addOne
          this
        }

        override def result: mutable.SortedSet[A] =
          segments
      }

  /**
   * Handles cases when a segment can be assigned to multiple target segments (one to many).
   * For those cases those assignments are grouped to form a single [[Assignment]] that's why
   * resulting type of [[Assignment]] is a ListBuffer[[B]].
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
  def groupAssignmentsForScoring[A, B, C <: Assignment.Result[mutable.SortedSet[A], mutable.SortedSet[A], B]](assignments: Iterable[C])(implicit inputDataType: CompactionDataType[A],
                                                                                                                                        targetDataType: CompactionDataType[B]): ListBuffer[Assignment.Result[mutable.SortedSet[A], mutable.SortedSet[A], ListBuffer[B]]] = {
    var previousAssignmentOrNull: Assignment.Result[mutable.SortedSet[A], mutable.SortedSet[A], B] = null

    val groupedAssignments = ListBuffer.empty[Assignment.Result[mutable.SortedSet[A], mutable.SortedSet[A], ListBuffer[B]]]

    val assignmentsIterator = assignments.iterator

    while (assignmentsIterator.hasNext) {
      val nextAssignment = assignmentsIterator.next()

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
          AssignmentResult(
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
                                      scoredAssignments: Iterable[Assignment.Result[Iterable[A], Iterable[A], B]])(implicit segmentOrdering: Ordering[A],
                                                                                                                   dataType: CompactionDataType[A]): (scala.collection.SortedSet[A], scala.collection.SortedSet[A]) =
    if (dataOverflow <= 0) {
      (scala.collection.SortedSet.empty, scala.collection.SortedSet.empty)
    } else {
      var midOverlapTaken = 0L

      //segments that are overlapping and require merging
      val segmentsToMerge = mutable.SortedSet.empty[A]
      //segments that can be copied
      val segmentsToCopy = mutable.SortedSet.empty[A]

      val scoredAssignmentsIterator = scoredAssignments.iterator
      var break = false //break if overflow is collected.
      while (!break && scoredAssignmentsIterator.hasNext) {
        val assignment = scoredAssignmentsIterator.next()
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
        fillOverflow[A](
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
      val dataIterator = data.iterator
      var break = false
      while (!break && dataIterator.hasNext) {
        val next = dataIterator.next()
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
