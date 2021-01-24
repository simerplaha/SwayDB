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

import swaydb.core.level.compaction.task.CompactionDataType
import swaydb.core.level.compaction.task.CompactionDataType._
import swaydb.core.segment.assigner.SegmentAssignment

protected case object AssignmentScorer {


  /**
   * Scores and orders assignments.
   *
   * First score based on [[SegmentAssignment.midOverlap]] and if two assignments result in
   * the same score then use total of both
   *
   * Assignments can occur in three possible scenarios which are easier imagined as a funnels.
   *
   * Funnel types
   *    - 1. read data is > than target data.
   *    - 2. read data is == than target data.
   *    - 3. read data is < than target data.
   *
   * The ordering will prioritise funnel 2.
   *
   * @formatter:off
   *           1           2         3
   *        \     /      |  |       / \
   *         \   /       |  |      /   \
   *          \ /        |  |     /     \
   * @formatter:on
   */
  def scorer[A, B]()(implicit inputDataType: CompactionDataType[A],
                     targetDataType: CompactionDataType[B]) =
    new Ordering[SegmentAssignment.Result[Iterable[A], Iterable[A], Iterable[B]]] {
      override def compare(left: SegmentAssignment.Result[Iterable[A], Iterable[A], Iterable[B]],
                           right: SegmentAssignment.Result[Iterable[A], Iterable[A], Iterable[B]]): Int = {
        val leftDifference = difference(left)
        val rightDifference = difference(right)

        (leftDifference compare rightDifference) * -1
      }
    }

  def difference[A, B](assignment: SegmentAssignment.Result[Iterable[A], Iterable[A], Iterable[B]])(implicit inputDataType: CompactionDataType[A],
                                                                                                    targetDataType: CompactionDataType[B]) = {
    //total data being read
    val headGapSize = assignment.headGapResult.foldLeft(0)(_ + _.segmentSize)
    val tailGapSize = assignment.tailGapResult.foldLeft(headGapSize)(_ + _.segmentSize)
    val assignedDataSize = assignment.midOverlapResult.foldLeft(tailGapSize)(_ + _.segmentSize)

    //total data being overwritten
    val targetDataSize = assignment.segment.foldLeft(0)(_ + _.segmentSize)

    //difference between data being assigned and target data
    assignedDataSize - targetDataSize
  }
}
