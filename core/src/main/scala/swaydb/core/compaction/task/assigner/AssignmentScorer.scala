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

package swaydb.core.compaction.task.assigner

import swaydb.core.compaction.task.CompactionDataType
import swaydb.core.compaction.task.CompactionDataType._
import swaydb.core.segment.assigner.Assignment

protected case object AssignmentScorer {

  /**
   * Scores and orders assignments.
   *
   * First score based on [[Assignment.midOverlap]] and if two assignments result in
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
    new Ordering[Assignment.Result[Iterable[A], Iterable[A], Iterable[B]]] {
      override def compare(left: Assignment.Result[Iterable[A], Iterable[A], Iterable[B]],
                           right: Assignment.Result[Iterable[A], Iterable[A], Iterable[B]]): Int = {
        val leftDifference = difference(left)
        val rightDifference = difference(right)

        (leftDifference compare rightDifference) * -1
      }
    }

  def difference[A, B](assignment: Assignment.Result[Iterable[A], Iterable[A], Iterable[B]])(implicit inputDataType: CompactionDataType[A],
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
