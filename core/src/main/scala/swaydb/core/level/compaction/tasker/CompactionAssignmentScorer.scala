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

package swaydb.core.level.compaction.tasker

import swaydb.core.level.compaction.tasker.CompactionDataType._
import swaydb.core.segment.assigner.SegmentAssignment

case object CompactionAssignmentScorer {

  /**
   * Scores and orders assignments.
   *
   * [[SegmentAssignment]] that have no [[SegmentAssignment.midOverlap]] are always scored lower.
   * Assignments that are reading the same amount of data as the data being written are scored higher.
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
  def scorer[A, B](implicit inputDataTypeA: CompactionDataType[A],
                   inputDataTypeB: CompactionDataType[B]) =
    new Ordering[SegmentAssignment[Iterable[A], Iterable[A], Iterable[B]]] {
      override def compare(left: SegmentAssignment[Iterable[A], Iterable[A], Iterable[B]],
                           right: SegmentAssignment[Iterable[A], Iterable[A], Iterable[B]]): Int = {
        val leftMidSize = left.midOverlap.result.foldLeft(0)(_ + _.segmentSize)
        val leftTargetSegmentSize = left.segment.foldLeft(0)(_ + _.segmentSize)
        val leftDifference = leftMidSize - leftTargetSegmentSize

        val rightMidSize = right.midOverlap.result.foldLeft(0)(_ + _.segmentSize)
        val rightTargetSegmentSize = right.segment.foldLeft(0)(_ + _.segmentSize)
        val rightDifference = rightMidSize - rightTargetSegmentSize

        val compared =
          if (leftMidSize == 0 && rightMidSize == 0)
            0
          else
            leftDifference compare rightDifference

        if (compared == 0) {
          val leftHeadGapSize = left.headGap.result.foldLeft(0)(_ + _.segmentSize)
          val leftTailGapSize = left.tailGap.result.foldLeft(0)(_ + _.segmentSize)
          val leftGapSize = leftHeadGapSize + leftTailGapSize
          val leftDifference = leftGapSize - leftTargetSegmentSize

          val rightHeadGapSize = right.headGap.result.foldLeft(0)(_ + _.segmentSize)
          val rightTailGapSize = right.tailGap.result.foldLeft(0)(_ + _.segmentSize)
          val rightGapSize = rightHeadGapSize + rightTailGapSize
          val rightDifference = rightGapSize - rightTargetSegmentSize

          leftDifference compare rightDifference
        } else {
          compared
        }
      }
    }
}
