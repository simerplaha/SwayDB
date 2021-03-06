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

package swaydb.core.segment.assigner

import swaydb.Aggregator

object Assignment {

  sealed trait Result[+GAP, +MID, +SEG] {
    def segment: SEG
    def headGapResult: GAP
    def midOverlapResult: MID
    def tailGapResult: GAP
  }
}

/**
 * Stores assign gap and mergeable (overlapping) key-value of a Segment.
 *
 * @param segment    Segment this assignment belongs to.
 * @param headGap    Head key-values that can be added directly to the Segment without merge.
 * @param midOverlap Overlapping key-values that require merge with that target [[segment]]'s
 *                   key-values.
 * @param tailGap    Tail key-values that can be added directly to the Segment without merge.
 * @tparam GAP [[Aggregator]]'s result type that will store all gap key-values.
 * @tparam MID [[Aggregator]]'s result type that will store all mergeable/overlapping key-values.
 * @tparam SEG Target Segment to which key-values should be assigned to.
 *             This can be a [[swaydb.core.segment.Segment]] or [[swaydb.core.segment.ref.SegmentRef]].
 */

case class Assignment[+GAP, +MID, +SEG](segment: SEG,
                                        headGap: Aggregator[Assignable, GAP],
                                        midOverlap: Aggregator[Assignable, MID],
                                        tailGap: Aggregator[Assignable, GAP]) extends Assignment.Result[GAP, MID, SEG] {
  final override def headGapResult: GAP = headGap.result
  final override def midOverlapResult: MID = midOverlap.result
  final override def tailGapResult: GAP = tailGap.result

  def result: Assignment.Result[GAP, MID, SEG] =
    this
}

case class AssignmentResult[+GAP, +MID, +SEG](segment: SEG,
                                              headGapResult: GAP,
                                              midOverlapResult: MID,
                                              tailGapResult: GAP) extends Assignment.Result[GAP, MID, SEG]
