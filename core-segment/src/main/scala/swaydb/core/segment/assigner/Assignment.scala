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

package swaydb.core.segment.assigner

import swaydb.utils.Aggregator

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
