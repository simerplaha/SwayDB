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

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.level.compaction.task.CompactionDataType
import swaydb.core.segment.assigner.AssignmentResult
import swaydb.data.RunThis._

import scala.collection.mutable.ListBuffer

class TaskAssigner_finaliseSegmentsToCompact_Spec extends AnyWordSpec with Matchers with MockFactory {

  /**
   * SCENARIO 1
   *
   * Cases when there is single assignment.
   */
  "single assignment" when {
    val assignment =
      AssignmentResult[ListBuffer[Int], ListBuffer[Int], String](
        segment = "segment",
        headGapResult = ListBuffer(0, 1),
        midOverlapResult = ListBuffer(2, 3, 4),
        tailGapResult = ListBuffer(5, 6, 7)
      )

    "only mid assignment is enough to satisfy overflow" when {
      "no dataOverflow" in {
        //no segment sizes are computed
        implicit val inputDataType = mock[CompactionDataType[Int]]

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 0,
            scoredAssignments = List(assignment)
          )

        toMerge shouldBe empty
        toCopy shouldBe empty
      }

      "EXACTLY - overlapping Segments satisfy the overflow exactly" in {
        implicit val inputDataType = mock[CompactionDataType[Int]]
        //only size of mid Segments is computed.
        (inputDataType.segmentSize _).expects(2) returning 1
        (inputDataType.segmentSize _).expects(3) returning 1
        (inputDataType.segmentSize _).expects(4) returning 1

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 3, //overflow is 3 and midOverlap is also 3
            scoredAssignments = List(assignment)
          )

        //only enough Segments
        toMerge.toList should contain inOrderOnly(2, 3, 4)
        toCopy.toList shouldBe empty
      }

      "FEW - few (not all) of the overlapping Segments satisfy the overflow" in {
        runThis(5.times, log = true) {
          implicit val inputDataType = mock[CompactionDataType[Int]]
          //only size of mid Segments is computed.
          (inputDataType.segmentSize _).expects(2) returning 1
          (inputDataType.segmentSize _).expects(3) returning 1
          (inputDataType.segmentSize _).expects(4) returning 1

          val (toMerge, toCopy) =
            TaskAssigner.finaliseSegmentsToCompact[Int, String](
              dataOverflow = eitherOne(1, 2), //overflow is 1 or 2 and midOverlap size is 3. Still all midOverlaps are taken
              scoredAssignments = List(assignment)
            )

          //only enough Segments
          toMerge.toList should contain inOrderOnly(2, 3, 4)
          toCopy.toList shouldBe empty
        }
      }
    }

    "head and mid assignments are enough to satisfy overflow" when {
      "headGap's head is taken" in {
        implicit val inputDataType = mock[CompactionDataType[Int]]
        (inputDataType.segmentSize _).expects(2) returning 1
        (inputDataType.segmentSize _).expects(3) returning 1
        (inputDataType.segmentSize _).expects(4) returning 1
        (inputDataType.segmentSize _).expects(0) returning 1

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 4,
            scoredAssignments = List(assignment)
          )

        //only enough Segments
        toMerge.toList should contain inOrderOnly(2, 3, 4)
        toCopy.toList should contain only 0
      }

      "headGap is fully taken" in {
        implicit val inputDataType = mock[CompactionDataType[Int]]
        (inputDataType.segmentSize _).expects(2) returning 1
        (inputDataType.segmentSize _).expects(3) returning 1
        (inputDataType.segmentSize _).expects(4) returning 1
        (inputDataType.segmentSize _).expects(0) returning 1
        (inputDataType.segmentSize _).expects(1) returning 1

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 5,
            scoredAssignments = List(assignment)
          )

        //only enough Segments
        toMerge.toList should contain inOrderOnly(2, 3, 4)
        toCopy.toList should contain inOrderOnly(0, 1)
      }
    }

    "all (head, tail mid assignments) needed to satisfy overflow" when {
      "headGap is fully taken and tail gaps head is taken" in {
        implicit val inputDataType = mock[CompactionDataType[Int]]
        (inputDataType.segmentSize _).expects(2) returning 1
        (inputDataType.segmentSize _).expects(3) returning 1
        (inputDataType.segmentSize _).expects(4) returning 1
        (inputDataType.segmentSize _).expects(0) returning 1
        (inputDataType.segmentSize _).expects(1) returning 1
        (inputDataType.segmentSize _).expects(5) returning 1
        (inputDataType.segmentSize _).expects(6) returning 1

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 7,
            scoredAssignments = List(assignment)
          )

        //only enough Segments
        toMerge.toList should contain inOrderOnly(2, 3, 4)
        toCopy.toList should contain inOrderOnly(0, 1, 5, 6)
      }

      "all are taken" in {
        runThis(10.times, log = true) {
          implicit val inputDataType = mock[CompactionDataType[Int]]
          (inputDataType.segmentSize _).expects(2) returning 1
          (inputDataType.segmentSize _).expects(3) returning 1
          (inputDataType.segmentSize _).expects(4) returning 1
          (inputDataType.segmentSize _).expects(0) returning 1
          (inputDataType.segmentSize _).expects(1) returning 1
          (inputDataType.segmentSize _).expects(5) returning 1
          (inputDataType.segmentSize _).expects(6) returning 1
          (inputDataType.segmentSize _).expects(7) returning 1

          val (toMerge, toCopy) =
            TaskAssigner.finaliseSegmentsToCompact[Int, String](
              dataOverflow = eitherOne(8, 10, Long.MaxValue),
              scoredAssignments = List(assignment)
            )

          //only enough Segments
          toMerge.toList should contain inOrderOnly(2, 3, 4)
          toCopy.toList should contain inOrderOnly(0, 1, 5, 6, 7)
        }
      }
    }
  }

  /**
   * SCENARIO 2
   *
   * Cases when there are more than one assignments
   */
  "multi assignment" when {
    val assignment1 =
      AssignmentResult[ListBuffer[Int], ListBuffer[Int], String](
        segment = "segment1",
        headGapResult = ListBuffer(0),
        midOverlapResult = ListBuffer(1),
        tailGapResult = ListBuffer(2)
      )

    val assignment2 =
      AssignmentResult[ListBuffer[Int], ListBuffer[Int], String](
        segment = "segment2",
        headGapResult = ListBuffer(3),
        midOverlapResult = ListBuffer(4),
        tailGapResult = ListBuffer(5)
      )

    val assignment3 =
      AssignmentResult[ListBuffer[Int], ListBuffer[Int], String](
        segment = "segment3",
        headGapResult = ListBuffer(6),
        midOverlapResult = ListBuffer(7),
        tailGapResult = ListBuffer(8)
      )

    val assignments = Seq(assignment1, assignment2, assignment3)

    "only mid assignment is enough to satisfy overflow" when {
      "no dataOverflow" in {
        //no segment sizes are computed
        implicit val inputDataType = mock[CompactionDataType[Int]]

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 0,
            scoredAssignments = assignments
          )

        toMerge shouldBe empty
        toCopy shouldBe empty
      }

      "EXACTLY - mid gaps satisfy the overflow exactly" in {
        implicit val inputDataType = mock[CompactionDataType[Int]]
        //only size of mid Segments is computed.
        (inputDataType.segmentSize _).expects(1) returning 1
        (inputDataType.segmentSize _).expects(4) returning 1
        (inputDataType.segmentSize _).expects(7) returning 1

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 3,
            scoredAssignments = assignments
          )

        //only enough Segments
        toMerge.toList should contain inOrderOnly(1, 4, 7)
        toCopy.toList shouldBe empty
      }

      "FEW - some mid gaps satisfy the overflow" in {
        implicit val inputDataType = mock[CompactionDataType[Int]]
        //only size of mid Segments is computed.
        (inputDataType.segmentSize _).expects(1) returning 1
        (inputDataType.segmentSize _).expects(4) returning 1

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 2,
            scoredAssignments = assignments
          )

        //only enough Segments
        toMerge.toList should contain inOrderOnly(1, 4)
        toCopy.toList shouldBe empty
      }

      "mid gaps are required but also some of the gaps" in {
        implicit val inputDataType = mock[CompactionDataType[Int]]
        //only size of mid Segments is computed.
        (inputDataType.segmentSize _).expects(1) returning 1
        (inputDataType.segmentSize _).expects(4) returning 1
        (inputDataType.segmentSize _).expects(7) returning 1
        (inputDataType.segmentSize _).expects(0) returning 1
        (inputDataType.segmentSize _).expects(2) returning 1

        val (toMerge, toCopy) =
          TaskAssigner.finaliseSegmentsToCompact[Int, String](
            dataOverflow = 5,
            scoredAssignments = assignments
          )

        //only enough Segments
        toMerge.toList should contain inOrderOnly(1, 4, 7)
        toCopy.toList should contain inOrderOnly(0, 2)
      }

      "all are required" in {
        runThis(10.times, log = true) {
          implicit val inputDataType = mock[CompactionDataType[Int]]
          //only size of mid Segments is computed.
          (inputDataType.segmentSize _).expects(1) returning 1
          (inputDataType.segmentSize _).expects(4) returning 1
          (inputDataType.segmentSize _).expects(7) returning 1
          (inputDataType.segmentSize _).expects(0) returning 1
          (inputDataType.segmentSize _).expects(2) returning 1
          (inputDataType.segmentSize _).expects(3) returning 1
          (inputDataType.segmentSize _).expects(5) returning 1
          (inputDataType.segmentSize _).expects(6) returning 1
          (inputDataType.segmentSize _).expects(8) returning 1

          val (toMerge, toCopy) =
            TaskAssigner.finaliseSegmentsToCompact[Int, String](
              dataOverflow = eitherOne(9, 10, Long.MaxValue),
              scoredAssignments = assignments
            )

          //only enough Segments
          toMerge.toList should contain inOrderOnly(1, 4, 7)
          toCopy.toList should contain inOrderOnly(0, 2, 3, 5, 6, 8)
        }
      }
    }
  }
}
