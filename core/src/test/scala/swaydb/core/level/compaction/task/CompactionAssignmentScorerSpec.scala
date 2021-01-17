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

package swaydb.core.level.compaction.task

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.segment.assigner.SegmentAssignmentResult
import swaydb.data.RunThis._

import scala.util.Random

class CompactionAssignmentScorerSpec extends AnyWordSpec with Matchers with MockFactory {

  "scoring" when {
    "NO GAPS" should {
      "Scenario 1" in {
        runThis(10.times, log = true) {
          implicit val dataType = mock[CompactionDataType[Int]]
          (dataType.segmentSize _).expects(*).returns(1).anyNumberOfTimes()

          //FUNNEL - scores lower because data read much greater than data written
          //  4
          //0 1 2

          val assignment1 =
            SegmentAssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(4),
              tailGapResult = List.empty
            )

          //FUNNEL - scores higher because data read closer to data written
          // 5 6
          //0 1 2

          val assignment2 =
            SegmentAssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(5, 6),
              tailGapResult = List.empty
            )


          val scored = Random.shuffle(List(assignment1, assignment2)).sorted(CompactionAssignmentScorer.scorer[Int, Int]())

          scored should have size 2

          //assignment2 has the least difference so it gets scored higher.
          scored.head shouldBe assignment2
          scored.last shouldBe assignment1

        }
      }

      "Scenario 2" in {
        runThis(10.times, log = true) {
          implicit val dataType = mock[CompactionDataType[Int]]
          (dataType.segmentSize _).expects(*).returns(1).anyNumberOfTimes()

          //FUNNEL - scores lower because data read much greater than data written
          //  4
          //0 1 2

          val assignment1 =
            SegmentAssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(4),
              tailGapResult = List.empty
            )

          //FUNNEL - scores higher because data read closer to data written
          // 5 6
          //0 1 2

          val assignment2 =
            SegmentAssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(5, 6),
              tailGapResult = List.empty
            )

          //FUNNEL - scores higher because data read closer to data written
          //  7
          //0 1 2

          val assignment3 =
            SegmentAssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(7),
              tailGapResult = List.empty
            )

          val assignments = List(assignment1, assignment2, assignment3)

          val scored = Random.shuffle(assignments).sorted(CompactionAssignmentScorer.scorer[Int, Int]())

          scored should have size 3

          scored.head shouldBe assignment2
          //1 and 2 have the same score
          scored.drop(1) should contain allOf(assignment1, assignment3)

        }
      }
    }

    "GAPS" should {
      "Scenario 1" in {
        runThis(10.times, log = true) {
          implicit val dataType = mock[CompactionDataType[Int]]
          (dataType.segmentSize _).expects(*).returns(1).anyNumberOfTimes()

          //FUNNEL - scores higher because 2 segments can be assigned to 3 (3 - 2 = 1 difference)
          //[3] - [4] - []
          //   [0, 1, 2]

          val assignment1 =
            SegmentAssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List(3),
              midOverlapResult = List(4),
              tailGapResult = List.empty
            )

          //FUNNEL - scored lower because there is more different = 2
          //[] - [6] - []
          //  [0, 1, 2]

          val assignment2 =
            SegmentAssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(6),
              tailGapResult = List.empty
            )

          val scored = Random.shuffle(List(assignment1, assignment2)).sorted(CompactionAssignmentScorer.scorer[Int, Int]())

          scored should have size 2

          //assignment2 has the least difference so it gets scored higher.
          scored.head shouldBe assignment1
          scored.last shouldBe assignment2

        }
      }
    }
  }
}
