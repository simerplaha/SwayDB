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

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.compaction.task.CompactionDataType
import swaydb.core.segment.assigner.AssignmentResult
import swaydb.testkit.RunThis._

import scala.util.Random

class AssignmentScorerSpec extends AnyWordSpec with Matchers with MockFactory {

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
            AssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(4),
              tailGapResult = List.empty
            )

          //FUNNEL - scores higher because data read closer to data written
          // 5 6
          //0 1 2

          val assignment2 =
            AssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(5, 6),
              tailGapResult = List.empty
            )


          val scored = Random.shuffle(List(assignment1, assignment2)).sorted(AssignmentScorer.scorer[Int, Int]())

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
            AssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(4),
              tailGapResult = List.empty
            )

          //FUNNEL - scores higher because data read closer to data written
          // 5 6
          //0 1 2

          val assignment2 =
            AssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(5, 6),
              tailGapResult = List.empty
            )

          //FUNNEL - scores higher because data read closer to data written
          //  7
          //0 1 2

          val assignment3 =
            AssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(7),
              tailGapResult = List.empty
            )

          val assignments = List(assignment1, assignment2, assignment3)

          val scored = Random.shuffle(assignments).sorted(AssignmentScorer.scorer[Int, Int]())

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
            AssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List(3),
              midOverlapResult = List(4),
              tailGapResult = List.empty
            )

          //FUNNEL - scored lower because there is more different = 2
          //[] - [6] - []
          //  [0, 1, 2]

          val assignment2 =
            AssignmentResult[List[Int], List[Int], List[Int]](
              segment = List(0, 1, 2),
              headGapResult = List.empty,
              midOverlapResult = List(6),
              tailGapResult = List.empty
            )

          val scored = Random.shuffle(List(assignment1, assignment2)).sorted(AssignmentScorer.scorer[Int, Int]())

          scored should have size 2

          //assignment2 has the least difference so it gets scored higher.
          scored.head shouldBe assignment1
          scored.last shouldBe assignment2

        }
      }
    }
  }
}
