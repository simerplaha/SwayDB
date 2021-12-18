///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core.compaction.task.assigner
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.matchers.should.Matchers._
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.compaction.task.CompactionDataType
//import swaydb.core.segment.assigner.AssignmentResult
//
//import scala.collection.mutable
//
//class TaskAssigner_groupAssignments_Spec extends AnyWordSpec {
//
//  "not join assignments" when {
//    "there is no spread" in {
//      //no segment sizes are computed
//      implicit val dataType = mock[CompactionDataType[Int]]
//      (dataType.segmentSize _).expects(*).returns(1).anyNumberOfTimes()
//
//      //1, 2, 3
//      //   2
//
//      val assignment1 =
//        AssignmentResult(
//          segment = 2,
//          headGapResult = List(1),
//          midOverlapResult = List(2),
//          tailGapResult = List(3)
//        )
//
//      //4, 5, 6
//      //   3
//
//      val assignment2 =
//        AssignmentResult(
//          segment = 3,
//          headGapResult = List(4),
//          midOverlapResult = List(5),
//          tailGapResult = List(6)
//        )
//
//      val (toMerge, toCopy) =
//        TaskAssigner.finaliseSegmentsToCompact[Int, Int](
//          dataOverflow = Long.MaxValue,
//          scoredAssignments = List(assignment1, assignment2)
//        )
//
//      toMerge.toList should contain allOf(2, 5)
//      toCopy.toList should contain allOf(1, 3, 4, 6)
//    }
//  }
//
//  "join assignments" when {
//    "there is a spread" in {
//      //no segment sizes are computed
//      implicit val dataType = mock[CompactionDataType[Int]]
//      (dataType.segmentSize _).expects(*).returns(1).anyNumberOfTimes()
//
//      //1, 2, 3
//      //   2
//
//      val assignment1: AssignmentResult[mutable.SortedSet[Int], mutable.SortedSet[Int], Int] =
//        AssignmentResult(
//          segment = 2,
//          headGapResult = mutable.SortedSet(1),
//          midOverlapResult = mutable.SortedSet(2),
//          tailGapResult = mutable.SortedSet(3)
//        )
//
//      //3, 4, 5
//      //   3
//
//      val assignment2: AssignmentResult[mutable.SortedSet[Int], mutable.SortedSet[Int], Int] =
//        AssignmentResult(
//          segment = 3,
//          headGapResult = mutable.SortedSet(3),
//          midOverlapResult = mutable.SortedSet(4),
//          tailGapResult = mutable.SortedSet(5)
//        )
//
//      val groups =
//        TaskAssigner.groupAssignmentsForScoring[Int, Int, AssignmentResult[mutable.SortedSet[Int], mutable.SortedSet[Int], Int]](List(assignment1, assignment2))
//
//      groups should have size 1
//      val group = groups.head
//
//      /**
//       * These assignments not actually correct but good enough for scoring
//       * which is the only use-case here.
//       *
//       * Eg: the tailHap 3 is not a gap anymore and should be midOverlap with the second Segment.
//       */
//      group.headGapResult.toList shouldBe List(1, 3)
//      group.midOverlapResult.toList shouldBe List(2, 4)
//      group.tailGapResult.toList shouldBe List(3, 5)
//    }
//  }
//}
