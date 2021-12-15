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
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpec
//import swaydb.core.compaction.task.CompactionDataType
//
//import scala.collection.SortedSet
//
//class TaskAssigner_fillOnOverflow_Spec extends AnyWordSpec with Matchers with MockFactory {
//
//  "fillOverflow" when {
//    "no overflow" in {
//      implicit val dataType = mock[CompactionDataType[Int]] //never invoked
//      TaskAssigner.fillOverflow[Int](0, List(1, 2)) shouldBe empty
//    }
//
//    "half overflow" in {
//      implicit val dataType = mock[CompactionDataType[Int]]
//      (dataType.segmentSize _).expects(1) returning 1
//
//      TaskAssigner.fillOverflow[Int](1, List(1, 2)) shouldBe SortedSet(1)
//    }
//
//    "full overflow" in {
//      implicit val dataType = mock[CompactionDataType[Int]]
//      (dataType.segmentSize _).expects(1) returning 1
//      (dataType.segmentSize _).expects(2) returning 1
//
//      //3 does not get added
//      TaskAssigner.fillOverflow[Int](2, List(1, 2, 3)) shouldBe SortedSet(1, 2)
//    }
//
//    "inconsistent sizes" in {
//      implicit val dataType = mock[CompactionDataType[Int]]
//      //one has size 2
//      (dataType.segmentSize _).expects(1) returning 2
//      //two has size 3
//      (dataType.segmentSize _).expects(2) returning 3
//
//      //overflow is 3 so 1 does not satisfy there 2 will also get added but 3 never does.
//      TaskAssigner.fillOverflow[Int](3, List(1, 2, 3)) shouldBe SortedSet(1, 2)
//    }
//  }
//}
