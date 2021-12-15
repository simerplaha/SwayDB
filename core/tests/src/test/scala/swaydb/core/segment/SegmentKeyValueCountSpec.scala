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
//package swaydb.core.segment
//
//import org.scalatest.PrivateMethodTester
//import org.scalatest.concurrent.ScalaFutures
//import swaydb.IOValues._
//import swaydb.config.{MMAP, TestForceSave}
//import swaydb.core.CoreTestData._
//import swaydb.core.{ACoreSpec, TestSweeper}
//import swaydb.core.level.ALevelSpec
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//import swaydb.utils.OperatingSystem
//
//class SegmentKeyValueCount0 extends SegmentKeyValueCount {
//  val keyValuesCount = 1000
//}
//
//class SegmentKeyValueCount1 extends SegmentKeyValueCount {
//  val keyValuesCount = 1000
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class SegmentKeyValueCount2 extends SegmentKeyValueCount {
//  val keyValuesCount = 1000
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
//}
//
//class SegmentKeyValueCount3 extends SegmentKeyValueCount {
//  val keyValuesCount = 10000
//
//  override def isMemorySpec = true
//}
//
//sealed trait SegmentKeyValueCount extends ALevelSpec with ScalaFutures with PrivateMethodTester {
//
//  implicit val keyOrder = KeyOrder.default
//
//  def keyValuesCount: Int
//
//  "Segment.keyValueCount" should {
//
//    "return 1 when the Segment contains only 1 key-value" in {
//      runThis(10.times) {
//        TestSweeper {
//          implicit sweeper =>
//
//            assertSegment(
//              keyValues = randomizedKeyValues(1),
//
//              assert =
//                (keyValues, segment) => {
//                  keyValues should have size 1
//                  segment.keyValueCount.runRandomIO.right.value shouldBe keyValues.size
//                }
//            )
//        }
//      }
//    }
//
//    "return the number of randomly generated key-values where there are no Groups" in {
//      runThis(10.times) {
//        TestSweeper {
//          implicit sweeper =>
//            assertSegment(
//              keyValues = randomizedKeyValues(keyValuesCount),
//
//              assert =
//                (keyValues, segment) => {
//                  segment.keyValueCount.runRandomIO.right.value shouldBe keyValues.size
//                }
//            )
//        }
//      }
//    }
//  }
//}
