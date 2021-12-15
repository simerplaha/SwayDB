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
//package swaydb.core.level
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.PrivateMethodTester
//import swaydb.IO
//import swaydb.config.{MMAP, TestForceSave}
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestSweeper._
//import swaydb.core.CoreTestData._
//import swaydb.core._
//import swaydb.core.log.ALogSpec
//import swaydb.core.segment.io.SegmentCompactionIO
//import swaydb.core.segment.data.Value.FromValue
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.testkit.RunThis._
//import swaydb.utils.OperatingSystem
//
//class LevelMergeSpec0 extends LevelMergeSpec
//
//class LevelMergeSpec1 extends LevelMergeSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class LevelMergeSpec2 extends LevelMergeSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
//}
//
//class LevelMergeSpec3 extends LevelMergeSpec {
//  override def isMemorySpec = true
//}
//
//sealed trait LevelMergeSpec extends ALevelSpec with MockFactory with PrivateMethodTester {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val testTimer: TestTimer = TestTimer.Empty
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit val ec = TestExecutionContext.executionContext
//  val keyValuesCount = 100
//
//  "level is empty" should {
//    "write put key-values" in {
//      runThis(10.times, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//            //create a Level
//            val level = TestLevel()
//
//            //create put key-values so they do not get cleared.
//            val keyValues = randomPutKeyValues(startId = Some(0))
//            val segment = TestSegment(keyValues)
//
//            //level is empty
//            level.isEmpty shouldBe true
//            assertEmpty(keyValues, level)
//
//            //assign
//            val assignmentResult = level.assign(segment, level.segments(), removeDeletedRecords = false)
//
//            implicit val compactionActor: SegmentCompactionIO.Actor =
//              SegmentCompactionIO.create().sweep()
//
//            //merge
//            val compactionResult = level.merge(assignmentResult, removeDeletedRecords = false).await
//            compactionResult should have size 1
//
//            //level is still empty because nothing is committed.
//            level.isEmpty shouldBe true
//
//            //original segment does not get deleted
//            segment.existsOnDiskOrMemory() shouldBe true
//
//            //commit
//            level.commitPersisted(compactionResult) shouldBe IO.unit
//
//            level.isEmpty shouldBe false
//            assertReads(keyValues, level)
//        }
//      }
//    }
//
//    "cleared update key-values" in {
//      runThis(10.times, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//            val level = TestLevel()
//
//            //create put key-values so they do not get cleared.
//            val keyValues = Slice(randomUpdateKeyValue(1), randomRangeKeyValue(2, 10, FromValue.Null), randomFunctionKeyValue(10))
//            val segment = TestSegment(keyValues)
//
//            //level is empty
//            level.isEmpty shouldBe true
//
//            //assign
//            val assignmentResult = level.assign(segment, level.segments(), removeDeletedRecords = false)
//
//            implicit val compactionActor: SegmentCompactionIO.Actor =
//              SegmentCompactionIO.create().sweep()
//
//            //merge
//            val compactionResult = level.merge(assignmentResult, removeDeletedRecords = false).awaitInf
//            compactionResult should have size 1
//
//            //level is still empty because nothing is committed.
//            level.isEmpty shouldBe true
//
//            //original segment does not get deleted
//            segment.existsOnDiskOrMemory() shouldBe true
//
//            //commit
//            level.commitPersisted(compactionResult) shouldBe IO.unit
//
//            level.isEmpty shouldBe true
//        }
//      }
//    }
//  }
//
//  "level is non empty" should {
//    "overwrite existing key-values" in new ALogSpec {
//      runThis(10.times, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//            val level = TestLevel()
//
//            //create put key-values so they do not get cleared.
//            val keyValues = randomPutKeyValues(startId = Some(0))
//            val segment = TestSegment(keyValues)
//
//            //assign
//            val segmentAssignment = level.assign(segment, level.segments(), removeDeletedRecords = false)
//
//            implicit val compactionActor: SegmentCompactionIO.Actor =
//              SegmentCompactionIO.create().sweep()
//
//            val segmentMergeResult = level.merge(segmentAssignment, removeDeletedRecords = false).awaitInf
//            level.commitPersisted(segmentMergeResult) shouldBe IO.unit
//
//            assertGet(keyValues, level)
//
//            val map = TestLog(keyValues)
//
//            val mapAssignment = level.assign(map, level.segments(), removeDeletedRecords = false)
//
//            val mapMergeResult = level.merge(mapAssignment, removeDeletedRecords = false).awaitInf
//            level.commitPersisted(mapMergeResult) shouldBe IO.unit
//
//            assertGet(keyValues, level)
//
//        }
//      }
//    }
//
//    "merge new key-values" in new ALevelSpec with ALogSpec {
//      runThis(10.times, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//            val level = TestLevel()
//
//            /**
//             * FIRST with 100 key-values
//             */
//            val keyValues1 = randomPutKeyValues(startId = Some(0), count = 100)
//            val segment = TestSegment(keyValues1)
//
//            val segmentAssigment = level.assign(segment, level.segments(), removeDeletedRecords = false)
//
//            implicit val compactionActor: SegmentCompactionIO.Actor =
//              SegmentCompactionIO.create().sweep()
//
//            val segmentMergeResult = level.merge(segmentAssigment, removeDeletedRecords = false).awaitInf
//            level.commitPersisted(segmentMergeResult) shouldBe IO.unit
//
//            assertGet(keyValues1, level)
//
//            val oldSegments = level.segments().toList //toList so that it's immutable
//            oldSegments.foreach(_.existsOnDiskOrMemory() shouldBe true)
//
//            /**
//             * SECOND with 200 key-values
//             */
//            val keyValues2 = randomPutKeyValues(startId = Some(0), count = 200)
//
//            val map = TestLog(keyValues2)
//
//            val mapAssigment = level.assign(map, level.segments(), removeDeletedRecords = false)
//
//            val mapMergeResult = level.merge(mapAssigment, removeDeletedRecords = false).awaitInf
//            level.commitPersisted(mapMergeResult) shouldBe IO.unit
//
//            assertGet(keyValues2, level)
//
//            oldSegments.foreach(_.existsOnDiskOrMemory() shouldBe false)
//
//        }
//      }
//    }
//
//    "seconds merge clears all existing key-values" in new ALogSpec {
//      runThis(10.times, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//            val level = TestLevel()
//
//            /**
//             * FIRST with 100 key-values
//             */
//            val keyValues1 = randomPutKeyValues(startId = Some(0), count = 100)
//            val segment = TestSegment(keyValues1)
//
//            val segmentAssigment = level.assign(segment, level.segments(), removeDeletedRecords = false)
//
//            implicit val compactionActor: SegmentCompactionIO.Actor =
//              SegmentCompactionIO.create().sweep()
//
//            val segmentMergeResult = level.merge(segmentAssigment, removeDeletedRecords = false).awaitInf
//            level.commitPersisted(segmentMergeResult) shouldBe IO.unit
//
//            assertGet(keyValues1, level)
//
//            val oldSegments = level.segments().toList //toList so that it's immutable
//            oldSegments.foreach(_.existsOnDiskOrMemory() shouldBe true)
//
//            /**
//             * SECOND with 200 key-values
//             */
//            val map = TestLog(Slice(randomRemoveRange(0, Int.MaxValue)))
//
//            val mapAssigment = level.assign(map, level.segments(), removeDeletedRecords = false)
//
//            val mapMergeResult = level.merge(mapAssigment, removeDeletedRecords = false).awaitInf
//            level.commitPersisted(mapMergeResult) shouldBe IO.unit
//
//            level.isEmpty shouldBe true
//
//            oldSegments.foreach(_.existsOnDiskOrMemory() shouldBe false)
//        }
//      }
//    }
//  }
//}
