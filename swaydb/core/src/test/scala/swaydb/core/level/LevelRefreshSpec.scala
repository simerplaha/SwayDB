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
//import swaydb.IOValues._
//import swaydb.config.MMAP
//import swaydb.core.CoreTestData._
//import swaydb.core._
//import swaydb.core.log.ALogSpec
//import swaydb.core.segment.block.segment.SegmentBlockConfig
//import swaydb.core.segment.data._
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.testkit.RunThis._
//import swaydb.utils.OperatingSystem
//import swaydb.utils.StorageUnits._
//
//import scala.concurrent.duration._
//
//class LevelRefreshSpec0 extends LevelRefreshSpec
//
//class LevelRefreshSpec1 extends LevelRefreshSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class LevelRefreshSpec2 extends LevelRefreshSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
//}
//
//class LevelRefreshSpec3 extends LevelRefreshSpec {
//  override def isMemorySpec = true
//}
//
//sealed trait LevelRefreshSpec extends ALevelSpec with ALogSpec with MockFactory with PrivateMethodTester {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val testTimer: TestTimer = TestTimer.Empty
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit val ec = TestExecutionContext.executionContext
//  val keyValuesCount = 100
//
//  "refresh" should {
//    "remove expired key-values" in {
//      runThis(5.times, log = true) {
//        TestSweeper {
//          implicit sweeper =>
//            import sweeper._
//
//            val level = TestLevel(segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.byte, mmap = mmapSegments))
//            val keyValues = randomPutKeyValues(1000, valueSize = 0, startId = Some(0))(TestTimer.Empty)
//            level.put(keyValues).runRandomIO.right.value
//            //dispatch another put request so that existing Segment gets split
//            level.put(Slice(keyValues.head)).runRandomIO.right.value
//            level.segmentsCount() should be >= 1
//
//            //expire all key-values
//            level.put(Slice(Memory.Range(0, Int.MaxValue, Value.FromValue.Null, Value.Remove(Some(2.seconds.fromNow), Time.empty)))).runRandomIO.right.value
//            level.segmentFilesInAppendix should be > 1
//
//            sleep(3.seconds)
//
//            //refresh multiple times if necessary to cover cases if throttle only allows
//            //small number of segments to be refreshed at a time.
//            var attempts = 20
//            while (level.segmentFilesInAppendix != 0 && attempts > 0) {
//              val segments = level.segments()
//              val compact = level.refresh(segments, removeDeletedRecords = false).await
//              level.commit(compact) shouldBe IO.unit
//              attempts -= 1
//            }
//
//            if (isWindowsAndMMAPSegments())
//              eventual(10.seconds) {
//                sweeper.receiveAll()
//                level.segmentFilesInAppendix shouldBe 0
//              }
//            else
//              level.segmentFilesInAppendix shouldBe 0
//        }
//      }
//    }
//
//    "update createdInLevel" in {
//      TestSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          val level = TestLevel(segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, mmap = mmapSegments))
//
//          val keyValues = randomPutKeyValues(keyValuesCount, addExpiredPutDeadlines = false)
//          val map = TestLog(keyValues)
//          level.putMap(map).right.right.value
//
//          val nextLevel = TestLevel()
//          nextLevel.putSegments(level.segments()) shouldBe IO.unit
//
//          nextLevel.segments() foreach {
//            segment =>
//              val compactResult = nextLevel.refresh(Seq(segment), removeDeletedRecords = false).await
//              nextLevel.commit(compactResult) shouldBe IO.unit
//          }
//          nextLevel.segments() foreach (_.createdInLevel shouldBe nextLevel.levelNumber)
//      }
//    }
//  }
//}
