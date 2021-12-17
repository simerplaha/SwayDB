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
//import org.scalatest.OptionValues._
//import swaydb.IO
//import swaydb.config.MMAP
//import swaydb.core.CommonAssertions._
//import swaydb.core.CoreTestData._
//import swaydb.core.segment.Segment
//import swaydb.core.segment.block.segment.SegmentBlockConfig
//import swaydb.core.segment.data.Memory
//import swaydb.core.{ACoreSpec, TestSweeper, TestForceSave, TestTimer}
//import swaydb.core.level.ALevelSpec
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.utils.OperatingSystem
//import swaydb.utils.StorageUnits._
//
//import scala.concurrent.duration._
//import swaydb.testkit.TestKit._
//
//class LevelTaskAssignerSpec0 extends LevelTaskAssignerSpec
//
//class LevelTaskAssignerSpec1 extends LevelTaskAssignerSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class LevelTaskAssignerSpec2 extends LevelTaskAssignerSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
//}
//
//class LevelTaskAssignerSpec3 extends LevelTaskAssignerSpec {
//  override def isMemorySpec = true
//}
//
//sealed trait LevelTaskAssignerSpec extends ALevelSpec with MockFactory {
//
//  implicit val timer = TestTimer.Empty
//  implicit val keyOrder = KeyOrder.default
//  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
//
//  "refresh" when {
//    "Level is empty" in {
//      TestSweeper {
//        implicit sweeper =>
//          val level = TestLevel()
//          LevelTaskAssigner.refresh(level) shouldBe empty
//      }
//    }
//
//    "Level is non-empty but no deadline key-values" in {
//      TestSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          val level = TestLevel()
//          level.put(Slice(Memory.put(1))) shouldBe IO.unit
//
//          LevelTaskAssigner.refresh(level) shouldBe empty
//      }
//    }
//
//    "Level has unexpired key-values" in {
//      TestSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          val level = TestLevel()
//          level.put(Slice(Memory.put(1, Slice.Null, 1.minute.fromNow))) shouldBe IO.unit
//
//          LevelTaskAssigner.refresh(level) shouldBe empty
//      }
//    }
//
//    "Level has expired key-values" in {
//      TestSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          val level = TestLevel()
//          level.put(Slice(Memory.put(1, Slice.Null, expiredDeadline()))) shouldBe IO.unit
//
//          val task = LevelTaskAssigner.refresh(level).value
//          task.source shouldBe level
//          task.segments shouldBe level.segments()
//      }
//    }
//  }
//
//  "collapse" when {
//    "Level is empty" in {
//      TestSweeper {
//        implicit sweeper =>
//          val level = TestLevel()
//          LevelTaskAssigner.collapse(level) shouldBe empty
//      }
//    }
//
//    "Level is non-empty and contains only one Segment" in {
//      TestSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          val level = TestLevel()
//          level.put(Slice(Memory.put(1))) shouldBe IO.unit
//
//          val task = LevelTaskAssigner.collapse(level).value
//          task.source shouldBe level
//          task.segments shouldBe level.segments()
//      }
//    }
//
//    "Level has unexpired key-values" in {
//      if (isMemorySpec)
//        cancel("Test not required for in-memory")
//      else
//        TestSweeper {
//          implicit sweeper =>
//            import sweeper._
//
//            val level = TestLevel(segmentConfig = SegmentBlockConfig.random2(deleteDelay = Duration.Zero, mmap = mmapSegments, minSegmentSize = 1.byte))
//            level.put(Slice(Memory.put(1), Memory.put(2), Memory.put(3), Memory.put(4), Memory.put(5), Memory.put(6))) shouldBe IO.unit
//
//            val reopened = level.reopen(segmentSize = Int.MaxValue)
//
//            val task = LevelTaskAssigner.collapse(reopened).value
//            task.source shouldBe level
//            task.segments shouldBe reopened.segments()
//        }
//    }
//  }
//}
