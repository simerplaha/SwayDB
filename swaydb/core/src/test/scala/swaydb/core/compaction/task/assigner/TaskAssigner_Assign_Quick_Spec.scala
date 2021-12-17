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
//import swaydb.config.MMAP
//import swaydb.config.compaction.PushStrategy
//import swaydb.core.CoreTestData._
//import swaydb.core.compaction.task.CompactionTask
//import swaydb.core.level.{ALevelSpec, Level}
//import swaydb.core.segment.Segment
//import swaydb.core.segment.data.Memory
//import swaydb.core.{ACoreSpec, CoreTestSweeper, TestForceSave, TestTimer}
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.utils.{NonEmptyList, OperatingSystem}
//
//import scala.collection.SortedSet
//
//class TaskAssigner_run_Spec0 extends TaskAssigner_run_Spec
//
//class TaskAssigner_run_Spec1 extends TaskAssigner_run_Spec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class TaskAssigner_run_Spec2 extends TaskAssigner_run_Spec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
//}
//
//class TaskAssigner_run_Spec3 extends TaskAssigner_run_Spec {
//  override def isMemorySpec = true
//}
//
//sealed trait TaskAssigner_run_Spec extends AnyWordSpec {
//
//  implicit val timer = TestTimer.Empty
//  implicit val keyOrder = KeyOrder.default
//  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
//
//  "Level is empty" when {
//    def runTest(pushStrategy: PushStrategy): Unit =
//      CoreTestSweeper {
//        implicit sweeper =>
//          val segment = TestSegment(Slice(Memory.put(1)))
//          val level = TestLevel()
//
//          val tasks =
//            TaskAssigner.assignQuick(
//              data = Slice(segment),
//              lowerLevels = NonEmptyList(level),
//              dataOverflow = Int.MaxValue,
//              pushStrategy = pushStrategy
//            )
//
//          tasks should contain only
//            CompactionTask.Task(
//              target = level,
//              data = SortedSet(segment)
//            )
//      }
//
//    "PushStrategy.OnOverflow" in {
//      runTest(PushStrategy.OnOverflow)
//    }
//
//    "PushStrategy.Immediately" in {
//      runTest(PushStrategy.Immediately)
//    }
//  }
//
//  "all levels are empty" when {
//    def setupTest(pushStrategy: PushStrategy)(implicit sweeper: CoreTestSweeper) = {
//      val segment = TestSegment(Slice(Memory.put(1)))
//      //multiple nested Levels but
//      val level = TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel()))))))
//
//      val tasks =
//        TaskAssigner.assignQuick(
//          data = Slice(segment),
//          lowerLevels = NonEmptyList(level, level.nextLevels.map(_.asInstanceOf[Level])),
//          dataOverflow = Int.MaxValue,
//          pushStrategy = pushStrategy
//        )
//
//      (tasks, level, segment)
//    }
//
//    "PushStrategy.OnOverflow" in {
//      CoreTestSweeper {
//        implicit sweeper: CoreTestSweeper =>
//
//          val (tasks, level, segment) = setupTest(PushStrategy.OnOverflow)
//
//          tasks should contain only
//            CompactionTask.Task(
//              target = level,
//              data = SortedSet(segment)
//            )
//
//          tasks.head.target.levelNumber shouldBe level.levelNumber
//      }
//    }
//
//    "PushStrategy.Immediately" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val (tasks, level, segment) = setupTest(PushStrategy.Immediately)
//
//          val lastLevel = level.nextLevels.last.asInstanceOf[Level]
//
//          lastLevel.levelNumber should be > level.levelNumber
//
//          tasks should contain only
//            CompactionTask.Task(
//              target = lastLevel,
//              data = SortedSet(segment)
//            )
//
//          tasks.head.target.levelNumber shouldBe lastLevel.levelNumber
//      }
//    }
//  }
//
//  "parent levels are empty but last Level has OVERLAPPING key-values" when {
//    def runTest(pushStrategy: PushStrategy)(implicit sweeper: CoreTestSweeper) = {
//      //segment gets assigned to last Level with overlapping key-values.
//      val segment = TestSegment(Slice(Memory.put(1)))
//      //multiple nested Levels but last level is non-empty with the same key-values as the segment
//      val lastLevel = TestLevel(keyValues = Slice(Memory.put(1)))
//      val level = TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(lastLevel))))))
//
//      val tasks =
//        TaskAssigner.assignQuick(
//          data = Slice(segment),
//          lowerLevels = NonEmptyList(level, level.nextLevels.map(_.asInstanceOf[Level])),
//          dataOverflow = Int.MaxValue,
//          pushStrategy = pushStrategy
//        )
//
//      tasks should contain only
//        CompactionTask.Task(
//          target = lastLevel,
//          data = SortedSet(segment)
//        )
//
//      tasks.head.target.levelNumber shouldBe lastLevel.levelNumber
//    }
//
//    "PushStrategy.OnOverflow" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          runTest(PushStrategy.OnOverflow)
//      }
//    }
//
//    "PushStrategy.Immediately" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          runTest(PushStrategy.Immediately)
//      }
//    }
//  }
//
//  "parent levels are empty but last Level has NON-OVERLAPPING key-values" when {
//    def setupTest(pushStrategy: PushStrategy)(implicit sweeper: CoreTestSweeper) = {
//      //segment gets assigned to last Level with overlapping key-values.
//      val segment = TestSegment(Slice(Memory.put(1)))
//      //multiple nested Levels but last level is non-empty and does not overlap the Segment's key-values
//      val level = TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(keyValues = Slice(Memory.put(2)))))))))
//
//      val tasks =
//        TaskAssigner.assignQuick(
//          data = Slice(segment),
//          lowerLevels = NonEmptyList(level, level.nextLevels.map(_.asInstanceOf[Level])),
//          dataOverflow = Int.MaxValue,
//          pushStrategy = pushStrategy
//        )
//
//      (tasks, level, segment)
//    }
//
//    "PushStrategy.OnOverflow" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val (tasks, level, segment) = setupTest(PushStrategy.OnOverflow)
//
//          tasks should contain only
//            CompactionTask.Task(
//              target = level,
//              data = SortedSet(segment)
//            )
//
//          tasks.head.target.levelNumber shouldBe level.levelNumber
//      }
//    }
//
//    "PushStrategy.Immediately" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val (tasks, level, segment) = setupTest(PushStrategy.Immediately)
//
//          val lastLevel = level.nextLevels.last.asInstanceOf[Level]
//
//          lastLevel.levelNumber should be > level.levelNumber
//
//          tasks should contain only
//            CompactionTask.Task(
//              target = lastLevel,
//              data = SortedSet(segment)
//            )
//
//          tasks.head.target.levelNumber shouldBe lastLevel.levelNumber
//      }
//    }
//  }
//
//  "head and last Level has NON-OVERLAPPING key-values and all other levels are empty" when {
//    def setupTest(pushStrategy: PushStrategy)(implicit sweeper: CoreTestSweeper) = {
//      //input key-value is 3
//
//      //head level has key-value 1
//      //nextLevel is empty
//      //nextLevel is empty
//      //last level has key-value 2
//
//      val segment = TestSegment(Slice(Memory.put(3)))
//      val level = TestLevel(keyValues = Slice(Memory.put(1)), nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(keyValues = Slice(Memory.put(2)))))))))
//
//      val tasks =
//        TaskAssigner.assignQuick(
//          data = Slice(segment),
//          lowerLevels = NonEmptyList(level, level.nextLevels.map(_.asInstanceOf[Level])),
//          dataOverflow = Int.MaxValue,
//          pushStrategy = pushStrategy
//        )
//
//      (tasks, level, segment)
//    }
//
//    "PushStrategy.OnOverflow" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val (tasks, level, segment) = setupTest(PushStrategy.OnOverflow)
//
//          //expect segment to get assigned to first level.
//          tasks should contain only
//            CompactionTask.Task(
//              target = level,
//              data = SortedSet(segment)
//            )
//
//          tasks.head.target.levelNumber shouldBe level.levelNumber
//      }
//    }
//
//    "PushStrategy.Immediately" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val (tasks, level, segment) = setupTest(PushStrategy.Immediately)
//
//          val lastLevel = level.nextLevels.last.asInstanceOf[Level]
//
//          lastLevel.levelNumber should be > level.levelNumber
//
//          //expect segment to get assigned to first level.
//          tasks should contain only
//            CompactionTask.Task(
//              target = lastLevel,
//              data = SortedSet(segment)
//            )
//
//          tasks.head.target.levelNumber shouldBe lastLevel.levelNumber
//      }
//    }
//
//  }
//
//  "head has OVERLAPPING key-values and last level is non-empty" when {
//    def runTest(pushStrategy: PushStrategy)(implicit sweeper: CoreTestSweeper) = {
//      //input key-value is 1
//
//      //head level has key-value 1
//      //nextLevel is empty
//      //nextLevel is empty
//      //last level has key-value 2
//
//      val segment = TestSegment(Slice(Memory.put(1)))
//      val lastLevel = TestLevel(keyValues = Slice(Memory.put(2)))
//      val level = TestLevel(keyValues = Slice(Memory.put(1)), nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(lastLevel))))))
//
//      val tasks =
//        TaskAssigner.assignQuick(
//          data = Slice(segment),
//          lowerLevels = NonEmptyList(level, level.nextLevels.map(_.asInstanceOf[Level])),
//          dataOverflow = Int.MaxValue,
//          pushStrategy = pushStrategy
//        )
//
//      //expect segment to get assigned to first level.
//      tasks should contain only
//        CompactionTask.Task(
//          target = level,
//          data = SortedSet(segment)
//        )
//
//      tasks.head.target.levelNumber shouldBe level.levelNumber
//    }
//
//    "PushStrategy.OnOverflow" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          runTest(PushStrategy.OnOverflow)
//      }
//    }
//
//    "PushStrategy.Immediately" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          runTest(PushStrategy.Immediately)
//      }
//    }
//
//  }
//
//  "head has key-values assigned but there also exists a non-overlapping key-value" when {
//    def setupTest(pushStrategy: PushStrategy)(implicit sweeper: CoreTestSweeper) = {
//      //input 2 segments with key-values 1 and 3 (3 is copyable)
//
//      //head level has key-value 1
//      //nextLevel is empty
//      //nextLevel is empty
//      //last level has key-value 2
//
//      val segments = Slice(TestSegment(Slice(Memory.put(1))), TestSegment(Slice(Memory.put(3))))
//      val level = TestLevel(keyValues = Slice(Memory.put(1)), nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(nextLevel = Some(TestLevel(keyValues = Slice(Memory.put(2)))))))))
//
//      val tasks =
//        TaskAssigner.assignQuick(
//          data = segments,
//          lowerLevels = NonEmptyList(level, level.nextLevels.map(_.asInstanceOf[Level])),
//          dataOverflow = Int.MaxValue,
//          pushStrategy = pushStrategy
//        )
//
//      (tasks, level, segments)
//    }
//
//    "PushStrategy.OnOverflow" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val (tasks, level, segments) = setupTest(PushStrategy.OnOverflow)
//
//          segments should have size 2
//
//          //expect segment to get assigned to first level.
//          tasks should contain only
//            CompactionTask.Task(
//              target = level,
//              data = SortedSet(segments.head, segments.last)
//            )
//
//          tasks.head.target.levelNumber shouldBe level.levelNumber
//      }
//    }
//
//    "PushStrategy.Immediately" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val (tasks, level, segments) = setupTest(PushStrategy.Immediately)
//
//          segments should have size 2
//
//          //expect segment to get assigned to first level.
//          tasks should have size 2
//
//          tasks.head shouldBe
//            CompactionTask.Task(
//              target = level,
//              data = SortedSet(segments.head)
//            )
//
//          val lastLevel = level.nextLevels.last.asInstanceOf[Level]
//          lastLevel.levelNumber should be > level.levelNumber
//
//          tasks.last shouldBe
//            CompactionTask.Task(
//              target = lastLevel,
//              data = SortedSet(segments.last)
//            )
//
//          tasks.head.target.levelNumber shouldBe level.levelNumber
//          tasks.last.target.levelNumber shouldBe lastLevel.levelNumber
//      }
//    }
//  }
//
//
//  "assign all overlapping Segments to their respective levels" when {
//    "there are NO GAPS" when {
//      def runTest(pushStrategy: PushStrategy): Unit =
//        CoreTestSweeper {
//          implicit sweeper =>
//            //input Segments - 1, 2, 3, 4
//
//            val segments =
//              Slice(
//                TestSegment(Slice(Memory.put(1))),
//                TestSegment(Slice(Memory.put(2))),
//                TestSegment(Slice(Memory.put(3))),
//                TestSegment(Slice(Memory.put(4)))
//              )
//
//            val level4 = TestLevel(keyValues = Slice(Memory.put(4)))
//            val level3 = TestLevel(keyValues = Slice(Memory.put(3)), nextLevel = Some(level4))
//            val level2 = TestLevel(keyValues = Slice(Memory.put(2)), nextLevel = Some(level3))
//            val level1 = TestLevel(keyValues = Slice(Memory.put(1)), nextLevel = Some(level2))
//
//            val tasks =
//              TaskAssigner.assignQuick(
//                data = segments,
//                lowerLevels = NonEmptyList(level1, level1.nextLevels.map(_.asInstanceOf[Level])),
//                dataOverflow = Int.MaxValue,
//                pushStrategy = pushStrategy
//              ).toList
//
//            tasks should have size 4
//
//            tasks.head shouldBe
//              CompactionTask.Task(
//                target = level1,
//                data = SortedSet(segments.head)
//              )
//
//            tasks(1) shouldBe
//              CompactionTask.Task(
//                target = level2,
//                data = SortedSet(segments(1))
//              )
//
//            tasks(2) shouldBe
//              CompactionTask.Task(
//                target = level3,
//                data = SortedSet(segments(2))
//              )
//
//            tasks(3) shouldBe
//              CompactionTask.Task(
//                target = level4,
//                data = SortedSet(segments(3))
//              )
//        }
//
//      "PushStrategy.OnOverflow" in {
//        CoreTestSweeper {
//          implicit sweeper =>
//            runTest(PushStrategy.OnOverflow)
//        }
//      }
//
//      "PushStrategy.Immediately" in {
//        CoreTestSweeper {
//          implicit sweeper =>
//            runTest(PushStrategy.Immediately)
//        }
//      }
//    }
//
//    "there are tail GAPS" when {
//      def setupTest(pushStrategy: PushStrategy)(implicit sweeper: CoreTestSweeper) = {
//        //input Segments - 1, 2, 3, 4, 5, 6, 7
//        val segments =
//          Slice(
//            TestSegment(Slice(Memory.put(1))),
//            TestSegment(Slice(Memory.put(2))),
//            TestSegment(Slice(Memory.put(3))),
//            TestSegment(Slice(Memory.put(4))),
//            TestSegment(Slice(Memory.put(5))),
//            TestSegment(Slice(Memory.put(6))),
//            TestSegment(Slice(Memory.put(7)))
//          )
//
//        val level4 = TestLevel(keyValues = Slice(Memory.put(4)))
//        val level3 = TestLevel(keyValues = Slice(Memory.put(3)), nextLevel = Some(level4))
//        val level2 = TestLevel(keyValues = Slice(Memory.put(2)), nextLevel = Some(level3))
//        val level1 = TestLevel(keyValues = Slice(Memory.put(1)), nextLevel = Some(level2))
//
//        val tasks =
//          TaskAssigner.assignQuick(
//            data = segments,
//            lowerLevels = NonEmptyList(level1, level1.nextLevels.map(_.asInstanceOf[Level])),
//            dataOverflow = Int.MaxValue,
//            pushStrategy = pushStrategy
//          ).toList
//
//        (tasks, (level1, level2, level3, level4), segments)
//      }
//
//      "PushStrategy.OnOverflow" in {
//        CoreTestSweeper {
//          implicit sweeper =>
//            val (tasks, (level1, level2, level3, level4), segments) = setupTest(PushStrategy.OnOverflow)
//
//            tasks should have size 4
//
//            tasks.head shouldBe
//              CompactionTask.Task(
//                target = level1,
//                data = SortedSet(segments.head) ++ segments.takeRight(3)
//              )
//
//            tasks(1) shouldBe
//              CompactionTask.Task(
//                target = level2,
//                data = SortedSet(segments(1))
//              )
//
//            tasks(2) shouldBe
//              CompactionTask.Task(
//                target = level3,
//                data = SortedSet(segments(2))
//              )
//
//            tasks(3) shouldBe
//              CompactionTask.Task(
//                target = level4,
//                data = SortedSet(segments(3))
//              )
//        }
//      }
//
//      "PushStrategy.Immediately" in {
//        CoreTestSweeper {
//          implicit sweeper =>
//            val (tasks, (level1, level2, level3, level4), segments) = setupTest(PushStrategy.Immediately)
//
//            tasks should have size 4
//
//            tasks.head shouldBe
//              CompactionTask.Task(
//                target = level1,
//                data = SortedSet(segments.head)
//              )
//
//            tasks(1) shouldBe
//              CompactionTask.Task(
//                target = level2,
//                data = SortedSet(segments(1))
//              )
//
//            tasks(2) shouldBe
//              CompactionTask.Task(
//                target = level3,
//                data = SortedSet(segments(2))
//              )
//
//            tasks(3) shouldBe
//              CompactionTask.Task(
//                target = level4,
//                data = SortedSet(segments(3)) ++ segments.takeRight(3)
//              )
//        }
//      }
//    }
//
//    "there are head GAPS" when {
//      def setupTest(pushStrategy: PushStrategy)(implicit sweeper: CoreTestSweeper) = {
//        //input Segments - 1, 2, 3, 4, 5, 6, 7
//        val segments =
//          Slice(
//            TestSegment(Slice(Memory.put(1))),
//            TestSegment(Slice(Memory.put(2))),
//            TestSegment(Slice(Memory.put(3))),
//            TestSegment(Slice(Memory.put(4))),
//            TestSegment(Slice(Memory.put(5))),
//            TestSegment(Slice(Memory.put(6))),
//            TestSegment(Slice(Memory.put(7)))
//          )
//
//        val level4 = TestLevel(keyValues = Slice(Memory.put(7)))
//        val level3 = TestLevel(keyValues = Slice(Memory.put(6)), nextLevel = Some(level4))
//        val level2 = TestLevel(keyValues = Slice(Memory.put(5)), nextLevel = Some(level3))
//        val level1 = TestLevel(keyValues = Slice(Memory.put(4)), nextLevel = Some(level2))
//
//        val tasks =
//          TaskAssigner.assignQuick(
//            data = segments,
//            lowerLevels = NonEmptyList(level1, level1.nextLevels.map(_.asInstanceOf[Level])),
//            dataOverflow = Int.MaxValue,
//            pushStrategy = pushStrategy
//          ).toList
//
//        (tasks, (level1, level2, level3, level4), segments)
//      }
//
//      "PushStrategy.OnOverflow" in {
//        CoreTestSweeper {
//          implicit sweeper =>
//            val (tasks, (level1, level2, level3, level4), segments) = setupTest(PushStrategy.OnOverflow)
//
//            tasks should have size 4
//
//            tasks.head shouldBe
//              CompactionTask.Task(
//                target = level1,
//                data = SortedSet(segments.take(4).toList: _*)
//              )
//
//            tasks(1) shouldBe
//              CompactionTask.Task(
//                target = level2,
//                data = SortedSet(segments(4))
//              )
//
//            tasks(2) shouldBe
//              CompactionTask.Task(
//                target = level3,
//                data = SortedSet(segments(5))
//              )
//
//            tasks(3) shouldBe
//              CompactionTask.Task(
//                target = level4,
//                data = SortedSet(segments(6))
//              )
//        }
//      }
//
//      "PushStrategy.Immediately" in {
//        CoreTestSweeper {
//          implicit sweeper =>
//            val (tasks, (level1, level2, level3, level4), segments) = setupTest(PushStrategy.Immediately)
//
//            tasks should have size 4
//
//            tasks.head shouldBe
//              CompactionTask.Task(
//                target = level1,
//                data = SortedSet(segments.drop(3).head)
//              )
//
//            tasks(1) shouldBe
//              CompactionTask.Task(
//                target = level2,
//                data = SortedSet(segments(4))
//              )
//
//            tasks(2) shouldBe
//              CompactionTask.Task(
//                target = level3,
//                data = SortedSet(segments(5))
//              )
//
//            tasks(3) shouldBe
//              CompactionTask.Task(
//                target = level4,
//                data = SortedSet(segments.take(3).toList: _*) ++ SortedSet(segments(6))
//              )
//        }
//      }
//    }
//  }
//}
