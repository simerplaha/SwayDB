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

package swaydb.core.level.compaction.throttle.behaviour

import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.DefIO
import swaydb.core.level.compaction.throttle.LevelState
import swaydb.core.segment.Segment
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave}
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.compaction.LevelThrottle
import swaydb.data.config.MMAP
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import scala.concurrent.duration._


class BehaviourWakeUp_compactLastLevel_multiLevel_Spec0 extends BehaviourWakeUp_compactLastLevel_multiLevel_Spec

class BehaviourWakeUp_compactLastLevel_multiLevel_Spec1 extends BehaviourWakeUp_compactLastLevel_multiLevel_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class BehaviourWakeUp_compactLastLevel_multiLevel_Spec2 extends BehaviourWakeUp_compactLastLevel_multiLevel_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class BehaviourWakeUp_compactLastLevel_multiLevel_Spec3 extends BehaviourWakeUp_compactLastLevel_multiLevel_Spec {
  override def inMemoryStorage = true
}

sealed trait BehaviourWakeUp_compactLastLevel_multiLevel_Spec extends TestBase {

  implicit val ec = TestExecutionContext.executionContext
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()

  "ignore compaction" when {
    "empty" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val lowerLevel = TestLevel()
            val level = TestLevel(nextLevel = Some(lowerLevel))
            //get expected deadline early and expect the state's deadline to be greater than this
            //to account for the time taken running this test
            val expectedDeadline = LevelState.longSleep

            val state =
              BehaviorWakeUp.compactLastLevel(
                level = level,
                stateId = Int.MaxValue,
                pushStrategy = randomPushStrategy()
              ).awaitInf

            val sleeping = state.shouldBeInstanceOf[LevelState.Sleeping]

            sleeping.stateId shouldBe Int.MaxValue
            //deadline should be greater than long sleep.
            sleeping.sleepDeadline should be > expectedDeadline
        }
      }
    }

    "there is only one file with no expired key-values" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val keyValues =
              Slice.range(1, 6) map {
                key =>
                  randomPutKeyValue(key, randomString, someOrNone(1.hour.fromNow))
              }

            //second level
            val level2 = TestLevel()

            //level1
            val level1 =
              TestLevel(
                keyValues = keyValues,
                //compaction is never overdue
                throttle = _ => LevelThrottle(10.seconds, 20),
                //segments are too large means all existing segments are small
                segmentConfig = SegmentBlock.Config.random2(deleteDelay = Duration.Zero, mmap = mmapSegments, minSegmentSize = 100.mb),
                nextLevel = Some(level2)
              )
            //get expected deadline early and expect the state's deadline to be greater than this
            //to account for the time taken running this test
            val expectedDeadline = level1.nextCompactionDelay.fromNow

            val segmentsBeforeCompaction = level1.segments()
            level1.isEmpty shouldBe false
            segmentsBeforeCompaction should have size 1

            //run compaction and expect state to not change because there is only segment in the Level
            //also no extension is executed i.e. level2 should be empty
            val state =
            BehaviorWakeUp.compactLastLevel(
              level = level1,
              stateId = Int.MaxValue,
              pushStrategy = randomPushStrategy()
            ).awaitInf

            val sleeping = state.shouldBeInstanceOf[LevelState.Sleeping]

            sleeping.stateId shouldBe Int.MaxValue
            //deadline should be greater than long sleep.
            sleeping.sleepDeadline should be > expectedDeadline

            level1.segments() shouldBe segmentsBeforeCompaction
            level2.isEmpty shouldBe true
        }
      }
    }

    "all segments are small but there are only 2 segments (minimum is 3)" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val keyValues =
              Slice.range(1, 15) map {
                key =>
                  randomPutKeyValue(key, randomString, someOrNone(1.hour.fromNow))
              }

            val testSegments = keyValues.grouped(2).map(TestSegment(_)).toList

            //second level
            val level2 = TestLevel()

            //level1
            val level1 =
              TestLevel(
                keyValues = keyValues,
                //compaction is never overdue
                throttle = _ => LevelThrottle(10.seconds, 20),
                //segments are too large means all existing segments are small
                segmentConfig = SegmentBlock.Config.random2(deleteDelay = Duration.Zero, mmap = mmapSegments, minSegmentSize = 100.mb),
                nextLevel = Some(level2)
              )

            testSegments.foreach(_.segmentSize should be < level1.minSegmentSize)

            level1.commitPersisted(Seq(DefIO(Segment.Null, testSegments))) shouldBe IO.unit

            //3 small segments are created
            level1.segments() should have size testSegments.size

            //get expected deadline early and expect the state's deadline to be greater than this
            //to account for the time taken running this test
            val expectedDeadline = level1.nextCompactionDelay.fromNow

            val segmentsBeforeCompaction = level1.segments()

            val state =
              BehaviorWakeUp.compactLastLevel(
                level = level1,
                stateId = Int.MaxValue,
                pushStrategy = randomPushStrategy()
              ).awaitInf

            val sleeping = state.shouldBeInstanceOf[LevelState.Sleeping]

            sleeping.stateId shouldBe Int.MaxValue
            //deadline should be greater than long sleep.
            sleeping.sleepDeadline should be > expectedDeadline

            level1.segments() should have size testSegments.size
            level1.segments().flatMap(_.iterator(randomBoolean())) shouldBe keyValues

            //segments are not deleted
            testSegments.foreach(_.existsOnDiskOrMemory shouldBe true)
            level1.segments() shouldBe segmentsBeforeCompaction
            level2.isEmpty shouldBe true
        }
      }
    }
  }

  "extend" when {
    "the level is cleaned up but nextCompactionDelay is overdue" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val keyValues =
              Slice.range(1, 15) map {
                key =>
                  randomPutKeyValue(key, randomString, someOrNone(1.hour.fromNow))
              }

            //create Segments
            val testSegments = keyValues.grouped(3).map(TestSegment(_)).toList

            //second level
            val level2 = TestLevel()

            //level1
            val level1 =
              TestLevel(
                keyValues = keyValues,
                //compaction is always overdue
                throttle = _ => LevelThrottle(Duration.Zero, 100.mb),
                //segments are too large means all existing segments are small
                segmentConfig = SegmentBlock.Config.random2(deleteDelay = Duration.Zero, mmap = mmapSegments, minSegmentSize = 100.mb),
                nextLevel = Some(level2)
              )

            //all Segments are small
            testSegments.foreach(_.segmentSize should be < level1.minSegmentSize)

            //commit the Segments to the Level
            level1.commitPersisted(Seq(DefIO(Segment.Null, testSegments))) shouldBe IO.unit

            //3 small segments are created
            level1.segments() should have size testSegments.size

            //get expected deadline early and expect the state's deadline to be greater than this
            //to account for the time taken running this test
            val expectedDeadline = level1.nextCompactionDelay.fromNow

            //run compaction
            val state =
              BehaviorWakeUp.compactLastLevel(
                level = level1,
                stateId = Int.MaxValue,
                pushStrategy = randomPushStrategy()
              ).awaitInf

            val sleeping = state.shouldBeInstanceOf[LevelState.Sleeping]

            sleeping.stateId shouldBe Int.MaxValue
            //deadline should be greater than long sleep.
            sleeping.sleepDeadline should be > expectedDeadline

            level1.isEmpty shouldBe true
            //level1 segments are deleted
            testSegments.foreach(_.existsOnDiskOrMemory shouldBe false)
            level2.segments().flatMap(_.iterator(randomBoolean())) shouldBe keyValues
        }
      }
    }
  }
}
