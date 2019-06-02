/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.level.compaction

import org.scalamock.scalatest.MockFactory
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.{TestBase, TestExecutionContext, TestLimitQueues, TestTimer}
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

class CompactorSpec0 extends CompactorSpec

class CompactorSpec1 extends CompactorSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class CompactorSpec2 extends CompactorSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class CompactorSpec3 extends CompactorSpec {
  override def inMemoryStorage = true
}

sealed trait CompactorSpec extends TestBase with MockFactory {

  val keyValueCount = 1000

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val timer = TestTimer.Empty

  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  implicit val compactionOrdering = DefaultCompactionOrdering

  override def deleteFiles = false

  "createActor" should {
    "build compaction hierarchy" when {
      "there are two Levels and one new ExecutionContext" in {
        val nextLevel = TestLevel()
        val zero = TestLevelZero(nextLevel = Some(nextLevel), throttleOn = false)

        val actor =
          Compactor.createActor(
            List(zero, nextLevel),
            List(
              CompactionExecutionContext.Create(TestExecutionContext.executionContext),
              CompactionExecutionContext.Shared
            )
          ).get

        actor.unsafeGetState.compactionStates shouldBe empty
        actor.unsafeGetState.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath)
        actor.unsafeGetState.child shouldBe empty
      }

      "there are two Levels and two new ExecutionContext" in {
        val nextLevel = TestLevel()
        val zero = TestLevelZero(nextLevel = Some(nextLevel), throttleOn = false)

        val actor =
          Compactor.createActor(
            List(zero, nextLevel),
            List(
              CompactionExecutionContext.Create(TestExecutionContext.executionContext),
              CompactionExecutionContext.Create(TestExecutionContext.executionContext)
            )
          ).get

        actor.unsafeGetState.compactionStates shouldBe empty
        actor.unsafeGetState.levels.map(_.rootPath) should contain only zero.rootPath
        actor.unsafeGetState.child shouldBe defined

        val childActor = actor.unsafeGetState.child.get.unsafeGetState
        childActor.child shouldBe empty
        childActor.levels.map(_.rootPath) should contain only nextLevel.rootPath
      }

      "there are three Levels and one new ExecutionContext" in {
        val nextLevel2 = TestLevel()
        val nextLevel = TestLevel(nextLevel = Some(nextLevel2))
        val zero = TestLevelZero(nextLevel = Some(nextLevel), throttleOn = false)

        val actor =
          Compactor.createActor(
            List(zero, nextLevel, nextLevel2),
            List(
              CompactionExecutionContext.Create(TestExecutionContext.executionContext),
              CompactionExecutionContext.Shared,
              CompactionExecutionContext.Shared
            )
          ).get

        actor.unsafeGetState.compactionStates shouldBe empty
        actor.unsafeGetState.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath, nextLevel2.rootPath)
        actor.unsafeGetState.child shouldBe empty
      }

      "there are three Levels and two new ExecutionContext" in {
        val nextLevel2 = TestLevel()
        val nextLevel = TestLevel(nextLevel = Some(nextLevel2))
        val zero = TestLevelZero(nextLevel = Some(nextLevel), throttleOn = false)

        val actor =
          Compactor.createActor(
            List(zero, nextLevel, nextLevel2),
            List(
              CompactionExecutionContext.Create(TestExecutionContext.executionContext),
              CompactionExecutionContext.Shared,
              CompactionExecutionContext.Create(TestExecutionContext.executionContext)
            )
          ).get

        actor.unsafeGetState.compactionStates shouldBe empty
        actor.unsafeGetState.levels.map(_.rootPath) shouldBe Slice(zero.rootPath, nextLevel.rootPath)
        actor.unsafeGetState.child shouldBe defined

        val childActor = actor.unsafeGetState.child.get.unsafeGetState
        childActor.child shouldBe empty
        childActor.levels.map(_.rootPath) should contain only nextLevel2.rootPath
      }
    }
  }
}
