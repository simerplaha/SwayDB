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

package swaydb.core.level.compaction.task.assigner

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.segment.Segment
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestTimer}
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.{IO, StorageByteImplicits}

import scala.concurrent.duration._

class LevelTaskAssignerSpec0 extends LevelTaskAssignerSpec

class LevelTaskAssignerSpec1 extends LevelTaskAssignerSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelTaskAssignerSpec2 extends LevelTaskAssignerSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelTaskAssignerSpec3 extends LevelTaskAssignerSpec {
  override def inMemoryStorage = true
}

sealed trait LevelTaskAssignerSpec extends TestBase with MockFactory {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)

  "refresh" when {
    "Level is empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()
          LevelTaskAssigner.refresh(level) shouldBe empty
      }
    }

    "Level is non-empty but no deadline key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()
          level.put(Slice(Memory.put(1))) shouldBe IO.unit

          LevelTaskAssigner.refresh(level) shouldBe empty
      }
    }

    "Level has unexpired key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()
          level.put(Slice(Memory.put(1, Slice.Null, 1.minute.fromNow))) shouldBe IO.unit

          LevelTaskAssigner.refresh(level) shouldBe empty
      }
    }

    "Level has expired key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()
          level.put(Slice(Memory.put(1, Slice.Null, expiredDeadline()))) shouldBe IO.unit

          val task = LevelTaskAssigner.refresh(level).value
          task.source shouldBe level
          task.segments shouldBe level.segments()
      }
    }
  }

  "collapse" when {
    "Level is empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()
          LevelTaskAssigner.collapse(level) shouldBe empty
      }
    }

    "Level is non-empty and contains only one Segment" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()
          level.put(Slice(Memory.put(1))) shouldBe IO.unit

          val task = LevelTaskAssigner.collapse(level).value
          task.source shouldBe level
          task.segments shouldBe level.segments()
      }
    }

    "Level has unexpired key-values" in {
      if (memory)
        cancel("Test not required for in-memory")
      else
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel(segmentConfig = SegmentBlock.Config.random2(deleteDelay = Duration.Zero, mmap = mmapSegments, minSegmentSize = 1.byte))
            level.put(Slice(Memory.put(1), Memory.put(2), Memory.put(3), Memory.put(4), Memory.put(5), Memory.put(6))) shouldBe IO.unit

            val reopened = level.reopen(segmentSize = Int.MaxValue)

            val task = LevelTaskAssigner.collapse(reopened).value
            task.source shouldBe level
            task.segments shouldBe reopened.segments()
        }
    }
  }
}
