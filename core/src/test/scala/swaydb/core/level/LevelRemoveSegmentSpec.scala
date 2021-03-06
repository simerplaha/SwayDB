/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IO
import swaydb.core.TestData._
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestTimer}
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.testkit.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.concurrent.duration.{Duration, DurationInt}
import swaydb.utils.OperatingSystem
import swaydb.utils.OperatingSystem._
import swaydb.utils.FiniteDurations._
import swaydb.utils.StorageUnits._

class LevelRemoveSegmentSpec0 extends LevelRemoveSegmentSpec

class LevelRemoveSegmentSpec1 extends LevelRemoveSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelRemoveSegmentSpec2 extends LevelRemoveSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelRemoveSegmentSpec3 extends LevelRemoveSegmentSpec {
  override def inMemoryStorage = true
}

sealed trait LevelRemoveSegmentSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  "removeSegments" should {
    "remove segments from disk and remove them from appendix" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, deleteDelay = Duration.Zero, mmap = mmapSegments))
          level.put(randomPutKeyValues(keyValuesCount)) shouldBe IO.unit

          level.remove(level.segments()) shouldBe IO.unit

          level.isEmpty shouldBe true

          if (persistent) {
            if (isWindowsAndMMAPSegments())
              eventual(10.seconds) {
                sweeper.receiveAll()
                level.segmentFilesOnDisk shouldBe empty
              }
            else
              level.segmentFilesOnDisk shouldBe empty


            level.reopen.isEmpty shouldBe true
          }
      }
    }
  }
}
