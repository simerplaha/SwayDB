/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IO
import swaydb.config.MMAP
import swaydb.core.CoreTestData._
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.{ACoreSpec, TestSweeper, TestForceSave, TestTimer}
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import scala.concurrent.duration.{Duration, DurationInt}

class LevelRemoveSegmentSpec0 extends LevelRemoveSegmentSpec

class LevelRemoveSegmentSpec1 extends LevelRemoveSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelRemoveSegmentSpec2 extends LevelRemoveSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
}

class LevelRemoveSegmentSpec3 extends LevelRemoveSegmentSpec {
  override def isMemorySpec = true
}

sealed trait LevelRemoveSegmentSpec extends ALevelSpec with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  "removeSegments" should {
    "remove segments from disk and remove them from appendix" in {
      TestSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel(segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, deleteDelay = Duration.Zero, mmap = mmapSegments))
          level.put(randomPutKeyValues(keyValuesCount)) shouldBe IO.unit

          level.remove(level.segments()) shouldBe IO.unit

          level.isEmpty shouldBe true

          if (isPersistentSpec) {
            if (isWindowsAndMMAPSegments())
              eventual(10.seconds) {
                sweeper.receiveAll()
                level.segmentFilesOnDisk() shouldBe empty
              }
            else
              level.segmentFilesOnDisk() shouldBe empty

            level.reopen.isEmpty shouldBe true
          }
      }
    }
  }
}
