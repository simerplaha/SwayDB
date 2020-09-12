/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import java.nio.file.NoSuchFileException

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestTimer}
import swaydb.data.RunThis.eventual
import swaydb.data.config.{ForceSave, MMAP}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice._
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration.DurationInt
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Sliced

class LevelCopySpec0 extends LevelCopySpec

class LevelCopySpec1 extends LevelCopySpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelCopySpec2 extends LevelCopySpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Disabled(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
}

class LevelCopySpec3 extends LevelCopySpec {
  override def inMemoryStorage = true
}

sealed trait LevelCopySpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Sliced[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Sliced[Byte]] = TimeOrder.long
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  implicit val skipListMerger = LevelZeroSkipListMerger

  "copy" should {
    "copy segments" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel()
          level.isEmpty shouldBe true

          val keyValues1 = randomIntKeyStringValues()
          val keyValues2 = randomIntKeyStringValues()
          val segments = Iterable(TestSegment(keyValues1), TestSegment(keyValues2))
          val copiedSegments = level.copyLocal(segments).value

          val allKeyValues = Slice((keyValues1 ++ keyValues2).toArray)

          level.isEmpty shouldBe true //copy function does not write to appendix.

          if (persistent) level.segmentFilesOnDisk should not be empty

          Segment.getAllKeyValues(copiedSegments) shouldBe allKeyValues
      }
    }

    "fail copying Segments if it failed to copy one of the Segments" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel()
          level.isEmpty shouldBe true

          val segment1 = TestSegment()
          val segment2 = TestSegment()

          segment2.delete // delete segment2 so there is a failure in copying Segments

          if (isWindowsAndMMAPSegments())
            eventual(10.seconds) {
              sweeper.receiveAll()
              segment2.existsOnDisk shouldBe false
            }
          else
            segment2.existsOnDisk shouldBe false

          val segments = Iterable(segment1, segment2)
          level.copyLocal(segments).left.value.exception shouldBe a[NoSuchFileException]

          level.isEmpty shouldBe true
          if (persistent) level.reopen.isEmpty shouldBe true
      }
    }

    "copy Map" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel()
          level.isEmpty shouldBe true

          val keyValues = randomPutKeyValues(keyValuesCount)
          val copiedSegments = level.copy(TestMap(keyValues)).value
          level.isEmpty shouldBe true //copy function does not write to appendix.

          if (persistent) level.segmentFilesOnDisk should not be empty

          Segment.getAllKeyValues(copiedSegments) shouldBe keyValues
      }
    }
  }

  "copy map directly into lower level" in {
    TestCaseSweeper {
      implicit sweeper =>

        val level2 = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))
        val level1 = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, pushForward = true, mmap = mmapSegments), nextLevel = Some(level2))

        val keyValues = randomPutKeyValues(keyValuesCount)
        val maps = TestMap(keyValues)

        level1.put(maps).right.right.value.right.value should contain only level2.levelNumber

        level1.isEmpty shouldBe true
        level2.isEmpty shouldBe false

        assertReads(keyValues, level1)

        level1.segmentsInLevel() foreach (_.createdInLevel shouldBe level2.levelNumber)
    }
  }
}
