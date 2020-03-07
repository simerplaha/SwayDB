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
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.io.file.BlockCache
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.{TestBase, TestSweeper, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class LevelCopySpec0 extends LevelCopySpec

class LevelCopySpec1 extends LevelCopySpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelCopySpec2 extends LevelCopySpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelCopySpec3 extends LevelCopySpec {
  override def inMemoryStorage = true
}

sealed trait LevelCopySpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeper.Enabled = TestSweeper.fileSweeper
  implicit val memorySweeperImplicitSweeper: Option[MemorySweeper.All] = TestSweeper.memorySweeperMax
  implicit val skipListMerger = LevelZeroSkipListMerger

  "copy" should {
    "copy segments" in {
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

    "fail copying Segments if it failed to copy one of the Segments" in {
      val level = TestLevel()
      level.isEmpty shouldBe true

      val segment1 = TestSegment()
      val segment2 = TestSegment()

      segment2.delete // delete segment2 so there is a failure in copying Segments

      val segments = Iterable(segment1, segment2)
      level.copyLocal(segments).left.value.exception shouldBe a[NoSuchFileException]

      level.isEmpty shouldBe true
      if (persistent) level.reopen.isEmpty shouldBe true
    }

    "copy Map" in {
      val level = TestLevel()
      level.isEmpty shouldBe true

      val keyValues = randomPutKeyValues(keyValuesCount)
      val copiedSegments = level.copy(TestMap(keyValues)).value
      level.isEmpty shouldBe true //copy function does not write to appendix.

      if (persistent) level.segmentFilesOnDisk should not be empty

      Segment.getAllKeyValues(copiedSegments) shouldBe keyValues
    }
  }

  "copy map directly into lower level" in {
    val level2 = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb))
    val level1 = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, pushForward = true), nextLevel = Some(level2))

    val keyValues = randomPutKeyValues(keyValuesCount)
    val maps = TestMap(keyValues)

    level1.put(maps).right.right.value.right.value should contain only level2.levelNumber

    level1.isEmpty shouldBe true
    level2.isEmpty shouldBe false

    assertReads(keyValues, level1)

    level1.segmentsInLevel() foreach (_.createdInLevel shouldBe level2.levelNumber)
  }
}
