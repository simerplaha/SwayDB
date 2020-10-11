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

import org.scalamock.scalatest.MockFactory
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.io.file.Effect._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave, TestTimer}
import swaydb.data.compaction.Throttle
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class LevelReadSpec0 extends LevelReadSpec

class LevelReadSpec1 extends LevelReadSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelReadSpec2 extends LevelReadSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelReadSpec3 extends LevelReadSpec {
  override def inMemoryStorage = true
}

sealed trait LevelReadSpec extends TestBase with MockFactory {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit val ec = TestExecutionContext.executionContext
  val keyValuesCount = 100

  "Level.mightContainKey" should {
    "return true for key-values that exists or else false (bloom filter test on reboot)" in {
      TestCaseSweeper {
        implicit sweeper =>
          val keyValues = randomPutKeyValues(keyValuesCount, addPutDeadlines = false)

          def assert(level: Level) = {
            keyValues foreach {
              keyValue =>
                level.mightContainKey(keyValue.key).runRandomIO.right.value shouldBe true
            }

            level.mightContainKey("THIS KEY DOES NOT EXISTS").runRandomIO.right.value shouldBe false
          }

          val level = TestLevel(bloomFilterConfig = BloomFilterBlock.Config.random.copy(falsePositiveRate = 0.01))
          level.putKeyValuesTest(keyValues).runRandomIO.right.value

          assert(level)
          if (persistent) assert(level.reopen)
      }
    }
  }

  "Level.takeSmallSegments" should {
    "filter smaller segments from a Level" in {
      TestCaseSweeper {
        implicit sweeper =>
          //disable throttling so small segment compaction does not occur
          val level = TestLevel(nextLevel = None, throttle = (_) => Throttle(Duration.Zero, 0), segmentConfig = SegmentBlock.Config.random2(minSegmentSize = 1.kb))

          val keyValues = randomPutKeyValues(1000, addPutDeadlines = false)
          level.putKeyValuesTest(keyValues).runRandomIO.right.value
          //do another put so split occurs.
          level.putKeyValuesTest(keyValues.headSlice).runRandomIO.right.value
          level.segmentsCount() > 1 shouldBe true //ensure there are Segments in this Level

          if (persistent) {
            val reopen = level.reopen(segmentSize = 10.mb)

            reopen.takeSmallSegments(10000) should not be empty
            //iterate again on the same Iterable.
            // This test is to ensure that returned List is not a java Iterable which is only iterable once.
            reopen.takeSmallSegments(10000) should not be empty

            reopen.reopen(segmentSize = 10.mb).takeLargeSegments(1) shouldBe empty
          }
      }
    }
  }

  "Level.meter" should {
    "return Level stats" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val putKeyValues = randomPutKeyValues(keyValuesCount)
          //refresh so that if there is a compression running, this Segment will compressed.
          val segments =
            TestSegment(putKeyValues, segmentConfig = SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue, mmap = mmapSegments))
              .runRandomIO
              .right.value
              .refresh(
                removeDeletes = false,
                createdInLevel = 0,
                valuesConfig = ValuesBlock.Config.random,
                sortedIndexConfig = SortedIndexBlock.Config.random,
                binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
                hashIndexConfig = HashIndexBlock.Config.random,
                bloomFilterConfig = BloomFilterBlock.Config.random,
                segmentConfig = SegmentBlock.Config.random2(minSegmentSize = 100.mb)
              ).runRandomIO.right.value

          segments should have size 1
          val segment = segments.head

          level.put(Seq(segment), randomMaxParallelism()).right.right.value.right.value

          level.meter.segmentsCount shouldBe 1
          level.meter.levelSize shouldBe segment.segmentSize
      }
    }
  }

  "Level.meterFor" should {
    "forward request to the right level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level2 = TestLevel()
          val level1 = TestLevel(nextLevel = Some(level2))

          val putKeyValues = randomPutKeyValues(keyValuesCount)
          //refresh so that if there is a compression running, this Segment will compressed.
          val segments =
            TestSegment(putKeyValues, segmentConfig = SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue, mmap = mmapSegments))
              .runRandomIO.right.value
              .refresh(
                removeDeletes = false,
                createdInLevel = 0,
                valuesConfig = ValuesBlock.Config.random,
                sortedIndexConfig = SortedIndexBlock.Config.random,
                binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
                hashIndexConfig = HashIndexBlock.Config.random,
                bloomFilterConfig = BloomFilterBlock.Config.random,
                segmentConfig = SegmentBlock.Config.random2(minSegmentSize = 100.mb)
              ).runRandomIO.right.value

          segments should have size 1
          val segment = segments.head

          level2.put(Seq(segment), randomMaxParallelism()).right.right.value.right.value

          level1.meter.levelSize shouldBe 0
          level1.meter.segmentsCount shouldBe 0

          val level1Meter = level1.meterFor(level1.pathDistributor.headPath.folderId.toInt).get
          level1Meter.levelSize shouldBe 0
          level1Meter.segmentsCount shouldBe 0

          level2.meter.segmentsCount shouldBe 1
          level2.meter.levelSize shouldBe segment.segmentSize

          val level2Meter = level1.meterFor(level2.pathDistributor.headPath.folderId.toInt).get
          level2Meter.segmentsCount shouldBe 1
          level2Meter.levelSize shouldBe segment.segmentSize
      }
    }

    "return None is Level does not exist" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level2 = TestLevel()
          val level1 = TestLevel(nextLevel = Some(level2))

          val putKeyValues = randomPutKeyValues(keyValuesCount)
          val segment = TestSegment(putKeyValues).runRandomIO.right.value
          level2.put(Seq(segment), randomMaxParallelism()).right.right.value.right.value

          level1.meterFor(3) shouldBe empty
      }
    }
  }
}
