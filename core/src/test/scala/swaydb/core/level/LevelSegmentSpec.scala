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
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper.TestLevelPathSweeperImplicits
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.data._
import swaydb.effect.Effect._
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.util.PipeOps._
import swaydb.core.util.IDGenerator
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.effect.{Dir, Effect, Extension}
import swaydb.testkit.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.LevelStorage
import swaydb.utils.OperatingSystem

import java.nio.file.{FileAlreadyExistsException, NoSuchFileException}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.Random
import swaydb.utils.OperatingSystem._
import swaydb.utils.FiniteDurations._
import swaydb.utils.OperatingSystem
import swaydb.utils.OperatingSystem._
import swaydb.utils.FiniteDurations._
import swaydb.utils.StorageUnits._

class LevelSegmentSpec0 extends LevelSegmentSpec

class LevelSegmentSpec1 extends LevelSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelSegmentSpec2 extends LevelSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelSegmentSpec3 extends LevelSegmentSpec {
  override def inMemoryStorage = true
}

sealed trait LevelSegmentSpec extends TestBase with MockFactory {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  val keyValuesCount = 100
  implicit val ec = TestExecutionContext.executionContext
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()

  //  override def deleteFiles: Boolean =
  //    false

  "writing Segments to single level" should {
    "succeed" when {
      "level is empty" in {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()
            val keyValues = randomIntKeyStringValues(keyValuesCount)
            val segment = TestSegment(keyValues)
            segment.close.runRandomIO.right.value
            level.put(segment) shouldBe IO.unit
            assertReads(keyValues, level)
        }
      }

      "level is non-empty" in {
        runThis(10.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>
              //small Segment size so that small Segments do not collapse when running this test
              // as reads do not value retried on failure in Level, they only value retried in LevelZero.
              val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 100.bytes, mmap = mmapSegments))
              val keyValues = randomIntKeyStringValues(keyValuesCount)
              val segment = TestSegment(keyValues)
              level.put(segment) shouldBe IO.unit

              val keyValues2 = randomIntKeyStringValues(keyValuesCount * 10)
              val segment2 = TestSegment(keyValues2).runRandomIO.right.value
              level.put(segment2).get

              assertGet(keyValues, level)
              assertGet(keyValues2, level)
          }
        }
      }

      "writing multiple Segments to an empty Level" in {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()
            val keyValues = randomIntKeyStringValues(keyValuesCount * 3, valueSize = 1000)

            val (keyValues1, keyValues2, keyValues3) =
              keyValues
                .splitAt(keyValues.size / 3)
                .==> {
                  case (split1, split2) =>
                    val (two, three) = split2.splitAt(split2.size / 2)
                    (split1, two, three)
                }

            val segments = Seq(TestSegment(keyValues1).runRandomIO.right.value, TestSegment(keyValues2).runRandomIO.right.value, TestSegment(keyValues3).runRandomIO.right.value)
            level.putSegments(segments) shouldBe IO.unit

            assertReads(keyValues, level)
        }
      }

      "writing multiple Segments to a non empty Level" in {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()
            val allKeyValues = randomPutKeyValues(keyValuesCount * 3, valueSize = 1000, addPutDeadlines = false)(TestTimer.Empty)
            val slicedKeyValues = allKeyValues.groupedSlice(3)
            val keyValues1 = slicedKeyValues(0)
            val keyValues2 = slicedKeyValues(1)
            val keyValues3 = slicedKeyValues(2)

            //create a level with key-values
            level.put(keyValues2) shouldBe IO.unit
            level.isEmpty shouldBe false

            val segments =
              Seq(
                TestSegment(keyValues1, segmentConfig = SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, mmap = mmapSegments)),
                TestSegment(keyValues3, segmentConfig = SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, mmap = mmapSegments))
              )

            level.putSegments(segments) shouldBe IO.unit

            assertReads(allKeyValues, level)
        }
      }

      "distribute Segments to multiple directories based on the distribution ratio" in {
        if (persistent) {
          TestCaseSweeper {
            implicit sweeper =>
              val dir = testClassDir.resolve("distributeSegmentsTest").sweep()

              def assertDistribution() = {
                def assert() = {
                  dir.resolve(1.toString).files(Extension.Seg) should have size 7
                  dir.resolve(2.toString).files(Extension.Seg) should have size 14
                  dir.resolve(3.toString).files(Extension.Seg) should have size 21
                  dir.resolve(4.toString).files(Extension.Seg) should have size 28
                  dir.resolve(5.toString).files(Extension.Seg) should have size 30
                }

                if (OperatingSystem.isWindows)
                  eventual(10.seconds) {
                    sweeper.receiveAll()
                    assert()
                  }
                else
                  assert()
              }

              val storage =
                LevelStorage.Persistent(
                  dir = dir.resolve(1.toString),
                  otherDirs =
                    Seq(
                      Dir(dir.resolve(2.toString), 2),
                      Dir(dir.resolve(3.toString), 3),
                      Dir(dir.resolve(4.toString), 4),
                      Dir(dir.resolve(5.toString), 5)
                    ),
                  appendixMMAP = MMAP.randomForMap(),
                  appendixFlushCheckpointSize = 4.mb
                )

              val keyValues = randomPutKeyValues(100)

              val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.byte, deleteDelay = Duration.Zero, mmap = mmapSegments), levelStorage = storage)

              level.put(keyValues).runRandomIO.right.value
              level.segmentsCount() shouldBe keyValues.size
              assertDistribution()

              //write the same key-values again so that all Segments are updated. This should still maintain the Segment distribution
              level.put(keyValues).runRandomIO.right.value
              assertDistribution()

              //shuffle key-values should still maintain distribution order
              Random.shuffle(keyValues.grouped(10)) foreach {
                keyValues =>
                  level.put(keyValues).runRandomIO.right.value
              }
              assertDistribution()

              //delete some key-values
              Random.shuffle(keyValues.grouped(10)).take(2) foreach {
                keyValues =>
                  val deleteKeyValues = keyValues.map(keyValue => Memory.remove(keyValue.key)).toSlice
                  level.put(deleteKeyValues).runRandomIO.right.value
              }

              level.put(keyValues).runRandomIO.right.value
              assertDistribution()
          }
        }
      }
    }

    "fail" when {
      "fail when writing a deleted segment" in {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()

            val keyValues = randomIntKeyStringValues()
            val segment = TestSegment(keyValues)
            segment.delete

            if (isWindowsAndMMAPSegments())
              sweeper.receiveAll()

            val result = level.put(segment).left.get
            if (persistent)
              result.exception shouldBe a[NoSuchFileException]
            else
              result.exception shouldBe a[Exception]

            level.isEmpty shouldBe true

            //if it's a persistent Level, reopen to ensure that Segment did not value committed.
            if (persistent) level.reopen.isEmpty shouldBe true
        }
      }

      "revert copy if merge fails" in {
        if (persistent)
          runThis(10.times, log = true) {
            TestCaseSweeper {
              implicit sweeper =>
                val keyValues = randomKeyValues(100)(TestTimer.Empty).groupedSlice(10).toArray
                val segmentToMerge = keyValues map (keyValues => TestSegment(keyValues))

                val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 150.bytes, deleteDelay = Duration.Zero, mmap = mmapSegments))
                level.putSegments(segmentToMerge) shouldBe IO.unit

                //segment to copy
                val id = IDGenerator.segment(level.segmentIDGenerator.next + 9)
                level.pathDistributor.queuedPaths foreach { //create this file in all paths.
                  _ =>
                    Effect.createFile(level.pathDistributor.next.resolve(id))
                }

                val appendixBeforePut = level.segments()
                val levelFilesBeforePut = level.segmentFilesOnDisk
                level.putSegments(segmentToMerge).left.get.exception shouldBe a[FileAlreadyExistsException]

                if (isWindowsAndMMAPSegments())
                  eventual(10.seconds) {
                    sweeper.receiveAll()
                    level.segmentFilesOnDisk shouldBe levelFilesBeforePut
                  }
                else
                  level.segmentFilesOnDisk shouldBe levelFilesBeforePut

                level.segments().map(_.path) shouldBe appendixBeforePut.map(_.path)
            }
          }
      }
    }
  }
}
