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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import java.nio.file.{FileAlreadyExistsException, Files, NoSuchFileException}

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.core.CommonAssertions._
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.IOEffect._
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.util.PipeOps._
import swaydb.core.util.{Extension, IDGenerator}
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.IO
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.LevelStorage
import swaydb.data.util.StorageUnits._

import scala.util.Random

class LevelWriteSegmentSpec0 extends LevelWriteSegmentSpec

class LevelWriteSegmentSpec1 extends LevelWriteSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelWriteSegmentSpec2 extends LevelWriteSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelWriteSegmentSpec3 extends LevelWriteSegmentSpec {
  override def inMemoryStorage = true
}

sealed trait LevelWriteSegmentSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter
  implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(keyValuesCount)
  implicit val skipListMerger = LevelZeroSkipListMerger

  "put segments" when {
    "single Level" should {
      "write a segment to an empty Level" in {
        val level = TestLevel()
        val keyValues = randomIntKeyStringValues(keyValuesCount)
        val segment = TestSegment(keyValues).assertGet
        segment.close.assertGet
        level.put(segment).assertGet
        assertReads(keyValues, level)
      }

      "write a segment to a non empty Level" in {
        //small Segment size so that small Segments do not collapse when running this test
        // as reads do not get retried on failure in Level, they only get retried in LevelZero.
        val level = TestLevel(segmentSize = 100.bytes)
        val keyValues = randomIntKeyStringValues(keyValuesCount)
        val segment = TestSegment(keyValues).assertGet
        level.put(segment).assertGet

        val keyValues2 = randomIntKeyStringValues(keyValuesCount * 10)
        val segment2 = TestSegment(keyValues2).assertGet
        level.put(segment2).assertGet

        assertGet(keyValues, level)
        assertGet(keyValues2, level)
      }

      "write multiple Segments to an empty Level" in {
        val level = TestLevel()
        val keyValues = randomIntKeyStringValues(keyValuesCount * 3, valueSize = 1000)

        val (keyValues1, keyValues2, keyValues3) =
          keyValues
            .splitAt(keyValues.size / 3)
            .==> {
              case (split1, split2) =>
                val (two, three) = split2.splitAt(split2.size / 2)
                (split1.updateStats, two.updateStats, three.updateStats)
            }

        val segments = Seq(TestSegment(keyValues1).assertGet, TestSegment(keyValues2).assertGet, TestSegment(keyValues3).assertGet)
        level.put(segments).assertGet

        assertReads(keyValues, level)
      }

      "write multiple Segments to a non empty Level" in {
        val level = TestLevel()
        val allKeyValues = randomPutKeyValues(keyValuesCount * 3, valueSize = 1000, addRandomPutDeadlines = false)(TestTimer.Empty)
        val slicedKeyValues = allKeyValues.groupedSlice(3)
        val keyValues1 = slicedKeyValues(0)
        val keyValues2 = slicedKeyValues(1)
        val keyValues3 = slicedKeyValues(2)

        //create a level with key-values
        level.putKeyValues(keyValues2).assertGet
        level.isEmpty shouldBe false

        val segments = Seq(TestSegment(keyValues1.toTransient).assertGet, TestSegment(keyValues3.toTransient).assertGet)
        level.put(segments).assertGet

        assertReads(allKeyValues, level)
      }

      "distribute Segments to multiple directories based on the distribution ratio" in {
        if (persistent) {
          val dir = testDir.resolve("distributeSegmentsTest")

          def assertDistribution() = {
            dir.resolve(1.toString).files(Extension.Seg) should have size 7
            dir.resolve(2.toString).files(Extension.Seg) should have size 14
            dir.resolve(3.toString).files(Extension.Seg) should have size 21
            dir.resolve(4.toString).files(Extension.Seg) should have size 28
            dir.resolve(5.toString).files(Extension.Seg) should have size 30
          }

          val storage =
            LevelStorage.Persistent(
              mmapSegmentsOnWrite = mmapSegmentsOnWrite,
              mmapSegmentsOnRead = mmapSegmentsOnRead,
              dir = dir.resolve(1.toString),
              otherDirs =
                Seq(
                  Dir(dir.resolve(2.toString), 2),
                  Dir(dir.resolve(3.toString), 3),
                  Dir(dir.resolve(4.toString), 4),
                  Dir(dir.resolve(5.toString), 5)
                )
            )
          val keyValues = randomPutKeyValues(100)

          val level = TestLevel(segmentSize = 1.byte, levelStorage = storage)

          level.putKeyValues(keyValues).assertGet
          level.segmentsCount() shouldBe keyValues.size
          assertDistribution()

          //write the same key-values again so that all Segments are updated. This should still maintain the Segment distribution
          level.putKeyValues(keyValues).assertGet
          assertDistribution()

          //shuffle key-values should still maintain distribution order
          Random.shuffle(keyValues.grouped(10)) foreach {
            keyValues =>
              level.putKeyValues(keyValues).assertGet
          }
          assertDistribution()

          //delete some key-values
          Random.shuffle(keyValues.grouped(10)).take(2) foreach {
            keyValues =>
              val deleteKeyValues = keyValues.map(keyValue => Memory.remove(keyValue.key)).toSlice
              level.putKeyValues(deleteKeyValues).assertGet
          }

          level.putKeyValues(keyValues).assertGet
          assertDistribution()
        }
      }

      "fail when writing a deleted segment" in {
        val level = TestLevel()

        val keyValues = randomIntKeyStringValues()
        val segment = TestSegment(keyValues).assertGet
        segment.delete.assertGet

        val result = level.put(segment).failed.assertGet
        if (persistent)
          result.exception shouldBe a[NoSuchFileException]
        else
          result.exception shouldBe a[Exception]

        level.isEmpty shouldBe true

        //if it's a persistent Level, reopen to ensure that Segment did not get committed.
        if (persistent) level.reopen.isEmpty shouldBe true
      }

      "return failure if segmentToMerge has no target Segment" in {
        val keyValues = randomKeyValues(keyValuesCount)
        val segmentsToMerge = TestSegment(keyValues).assertGet
        val level = TestLevel()
        level.put(Seq(segmentsToMerge), Seq(), Seq()).failed.assertGet shouldBe IO.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(keyValues.size)
      }

      "copy Segments if segmentsToMerge is empty" in {
        val keyValues = randomKeyValues(keyValuesCount).groupedSlice(5).map(_.updateStats)
        val segmentToCopy = keyValues map (keyValues => TestSegment(keyValues).assertGet)

        val level = TestLevel()

        level.put(Seq.empty, segmentToCopy, Seq.empty).assertGet

        level.isEmpty shouldBe false
        assertReads(keyValues.flatten, level)
      }

      "revert copy on failure" in {
        if (persistent) {
          val keyValues = randomKeyValues(keyValuesCount).groupedSlice(5).map(_.updateStats)
          val segmentToCopy = keyValues map (keyValues => TestSegment(keyValues).assertGet)

          val level = TestLevel()

          //create a file with the same Segment name as the 4th Segment file. This should result in failure.
          val id = IDGenerator.segmentId(level.segmentIDGenerator.nextID + 4)
          level.paths.queuedPaths foreach { //create this file in all paths.
            _ =>
              Files.createFile(level.paths.next.resolve(id))

          }
          val levelFilesBeforePut = level.segmentFilesOnDisk

          level.put(Seq.empty, segmentToCopy, Seq.empty).failed.assertGet.exception shouldBe a[FileAlreadyExistsException]

          level.isEmpty shouldBe true
          level.segmentFilesOnDisk shouldBe levelFilesBeforePut
        }
      }

      "copy and merge Segments" in {
        val keyValues = randomKeyValues(100).groupedSlice(10).map(_.updateStats).toArray
        val segmentToCopy = keyValues.take(5) map (keyValues => TestSegment(keyValues).assertGet)
        val segmentToMerge = keyValues.drop(5).take(4) map (keyValues => TestSegment(keyValues).assertGet)
        val targetSegment = TestSegment(keyValues.last).assertGet

        val level = TestLevel()
        level.put(targetSegment).assertGet
        level.put(segmentToMerge, segmentToCopy, Seq(targetSegment)).assertGet

        level.isEmpty shouldBe false

        assertGet(keyValues.flatten, level)
      }

      "revert copy if merge fails" in {
        if (persistent) {
          val keyValues = randomKeyValues(100)(TestTimer.Empty).groupedSlice(10).map(_.updateStats).toArray
          val segmentToCopy = keyValues.take(5) map (keyValues => TestSegment(keyValues).assertGet)
          val segmentToMerge = keyValues.drop(5).take(4) map (keyValues => TestSegment(keyValues).assertGet)
          val targetSegment = TestSegment(keyValues.last).assertGet

          val level = TestLevel(segmentSize = 150.bytes)
          level.put(targetSegment).assertGet

          //segment to copy
          val id = IDGenerator.segmentId(level.segmentIDGenerator.nextID + 9)
          level.paths.queuedPaths foreach { //create this file in all paths.
            _ =>
              Files.createFile(level.paths.next.resolve(id))

          }

          val appendixBeforePut = level.segmentsInLevel()
          val levelFilesBeforePut = level.segmentFilesOnDisk
          level.put(segmentToMerge, segmentToCopy, Seq(targetSegment)).failed.assertGet.exception shouldBe a[FileAlreadyExistsException]
          level.segmentFilesOnDisk shouldBe levelFilesBeforePut
          level.segmentsInLevel().map(_.path) shouldBe appendixBeforePut.map(_.path)
        }
      }
    }
  }
}
