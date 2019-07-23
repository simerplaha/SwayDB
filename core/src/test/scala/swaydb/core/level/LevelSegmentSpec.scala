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

package swaydb.core.level

import java.nio.file.{FileAlreadyExistsException, Files, NoSuchFileException}

import org.scalamock.scalatest.MockFactory
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.IOEffect._
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.Segment
import swaydb.core.util.PipeOps._
import swaydb.core.util.{Extension, IDGenerator}
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.LevelStorage
import swaydb.data.util.StorageUnits._
import swaydb.ErrorHandler.CoreErrorHandler

import scala.concurrent.ExecutionContext
import scala.util.Random

class LevelSegmentSpec0 extends LevelSegmentSpec

class LevelSegmentSpec1 extends LevelSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelSegmentSpec2 extends LevelSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelSegmentSpec3 extends LevelSegmentSpec {
  override def inMemoryStorage = true
}

sealed trait LevelSegmentSpec extends TestBase with MockFactory {

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

  "writing Segments to single level" should {
    "succeed" when {
      "level is empty" in {
        val level = TestLevel()
        val keyValues = randomIntKeyStringValues(keyValuesCount)
        val segment = TestSegment(keyValues).runIO
        segment.close.runIO
        level.put(segment).runIO
        assertReads(keyValues, level)
        level.close.runIO
      }

      "level is non-empty" in {
        //small Segment size so that small Segments do not collapse when running this test
        // as reads do not value retried on failure in Level, they only value retried in LevelZero.
        val level = TestLevel(segmentSize = 100.bytes)
        val keyValues = randomIntKeyStringValues(keyValuesCount)
        val segment = TestSegment(keyValues).runIO
        level.put(segment).runIO

        val keyValues2 = randomIntKeyStringValues(keyValuesCount * 10)
        val segment2 = TestSegment(keyValues2).runIO
        level.put(segment2).runIO

        assertGet(keyValues, level)
        assertGet(keyValues2, level)
      }

      "writing multiple Segments to an empty Level" in {
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

        val segments = Seq(TestSegment(keyValues1).runIO, TestSegment(keyValues2).runIO, TestSegment(keyValues3).runIO)
        level.put(segments).runIO

        assertReads(keyValues, level)
      }

      "writing multiple Segments to a non empty Level" in {
        val level = TestLevel()
        val allKeyValues = randomPutKeyValues(keyValuesCount * 3, valueSize = 1000, addPutDeadlines = false)(TestTimer.Empty)
        val slicedKeyValues = allKeyValues.groupedSlice(3)
        val keyValues1 = slicedKeyValues(0)
        val keyValues2 = slicedKeyValues(1)
        val keyValues3 = slicedKeyValues(2)

        //create a level with key-values
        level.putKeyValuesTest(keyValues2).runIO
        level.isEmpty shouldBe false

        val segments = Seq(TestSegment(keyValues1.toTransient).runIO, TestSegment(keyValues3.toTransient).runIO)
        level.put(segments).runIO

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

          level.putKeyValuesTest(keyValues).runIO
          level.segmentsCount() shouldBe keyValues.size
          assertDistribution()

          //write the same key-values again so that all Segments are updated. This should still maintain the Segment distribution
          level.putKeyValuesTest(keyValues).runIO
          assertDistribution()

          //shuffle key-values should still maintain distribution order
          Random.shuffle(keyValues.grouped(10)) foreach {
            keyValues =>
              level.putKeyValuesTest(keyValues).runIO
          }
          assertDistribution()

          //delete some key-values
          Random.shuffle(keyValues.grouped(10)).take(2) foreach {
            keyValues =>
              val deleteKeyValues = keyValues.map(keyValue => Memory.remove(keyValue.key)).toSlice
              level.putKeyValuesTest(deleteKeyValues).runIO
          }

          level.putKeyValuesTest(keyValues).runIO
          assertDistribution()
        }
      }

      "copy Segments if segmentsToMerge is empty" in {
        val keyValues = randomKeyValues(keyValuesCount).groupedSlice(5).map(_.updateStats)
        val segmentToCopy = keyValues map (keyValues => TestSegment(keyValues).runIO)

        val level = TestLevel()

        level.put(Seq.empty, segmentToCopy, Seq.empty).runIO

        level.isEmpty shouldBe false
        assertReads(keyValues.flatten, level)
      }

      "copy and merge Segments" in {
        val keyValues = randomKeyValues(100).groupedSlice(10).map(_.updateStats).toArray
        val segmentToCopy = keyValues.take(5) map (keyValues => TestSegment(keyValues).runIO)
        val segmentToMerge = keyValues.drop(5).take(4) map (keyValues => TestSegment(keyValues).runIO)
        val targetSegment = TestSegment(keyValues.last).runIO

        val level = TestLevel()
        level.put(targetSegment).runIO
        level.put(segmentToMerge, segmentToCopy, Seq(targetSegment)).runIO

        level.isEmpty shouldBe false

        assertGet(keyValues.flatten, level)
      }
    }

    "fail" when {
      "fail when writing a deleted segment" in {
        val level = TestLevel()

        val keyValues = randomIntKeyStringValues()
        val segment = TestSegment(keyValues).runIO
        segment.delete.runIO

        val result = level.put(segment).failed.runIO
        if (persistent)
          result.exception shouldBe a[NoSuchFileException]
        else
          result.exception shouldBe a[Exception]

        level.isEmpty shouldBe true

        //if it's a persistent Level, reopen to ensure that Segment did not value committed.
        if (persistent) level.reopen.isEmpty shouldBe true
      }

      "return failure if segmentToMerge has no target Segment" in {
        val keyValues = randomKeyValues(keyValuesCount)
        val segmentsToMerge = TestSegment(keyValues).runIO
        val level = TestLevel()
        level.put(Seq(segmentsToMerge), Seq(), Seq()).failed.runIO shouldBe IO.Error.ReceivedKeyValuesToMergeWithoutTargetSegment(keyValues.size)
      }

      "revert copy if merge fails" in {
        if (persistent) {
          val keyValues = randomKeyValues(100)(TestTimer.Empty).groupedSlice(10).map(_.updateStats).toArray
          val segmentToCopy = keyValues.take(5) map (keyValues => TestSegment(keyValues).runIO)
          val segmentToMerge = keyValues.drop(5).take(4) map (keyValues => TestSegment(keyValues).runIO)
          val targetSegment = TestSegment(keyValues.last).runIO

          val level = TestLevel(segmentSize = 150.bytes)
          level.put(targetSegment).runIO

          //segment to copy
          val id = IDGenerator.segmentId(level.segmentIDGenerator.nextID + 9)
          level.paths.queuedPaths foreach { //create this file in all paths.
            _ =>
              Files.createFile(level.paths.next.resolve(id))
          }

          val appendixBeforePut = level.segmentsInLevel()
          val levelFilesBeforePut = level.segmentFilesOnDisk
          level.put(segmentToMerge, segmentToCopy, Seq(targetSegment)).failed.runIO.exception shouldBe a[FileAlreadyExistsException]
          level.segmentFilesOnDisk shouldBe levelFilesBeforePut
          level.segmentsInLevel().map(_.path) shouldBe appendixBeforePut.map(_.path)
        }
      }

      "revert copy on failure" in {
        if (persistent) {
          val keyValues = randomKeyValues(keyValuesCount).groupedSlice(5).map(_.updateStats)
          val segmentToCopy = keyValues map (keyValues => TestSegment(keyValues).runIO)

          val level = TestLevel()

          //create a file with the same Segment name as the 4th Segment file. This should result in failure.
          val id = IDGenerator.segmentId(level.segmentIDGenerator.nextID + 4)
          level.paths.queuedPaths foreach { //create this file in all paths.
            _ =>
              Files.createFile(level.paths.next.resolve(id))
          }
          val levelFilesBeforePut = level.segmentFilesOnDisk

          level.put(Seq.empty, segmentToCopy, Seq.empty).failed.runIO.exception shouldBe a[FileAlreadyExistsException]

          level.isEmpty shouldBe true
          level.segmentFilesOnDisk shouldBe levelFilesBeforePut
        }
      }
    }
  }

  "writing Segments to two levels" should {
    "succeed" when {
      "upper level has overlapping Segments" in {
        val nextLevel = mock[NextLevel]

        //no key-values value forwarded to next Level
        nextLevel.isTrash _ expects() returning false
        nextLevel.close _ expects() returning IO.unit
        nextLevel.releaseLocks _ expects() returning IO.unit
        nextLevel.closeSegments _ expects() returning IO.unit

        val level = TestLevel(nextLevel = Some(nextLevel))
        val keyValues = randomIntKeyStringValues(keyValuesCount, startId = Some(1))
        level.putKeyValues(keyValues, Seq(TestSegment(keyValues).runIO), None).runIO //write first Segment to Level
        assertGetFromThisLevelOnly(keyValues, level)

        level.put(TestSegment(keyValues.take(1).updateStats).runIO).runIO
        level.put(TestSegment(keyValues.takeRight(1).updateStats).runIO).runIO

        level.close.runIO
      }

      "upper level has no overlapping Segments and nextLevel allows Segment copying" in {
        val nextLevel = mock[NextLevel]
        nextLevel.isTrash _ expects() returning false

        val level = TestLevel(nextLevel = Some(nextLevel))
        val keyValues = randomIntKeyStringValues(keyValuesCount, startId = Some(1))
        level.putKeyValues(keyValues, Seq(TestSegment(keyValues).runIO), None).runIO //write first Segment to Level
        assertGetFromThisLevelOnly(keyValues, level)

        //write non-overlapping key-values
        val nextMaxKey = keyValues.last.key.readInt() + 1000
        val keyValues2 = randomIntKeyStringValues(keyValuesCount, startId = Some(nextMaxKey))
        val segment = TestSegment(keyValues2).runIO

        nextLevel.partitionUnreservedCopyable _ expects * onCall { //check if it can copied into next Level
          segments: Iterable[Segment] =>
            segments should have size 1
            segments.head.path shouldBe segment.path
            (segments, Iterable.empty)
        }

        (nextLevel.put(_: Iterable[Segment])(_: ExecutionContext)) expects(*, *) onCall { //copy into next Level
          (segments: Iterable[Segment], _) =>
            segments should have size 1
            segments.head.path shouldBe segment.path
            IO.unit
        }

        level.put(segment).runIO

        assertGet(keyValues, level) //previous existing key-values should still exist
        assertGetNoneFromThisLevelOnly(keyValues2, level) //newly added key-values do not exist because nextLevel is mocked.
      }

      "upper level has no overlapping Segments and nextLevel does not allows Segment copying due to reserved Segments" in {
        val nextLevel = mock[NextLevel]
        nextLevel.isTrash _ expects() returning false

        val level = TestLevel(nextLevel = Some(nextLevel))
        val keyValues = randomIntKeyStringValues(keyValuesCount, startId = Some(1))
        level.putKeyValues(keyValues, Seq(TestSegment(keyValues).runIO), None).runIO //write first Segment to Level
        assertGetFromThisLevelOnly(keyValues, level)

        //write non-overlapping key-values
        val nextMaxKey = keyValues.last.key.readInt() + 1000
        val keyValues2 = randomIntKeyStringValues(keyValuesCount, startId = Some(nextMaxKey))
        val segment = TestSegment(keyValues2).runIO

        nextLevel.partitionUnreservedCopyable _ expects * onCall { //check if it can copied into next Level
          segments: Iterable[Segment] =>
            segments should have size 1
            segments.head.path shouldBe segment.path
            (Iterable.empty, segments)
        }

        level.put(segment).runIO

        assertGet(keyValues, level) //previous existing key-values should still exist
        assertGetFromThisLevelOnly(keyValues2, level) //newly added key-values do not exist because nextLevel is mocked.
      }

      "lower level can copy 1 of 2 Segments" in {
        val nextLevel = mock[NextLevel]
        nextLevel.isTrash _ expects() returning false

        val level = TestLevel(nextLevel = Some(nextLevel))
        val keyValues = randomIntKeyStringValues(keyValuesCount, startId = Some(1))
        level.putKeyValues(keyValues, Seq(TestSegment(keyValues).runIO), None).runIO //write first Segment to Level
        assertGet(keyValues, level)

        //write non-overlapping key-values
        val nextMaxKey = keyValues.last.key.readInt() + 1000
        val keyValues2 = randomIntKeyStringValues(keyValuesCount, startId = Some(nextMaxKey)).groupedSlice(2)
        val segment2 = TestSegment(keyValues2.head).runIO
        val segment3 = TestSegment(keyValues2.last.updateStats).runIO

        nextLevel.partitionUnreservedCopyable _ expects * onCall {
          segments: Iterable[Segment] =>
            segments should have size 2
            segments.head.path shouldBe segment2.path
            segments.last.path shouldBe segment3.path
            (Seq(segments.last), Seq(segments.head)) //last Segment is copyable.
        }

        (nextLevel.put(_: Iterable[Segment])(_: ExecutionContext)) expects(*, *) onCall { //successfully copied last Segment into next Level.
          (segments: Iterable[Segment], _) =>
            segments should have size 1
            segments.head.path shouldBe segment3.path
            IO.unit
        }

        level.put(Seq(segment2, segment3)).runIO

        assertGetFromThisLevelOnly(keyValues, level) //all key-values value persisted into upper level.
        //segment2's key-values still readable from upper Level since they were copied locally.
        assertGetFromThisLevelOnly(keyValues2.head, level) //all key-values value persisted into upper level.
        assertGetNoneFromThisLevelOnly(keyValues2.last, level) //they were copied to lower level.
      }

      "lower level can copy all Segments but fails to copy" in {
        val nextLevel = mock[NextLevel]
        nextLevel.isTrash _ expects() returning false

        val level = TestLevel(nextLevel = Some(nextLevel))
        val keyValues = randomIntKeyStringValues(keyValuesCount, startId = Some(1))
        level.putKeyValues(keyValues, Seq(TestSegment(keyValues).runIO), None).runIO //write first Segment to Level
        assertGet(keyValues, level)

        //write non-overlapping key-values
        val nextMaxKey = keyValues.last.key.readInt() + 1000
        val keyValues2 = randomIntKeyStringValues(keyValuesCount, startId = Some(nextMaxKey))
        val segment = TestSegment(keyValues2).runIO

        nextLevel.partitionUnreservedCopyable _ expects * onCall { //check if it can copied into next Level
          segments: Iterable[Segment] =>
            segments should have size 1
            segments.head.path shouldBe segment.path //new segments gets requested to push forward.
            (segments, Iterable.empty)
        }

        (nextLevel.put(_: Iterable[Segment])(_: ExecutionContext)) expects(*, *) onCall { //copy into next Level
          (segments: Iterable[Segment], _) =>
            segments should have size 1
            segments.head.path shouldBe segment.path
            IO.Failure(new Exception("Kaboom!!")) //fail to copy, upper level will continue copying in it's Level.
        }

        level.put(segment).runIO

        assertGetFromThisLevelOnly(keyValues, level) //all key-values value persisted into upper level.
        assertGetFromThisLevelOnly(keyValues2, level) //all key-values value persisted into upper level.
      }
    }
  }
}
