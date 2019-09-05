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

import java.nio.channels.OverlappingFileLockException

import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues._
import org.scalatest.PrivateMethodTester
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data._
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.io.file.IOEffect
import swaydb.core.io.file.IOEffect._
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.map.MapEntry
import swaydb.core.segment.Segment
import swaydb.core.util.{Extension, ReserveRange}
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.LevelStorage
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.Promise

class LevelSpec0 extends LevelSpec

class LevelSpec1 extends LevelSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelSpec2 extends LevelSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelSpec3 extends LevelSpec {
  override def inMemoryStorage = true
}

sealed trait LevelSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  val keyValuesCount = 100

  //    override def deleteFiles: Boolean =
  //      false

  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeper.Enabled = TestLimitQueues.fileSweeper
  implicit val memorySweeperImplicitSweeper: Option[MemorySweeper.Both] = TestLimitQueues.memorySweeper
  implicit val groupBy: Option[GroupByInternal.KeyValues] = randomGroupByOption(keyValuesCount)
  implicit val skipListMerger = LevelZeroSkipListMerger

  "acquireLock" should {
    "create a lock file for only the root directory and not allow more locks" in {
      //memory databases do not perform locks
      if (persistent) {
        val otherDirs = (0 to randomIntMax(5)) map (_ => Dir(randomDir, 1))
        val storage =
          LevelStorage.Persistent(
            mmapSegmentsOnWrite = randomBoolean(),
            mmapSegmentsOnRead = randomBoolean(),
            dir = randomDir,
            otherDirs = otherDirs
          )

        val lock = Level.acquireLock(storage).runRandomIO.right.value
        lock shouldBe defined
        //other directories do not have locks.
        storage.otherDirs foreach {
          dir =>
            IOEffect.exists(dir.path.resolve("LOCK")) shouldBe false
        }

        //trying to lock again should fail
        Level.acquireLock(storage).left.runRandomIO.right.value.exception shouldBe a[OverlappingFileLockException]

        //closing the lock should allow re-locking
        lock.get.close()
        Level.acquireLock(storage).runRandomIO.right.value shouldBe defined
      }
    }
  }

  "apply" should {
    "create level" in {
      val level = TestLevel()
      if (memory) {
        //memory level always have one folder
        level.dirs should have size 1
        level.existsOnDisk shouldBe false
        level.inMemory shouldBe true
        level.mmapSegmentsOnRead shouldBe false
        level.mmapSegmentsOnWrite shouldBe false
        level.valuesConfig.compressDuplicateValues shouldBe true
      } else {
        level.existsOnDisk shouldBe true
        level.inMemory shouldBe false

        //there shouldBe at least one path
        level.dirs should not be empty

        //appendix path gets added to the head path
        val appendixPath = level.paths.headPath.resolve("appendix")
        appendixPath.exists shouldBe true
        appendixPath.resolve("0.log").exists shouldBe true

        //all paths should exists
        level.dirs.foreach(_.path.exists shouldBe true)
      }

      level.segmentsInLevel() shouldBe empty
      level.removeDeletedRecords shouldBe true

      level.delete.runRandomIO.right.value
      level.existsOnDisk shouldBe false
    }

    "report error if appendix file and folder does not exists" in {
      if (persistent) {
        //create a non empty level
        val level = TestLevel()
        val segment = TestSegment(randomKeyValues(keyValuesCount)).right.value

        level.put(segment).right.right.value.right.value

        //delete the appendix file
        level.paths.headPath.resolve("appendix").files(Extension.Log) map IOEffect.delete
        //expect failure when file does not exists
        level.tryReopen.left.get.exception shouldBe a[IllegalStateException]

        //delete folder
        IOEffect.delete(level.paths.headPath.resolve("appendix")).runRandomIO.right.value
        //expect failure when folder does not exist
        level.tryReopen.left.get.exception shouldBe a[IllegalStateException]

        level.delete.runRandomIO.right.value
      }
    }
  }

  "deleteUncommittedSegments" should {
    "delete segments that are not in the appendix" in {
      if (memory) {
        // memory Level do not have uncommitted Segments
      } else {
        val level = TestLevel()
        level.putKeyValuesTest(randomPutKeyValues()).runRandomIO.right.value
        val segmentsIdsBeforeInvalidSegments = level.segmentFilesOnDisk
        segmentsIdsBeforeInvalidSegments should have size 1

        val currentSegmentId = segmentsIdsBeforeInvalidSegments.head.fileId.runRandomIO.right.value._1

        //create 3 invalid segments in all the paths of the Level
        level.dirs.foldLeft(currentSegmentId) {
          case (currentSegmentId, dir) =>
            TestSegment(path = dir.path.resolve((currentSegmentId + 1).toSegmentFileId)).runRandomIO.right.value
            TestSegment(path = dir.path.resolve((currentSegmentId + 2).toSegmentFileId)).runRandomIO.right.value
            TestSegment(path = dir.path.resolve((currentSegmentId + 3).toSegmentFileId)).runRandomIO.right.value
            currentSegmentId + 3
        }
        //every level folder has 3 uncommitted Segments plus 1 valid Segment
        level.segmentFilesOnDisk should have size (level.dirs.size * 3) + 1

        Level.deleteUncommittedSegments(level.dirs, level.segmentsInLevel()).runRandomIO.right.value

        level.segmentFilesOnDisk should have size 1
        level.segmentFilesOnDisk should contain only segmentsIdsBeforeInvalidSegments.head
        level.reopen.segmentFilesOnDisk should contain only segmentsIdsBeforeInvalidSegments.head

        level.delete.runRandomIO.right.value
      }
    }
  }

  "largestSegmentId" should {
    "value the largest segment in the Level when the Level is not empty" in {
      val level = TestLevel(segmentSize = 1.kb)
      level.putKeyValuesTest(randomizedKeyValues(2000)).runRandomIO.right.value

      val largeSegmentId = Level.largestSegmentId(level.segmentsInLevel())
      largeSegmentId shouldBe level.segmentsInLevel().map(_.path.fileId.runRandomIO.right.value._1).max

      level.delete.runRandomIO.right.value
    }

    "return 0 when the Level is empty" in {
      val level = TestLevel(segmentSize = 1.kb)
      Level.largestSegmentId(level.segmentsInLevel()) shouldBe 0

      level.delete.runRandomIO.right.value
    }
  }

  "optimalSegmentsToPushForward" should {
    "return empty if there Levels are empty" in {
      val nextLevel = TestLevel()
      val level = TestLevel()
      implicit val reserve = ReserveRange.create[Unit]()

      Level.optimalSegmentsToPushForward(
        level = level,
        nextLevel = nextLevel,
        take = 10
      ) shouldBe Level.emptySegmentsToPush

      level.close.runRandomIO.right.value
      nextLevel.close.runRandomIO.right.value

      level.delete.runRandomIO.right.value
    }

    "return all Segments to copy if next Level is empty" in {
      val nextLevel = TestLevel()
      val level = TestLevel(keyValues = randomizedKeyValues(count = 10000, startId = Some(1)), segmentSize = 1.kb)
//      level.segmentsCount() should be >= 2

      implicit val reserve = ReserveRange.create[Unit]()

      val (toCopy, toMerge) =
        Level.optimalSegmentsToPushForward(
          level = level,
          nextLevel = nextLevel,
          take = 10
        )

      toMerge shouldBe empty
      toCopy.map(_.path) shouldBe level.segmentsInLevel().take(10).map(_.path)

      level.close.runRandomIO.right.value
      nextLevel.close.runRandomIO.right.value
    }

    "return all unreserved Segments to copy if next Level is empty" in {
      val nextLevel = TestLevel()
      val level = TestLevel(keyValues = randomizedKeyValues(count = 10000, startId = Some(1)), segmentSize = 1.kb)
      level.segmentsCount() should be >= 2

      implicit val reserve = ReserveRange.create[Unit]()
      val firstSegment = level.segmentsInLevel().head

      ReserveRange.reserveOrGet(firstSegment.minKey, firstSegment.maxKey.maxKey, firstSegment.maxKey.inclusive, ()) shouldBe empty //reserve first segment

      val (toCopy, toMerge) =
        Level.optimalSegmentsToPushForward(
          level = level,
          nextLevel = nextLevel,
          take = 10
        )

      toMerge shouldBe empty
      toCopy.map(_.path) shouldBe level.segmentsInLevel().drop(1).take(10).map(_.path)

      level.delete.runRandomIO.right.value
      nextLevel.delete.runRandomIO.right.value
    }
  }

  "optimalSegmentsToCollapse" should {
    "return empty if there Levels are empty" in {
      val level = TestLevel()
      implicit val reserve = ReserveRange.create[Unit]()

      Level.optimalSegmentsToCollapse(
        level = level,
        take = 10
      ) shouldBe empty

      level.delete.runRandomIO.right.value
    }

    "return empty if all segments were reserved" in {
      val keyValues = randomizedKeyValues(count = 10000, startId = Some(1))
      val level = TestLevel(keyValues = keyValues, segmentSize = 1.kb)
      level.segmentsCount() should be >= 2

      implicit val reserve = ReserveRange.create[Unit]()

      val minKey = keyValues.head.key
      val maxKey = Segment.minMaxKey(level.segmentsInLevel()).get
      ReserveRange.reserveOrGet(minKey, maxKey._2, maxKey._3, ()) shouldBe empty

      Level.optimalSegmentsToCollapse(
        level = level,
        take = 10
      ) shouldBe empty

      level.delete.runRandomIO.right.value
    }
  }

  "reserve" should {
    "reserve keys for compaction where Level is empty" in {
      val level = TestLevel()
      val keyValues = randomizedKeyValues(keyValuesCount).groupedSlice(2).map(_.updateStats)
      val segment1 = TestSegment(keyValues.head).runRandomIO.right.value
      val segment2 = TestSegment(keyValues.last).runRandomIO.right.value
      level.reserve(Seq(segment1, segment2)).get shouldBe IO.Right[Promise[Unit], Slice[Byte]](keyValues.head.head.key)(IO.ExceptionHandler.PromiseUnit)

      //cannot reserve again
      level.reserve(Seq(segment1, segment2)).get shouldBe a[IO.Left[_, _]]
      level.reserve(Seq(segment1)).get shouldBe a[IO.Left[_, _]]
      level.reserve(Seq(segment2)).get shouldBe a[IO.Left[_, _]]

      level.delete.runRandomIO.right.value
    }

    "return completed Future for empty Segments" in {
      val level = TestLevel()
      level.reserve(Seq.empty).get.left.get.isCompleted shouldBe true

      level.delete.runRandomIO.right.value
    }

    "reserve min and max keys" in {
      val level = TestLevel()
      val keyValues = randomizedKeyValues(keyValuesCount).groupedSlice(2).map(_.updateStats)
      val segments = Seq(TestSegment(keyValues.head).runRandomIO.right.value, TestSegment(keyValues.last).runRandomIO.right.value)
      level.put(segments).right.right.value.right.value

      level.delete.runRandomIO.right.value
    }
  }

  "buildNewMapEntry" should {
    import swaydb.core.map.serializer.AppendixMapEntryWriter._

    "build MapEntry.Put map for the first created Segment" in {
      val level = TestLevel()

      val segments = TestSegment(Slice(Transient.put(1, "value1"), Transient.put(2, "value2")).updateStats).runRandomIO.right.value
      val actualMapEntry = level.buildNewMapEntry(Slice(segments), originalSegmentMayBe = None, initialMapEntry = None).runRandomIO.right.value
      val expectedMapEntry = MapEntry.Put[Slice[Byte], Segment](segments.minKey, segments)

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])

      level.delete.runRandomIO.right.value
    }

    "build MapEntry.Put map for the newly merged Segments and not add MapEntry.Remove map " +
      "for original Segment as it's minKey is replace by one of the new Segment" in {
      val level = TestLevel()

      val originalSegment = TestSegment(Slice(Transient.put(1, "value"), Transient.put(5, "value")).updateStats).runRandomIO.right.value
      val mergedSegment1 = TestSegment(Slice(Transient.put(1, "value"), Transient.put(5, "value")).updateStats).runRandomIO.right.value
      val mergedSegment2 = TestSegment(Slice(Transient.put(6, "value"), Transient.put(10, "value")).updateStats).runRandomIO.right.value
      val mergedSegment3 = TestSegment(Slice(Transient.put(11, "value"), Transient.put(15, "value")).updateStats).runRandomIO.right.value

      val actualMapEntry = level.buildNewMapEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), Some(originalSegment), initialMapEntry = None).runRandomIO.right.value

      val expectedMapEntry =
        MapEntry.Put[Slice[Byte], Segment](1, mergedSegment1) ++
          MapEntry.Put[Slice[Byte], Segment](6, mergedSegment2) ++
          MapEntry.Put[Slice[Byte], Segment](11, mergedSegment3)

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])

      level.delete.runRandomIO.right.value
    }

    "build MapEntry.Put map for the newly merged Segments and also add Remove map entry for original map when all minKeys are unique" in {
      val level = TestLevel()

      val originalSegment = TestSegment(Slice(Transient.put(0, "value"), Transient.put(5, "value")).updateStats).runRandomIO.right.value
      val mergedSegment1 = TestSegment(Slice(Transient.put(1, "value"), Transient.put(5, "value")).updateStats).runRandomIO.right.value
      val mergedSegment2 = TestSegment(Slice(Transient.put(6, "value"), Transient.put(10, "value")).updateStats).runRandomIO.right.value
      val mergedSegment3 = TestSegment(Slice(Transient.put(11, "value"), Transient.put(15, "value")).updateStats).runRandomIO.right.value

      val expectedMapEntry =
        MapEntry.Put[Slice[Byte], Segment](1, mergedSegment1) ++
          MapEntry.Put[Slice[Byte], Segment](6, mergedSegment2) ++
          MapEntry.Put[Slice[Byte], Segment](11, mergedSegment3) ++
          MapEntry.Remove[Slice[Byte]](0)

      val actualMapEntry = level.buildNewMapEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), Some(originalSegment), initialMapEntry = None).runRandomIO.right.value

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])

      level.delete.runRandomIO.right.value
    }
  }
}
