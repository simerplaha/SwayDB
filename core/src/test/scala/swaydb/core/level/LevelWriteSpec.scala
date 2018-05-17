/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
import swaydb.core.actor.TestActor
import swaydb.core.data._
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.level.LevelException.ReceivedKeyValuesToMergeWithoutTargetSegment
import swaydb.core.level.actor.LevelAPI
import swaydb.core.level.actor.LevelCommand.{PushSegments, PushSegmentsResponse}
import swaydb.core.level.zero.LevelZeroSkipListMerge
import swaydb.core.map.{Map, MapEntry}
import swaydb.core.segment.Segment
import swaydb.core.util.{Extension, IDGenerator}
import swaydb.core.util.FileUtil._
import swaydb.core.util.PipeOps._
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.compaction.Throttle
import swaydb.data.config.Dir
import swaydb.data.slice.Slice
import swaydb.data.storage.LevelStorage
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.{Random, Success, Try}

//@formatter:off
class LevelWriteSpec0 extends LevelWriteSpec

class LevelWriteSpec1 extends LevelWriteSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelWriteSpec2 extends LevelWriteSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelWriteSpec3 extends LevelWriteSpec {
  override def inMemoryStorage = true
}
//@formatter:on

trait LevelWriteSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  //    override def deleteFiles: Boolean =
  //      false

  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: (Persistent, Segment) => Unit = TestLimitQueues.keyValueLimiter
  implicit val skipListMerger = LevelZeroSkipListMerge

  "Level" should {
    "initialise" in {
      val level = TestLevel()
      if (memory) {
        //memory level always have one folder
        level.dirs should have size 1
        level.existsOnDisk shouldBe false
        level.inMemory shouldBe true
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
    }

    "report error if appendix file and folder does not exists" in {
      if (persistent) {
        //create a non empty level
        val level = TestLevel()
        level.put(TestSegment(randomIntKeyValues(keyValuesCount)).assertGet).assertGet

        //delete the appendix file
        level.paths.headPath.resolve("appendix").files(Extension.Log) map IO.delete
        //expect failure when file does not exists
        level.tryReopen.failed.assertGet shouldBe a[IllegalStateException]

        //delete folder
        IO.delete(level.paths.headPath.resolve("appendix")).assertGet
        //expect failure when folder does not exist
        level.tryReopen.failed.assertGet shouldBe a[IllegalStateException]
      }
    }
  }

  "Level.deleteUncommittedSegments" should {
    "delete segments that are not in the appendix" in {
      if (memory) {
        // memory Level do not have uncommitted Segments
      } else {
        val level = TestLevel()
        level.putKeyValues(randomIntKeyValuesMemory()).assertGet
        val segmentsIdsBeforeInvalidSegments = level.segmentFilesOnDisk
        segmentsIdsBeforeInvalidSegments should have size 1

        val currentSegmentId = segmentsIdsBeforeInvalidSegments.head.fileId.assertGet._1

        //create 3 invalid segments in all the paths of the Level
        level.dirs.foldLeft(currentSegmentId) {
          case (currentSegmentId, dir) =>
            TestSegment(path = dir.path.resolve((currentSegmentId + 1).toSegmentFileId)).assertGet
            TestSegment(path = dir.path.resolve((currentSegmentId + 2).toSegmentFileId)).assertGet
            TestSegment(path = dir.path.resolve((currentSegmentId + 3).toSegmentFileId)).assertGet
            currentSegmentId + 3
        }
        //every level folder has 3 uncommitted Segments plus 1 valid Segment
        level.segmentFilesOnDisk should have size (level.dirs.size * 3) + 1

        val function = PrivateMethod[Unit]('deleteUncommittedSegments)
        level invokePrivate function()

        level.segmentFilesOnDisk should have size 1
        level.segmentFilesOnDisk should contain only segmentsIdsBeforeInvalidSegments.head
        level.reopen.segmentFilesOnDisk should contain only segmentsIdsBeforeInvalidSegments.head
      }
    }
  }

  "Level.currentLargestSegmentId" should {
    "get the largest segment in the Level when the Level is not empty" in {
      val level = TestLevel(segmentSize = 1.kb)
      level.putKeyValues(randomizedIntKeyValues(2000)).assertGet

      val function = PrivateMethod[Long]('largestSegmentId)
      val largeSegmentId = level invokePrivate function()

      largeSegmentId shouldBe level.segmentsInLevel().map(_.path.fileId.assertGet._1).max
    }

    "return 0 when the Level is empty" in {
      val level = TestLevel(segmentSize = 1.kb)

      val function = PrivateMethod[Int]('largestSegmentId)
      (level invokePrivate function()) shouldBe 0
    }
  }

  "Level.forward" should {
    "forward request to lower level only if push forward is true and the current level and lower level is empty" in {
      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times
      nextLevel.isEmpty _ expects() returning true

      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case request @ PushSegments(segments, replyTo) =>
              segments should have size 1
              //return successful response and expect upper level to have deleted the Segments
              replyTo ! PushSegmentsResponse(request, Success())
          }
      }

      val level = TestLevel(segmentSize = 1.kb, pushForward = true, nextLevel = Some(nextLevel), throttle = (_) => Throttle(Duration.Zero, 0))

      val replyTo = TestActor[PushSegmentsResponse]()
      val request = PushSegments(Seq(TestSegment().assertGet), replyTo)

      level.forward(request).assertGet

      replyTo.getMessage().result.assertGet
    }

    "fail forward if the current Level is the last Level" in {
      val level = TestLevel(segmentSize = 1.kb, pushForward = true, nextLevel = None)

      val replyTo = TestActor[PushSegmentsResponse]()
      val request = PushSegments(Seq(TestSegment().assertGet), replyTo)
      level.forward(request).failed.assertGet shouldBe LevelException.NotSentToNextLevel
    }

    "fail forward if push forward is false" in {
      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times

      val level = TestLevel(segmentSize = 1.kb, pushForward = false, nextLevel = Some(nextLevel))

      val replyTo = TestActor[PushSegmentsResponse]()
      val request = PushSegments(Seq(TestSegment().assertGet), replyTo)
      level.forward(request).failed.assertGet shouldBe LevelException.NotSentToNextLevel
    }

    "fail forward if lower level is not empty" in {
      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times
      nextLevel.isEmpty _ expects() returning false

      val level = TestLevel(segmentSize = 1.kb, pushForward = true, nextLevel = Some(nextLevel))

      val replyTo = TestActor[PushSegmentsResponse]()
      val request = PushSegments(Seq(TestSegment().assertGet), replyTo)
      level.forward(request).failed.assertGet shouldBe LevelException.NotSentToNextLevel
    }

  }

  "Level.put segments" should {
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
      val level = TestLevel(throttle = (_) => Throttle(Duration.Zero, 0))
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
      val level = TestLevel(throttle = (_) => Throttle(Duration.Zero, 0))
      val allKeyValues = randomIntKeyValuesMemory(keyValuesCount * 3, valueSize = Some(1000))
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

        def assertDistribution = {
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
        val keyValues = randomIntKeyValuesMemory(keyValuesCount)

        val level = TestLevel(throttle = (_) => Throttle(Duration.Zero, 0), segmentSize = 1.byte, levelStorage = storage)

        level.putKeyValues(keyValues).assertGet
        level.segmentsCount() shouldBe 100
        assertDistribution

        //write the same key-values again so that all Segments are updated. This should still maintain the Segment distribution
        level.putKeyValues(keyValues).assertGet
        assertDistribution

        //shuffle key-values should still maintain distribution order
        Random.shuffle(keyValues.grouped(10)).foreach {
          keyValues =>
            level.putKeyValues(keyValues).assertGet
        }
        assertDistribution

        //delete some key-values
        Random.shuffle(keyValues.grouped(10)).take(2).foreach {
          keyValues =>
            val deleteKeyValues = keyValues.map(keyValue => Memory.Remove(keyValue.key)).toSlice
            level.putKeyValues(deleteKeyValues).assertGet
        }

        level.putKeyValues(keyValues).assertGet
        assertDistribution
      }
    }

    "fail when writing a deleted segment" in {
      val level = TestLevel()

      val keyValues = randomIntKeyStringValues()
      val segment = TestSegment(keyValues).assertGet
      segment.delete.assertGet

      val result = level.put(segment).failed.assertGet
      if (persistent)
        result shouldBe a[NoSuchFileException]
      else
        result shouldBe a[Exception]

      level.isEmpty shouldBe true

      //if it's a persistent Level, reopen to ensure that Segment did not get committed.
      if (persistent) level.reopen.isEmpty shouldBe true
    }

    "return failure if segmentToMerge has no target Segment" in {
      val keyValues = randomIntKeyValues(keyValuesCount)
      val segmentsToMerge = TestSegment(keyValues).assertGet
      val level = TestLevel()
      level.put(Seq(segmentsToMerge), Seq(), Seq()).failed.assertGet shouldBe ReceivedKeyValuesToMergeWithoutTargetSegment(keyValues.size)
    }

    "copy Segments if segmentsToMerge is empty" in {
      val keyValues = randomIntKeyValues(keyValuesCount).groupedSlice(5).map(_.updateStats)
      val segmentToCopy = keyValues map (keyValues => TestSegment(keyValues).assertGet)

      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      level.put(Seq.empty, segmentToCopy, Seq.empty).assertGet

      level.isEmpty shouldBe false
      assertReads(keyValues.flatten, level)
    }

    "revert copy on failure" in {
      if (persistent) {
        val keyValues = randomIntKeyValues(keyValuesCount).groupedSlice(5).map(_.updateStats)
        val segmentToCopy = keyValues map (keyValues => TestSegment(keyValues).assertGet)

        val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

        //create a file with the same Segment name as the 4th Segment file. This should result in failure.
        val id = IDGenerator.segmentId(level.segmentIDGenerator.nextID + 4)
        level.paths.queuedPaths foreach { //create this file in all paths.
          _ =>
            Files.createFile(level.paths.next.resolve(id))

        }
        val levelFilesBeforePut = level.segmentFilesOnDisk

        level.put(Seq.empty, segmentToCopy, Seq.empty).failed.assertGet shouldBe a[FileAlreadyExistsException]

        level.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe levelFilesBeforePut
      }
    }

    "copy and merge Segments" in {
      val keyValues = randomIntKeyValues(100).groupedSlice(10).map(_.updateStats).toArray
      val segmentToCopy = keyValues.take(5) map (keyValues => TestSegment(keyValues).assertGet)
      val segmentToMerge = keyValues.drop(5).take(4) map (keyValues => TestSegment(keyValues).assertGet)
      val targetSegment = TestSegment(keyValues.last).assertGet

      val level = TestLevel(nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
      level.put(targetSegment).assertGet
      level.put(segmentToMerge, segmentToCopy, Seq(targetSegment)).assertGet

      level.isEmpty shouldBe false

      assertGet(keyValues.flatten, level)
    }

    "revert copy if merge fails" in {
      if (persistent) {
        val keyValues = randomIntKeyValues(100).groupedSlice(10).map(_.updateStats).toArray
        val segmentToCopy = keyValues.take(5) map (keyValues => TestSegment(keyValues).assertGet)
        val segmentToMerge = keyValues.drop(5).take(4) map (keyValues => TestSegment(keyValues).assertGet)
        val targetSegment = TestSegment(keyValues.last).assertGet

        val level = TestLevel(segmentSize = 150.bytes, nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))
        level.put(targetSegment).assertGet

        //segment to copy
        val id = IDGenerator.segmentId(level.segmentIDGenerator.nextID + 9)
        level.paths.queuedPaths foreach { //create this file in all paths.
          _ =>
            Files.createFile(level.paths.next.resolve(id))

        }

        val appendixBeforePut = level.segmentsInLevel()
        val levelFilesBeforePut = level.segmentFilesOnDisk
        level.put(segmentToMerge, segmentToCopy, Seq(targetSegment)).failed.assertGet shouldBe a[FileAlreadyExistsException]
        level.segmentFilesOnDisk shouldBe levelFilesBeforePut
        level.segmentsInLevel().map(_.path) shouldBe appendixBeforePut.map(_.path)
      }
    }
  }

  "Level.putMap" should {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader._
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val merged = LevelZeroSkipListMerge(10.seconds)

    val map =
      if (persistent)
        Map.persistent[Slice[Byte], Memory](randomIntDirectory, true, true, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      else
        Map.memory[Slice[Byte], Memory]()

    val keyValues = randomIntKeyValuesMemory(keyValuesCount, addRandomRemoves = true)
    keyValues foreach {
      keyValue =>
        map.write(MapEntry.Put(keyValue.key, keyValue))
    }

    "create a segment to an empty Level with no lower level" in {
      val level = TestLevel()
      level.putMap(map).assertGet
      //since this is a new Segment and Level has no sub-level, all the deleted key-values will get removed.
      val (deletedKeyValues, otherKeyValues) = keyValues.partition(_.isInstanceOf[Memory.Remove])

      assertReads(otherKeyValues, level)

      //deleted key-values do not exist.
      deletedKeyValues foreach {
        deleted =>
          level.get(deleted.key).assertGetOpt shouldBe empty
      }
    }

    "create a segment to a non empty Level with no lower level" in {
      val level = TestLevel()

      //creating a Segment with existing string key-values
      val existingKeyValues = Array(Memory.Put("one", "one"), Memory.Put("two", "two"), Memory.Put("three", "three"))

      val sortedExistingKeyValues =
        Slice(
          Array(
            //also randomly set expired deadline for Remove.
            Memory.Put("one", "one"), Memory.Put("two", "two"), Memory.Put("three", "three"), Memory.Remove("four", randomly(expiredDeadline()))
          ).sorted(ordering.on[KeyValue](_.key)))

      level.putKeyValues(sortedExistingKeyValues).assertGet

      //put a new map
      level.putMap(map).assertGet
      assertGet(keyValues.filterNot(_.isInstanceOf[Memory.Remove]), level)

      level.get("one").assertGet shouldBe existingKeyValues(0)
      level.get("two").assertGet shouldBe existingKeyValues(1)
      level.get("three").assertGet shouldBe existingKeyValues(2)
      level.get("four").assertGetOpt shouldBe empty
    }
  }

  "Level.put KeyValues" should {
    "write a key-values to the Level" in {
      val level = TestLevel(throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValuesMemory()
      level.putKeyValues(keyValues).assertGet

      level.putKeyValues(Slice(keyValues.head)).assertGet

      if (persistent) {
        val reopen = level.reopen
        eventual(assertReads(keyValues, reopen))
      }
    }

    "return an empty level if all the key values in the Level were REMOVED and if Level is the only Level" in {
      val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValuesMemory(keyValuesCount)
      level.putKeyValues(keyValues).assertGet

      val deleteKeyValues = Slice.create[KeyValue.ReadOnly](keyValues.size * 2)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Memory.Remove(keyValue.key)
      }
      //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
      //these delete keys should also be removed
      val lastKeyValuesId = keyValues.last.key.read[Int] + 1
      (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
        id =>
          deleteKeyValues add Memory.Remove(id, randomly(expiredDeadline()))
      }

      level.putKeyValues(deleteKeyValues).assertGet
      level.segmentFilesInAppendix shouldBe 0

      level.isEmpty shouldBe true
      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were REMOVED but it has lower level" in {
      val level = TestLevel(nextLevel = Some(TestLevel()))

      val keyValues = randomIntKeyValuesMemory()
      level.putKeyValues(keyValues).assertGet

      val deleteKeyValues = Slice.create[KeyValue.ReadOnly](keyValues.size)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Memory.Remove(keyValue.key)
      }

      level.putKeyValues(deleteKeyValues).assertGet
      level.isEmpty shouldBe false
      keyValues foreach {
        keyValue =>
          level.get(keyValue.key).assertGetOpt shouldBe empty
      }
    }

    "return an empty level if all the key values in the Level were REMOVED by RANGE and if Level is the only Level" in {
      val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValuesMemory(keyValuesCount)
      level.putKeyValues(keyValues).assertGet

      level.putKeyValues(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, None, Value.Remove(None)))).assertGet
      level.segmentFilesInAppendix shouldBe 0

      level.isEmpty shouldBe true
      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were REMOVED by RANGE but it has a lower Level" in {
      val level = TestLevel(segmentSize = 1.kb, nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValuesMemory(keyValuesCount)
      level.putKeyValues(keyValues).assertGet
      val segmentsCountBeforeRemove = level.segmentFilesInAppendix

      level.putKeyValues(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, None, Value.Remove(None)))).assertGet
      level.segmentFilesInAppendix shouldBe segmentsCountBeforeRemove

      level.isEmpty shouldBe false
      if (persistent) {
        level.reopen.isEmpty shouldBe false
        level.segmentFilesOnDisk should have size segmentsCountBeforeRemove
      }
    }

    "return an empty level if all the key values in the Level were EXPIRED and if Level is the only Level" in {
      val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValuesMemory(keyValuesCount)
      level.putKeyValues(keyValues).assertGet

      val deleteKeyValues = Slice.create[KeyValue.ReadOnly](keyValues.size * 2)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Memory.Remove(keyValue.key, 1.seconds)
      }
      //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
      //these delete keys should also be removed
      val lastKeyValuesId = keyValues.last.key.read[Int] + 1
      (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
        id =>
          deleteKeyValues add Memory.Remove(id, randomly(expiredDeadline()))
      }

      level.nextLevel shouldBe empty

      level.putKeyValues(deleteKeyValues).assertGet

      //expired key-values return empty after 2.seconds
      eventual(5.seconds) {
        keyValues foreach {
          keyValue =>
            level.get(keyValue.key).assertGetOpt shouldBe empty
        }
      }

      eventual(5.seconds) {
        level.segmentFilesInAppendix shouldBe 0
      }

      level.isEmpty shouldBe true

      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were EXPIRED and if Level has a lower Level" in {
      val level = TestLevel(segmentSize = 1.kb, nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValuesMemory(keyValuesCount)
      level.putKeyValues(keyValues).assertGet

      val deleteKeyValues = Slice.create[KeyValue.ReadOnly](keyValues.size * 2)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Memory.Remove(keyValue.key, 0.seconds)
      }
      //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
      //these delete keys should also be removed
      val lastKeyValuesId = keyValues.last.key.read[Int] + 1
      (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
        id =>
          deleteKeyValues add Memory.Remove(id, randomly(expiredDeadline()))
      }

      level.putKeyValues(deleteKeyValues).assertGet

      //expired key-values return empty.
      keyValues foreach {
        keyValue =>
          level.get(keyValue.key).assertGetOpt shouldBe empty
      }

      //sleep for 2.seconds and Segments should still exists.
      sleep(2.seconds)
      level.isEmpty shouldBe false
      level.segmentFilesInAppendix should be >= 1

      if (persistent) {
        level.reopen.isEmpty shouldBe false
        level.segmentFilesOnDisk.size should be >= 1
      }
    }

    "return an empty level if all the key values in the Level were EXPIRED by RANGE and if Level is the only Level" in {
      val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValuesMemory(keyValuesCount)
      level.putKeyValues(keyValues).assertGet

      level.putKeyValues(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, None, Value.Remove(2.seconds.fromNow)))).assertGet

      //expired key-values return empty after 2.seconds
      eventual(5.seconds) {
        keyValues foreach {
          keyValue =>
            level.get(keyValue.key).assertGetOpt shouldBe empty
        }
      }

      eventual(5.seconds) {
        level.segmentFilesInAppendix shouldBe 0
      }

      level.isEmpty shouldBe true

      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were EXPIRED by RANGE and if Level has a last Level" in {
      val level = TestLevel(segmentSize = 1.kb, nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValuesMemory(keyValuesCount)
      level.putKeyValues(keyValues).assertGet

      level.putKeyValues(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, None, Value.Remove(2.seconds.fromNow)))).assertGet

      //expired key-values return empty after 2.seconds
      eventual(5.seconds) {
        keyValues foreach {
          keyValue =>
            level.get(keyValue.key).assertGetOpt shouldBe empty
        }
      }

      level.segmentFilesInAppendix should be >= 1

      level.isEmpty shouldBe false

      if (persistent) {
        level.reopen.isEmpty shouldBe false
        level.segmentFilesOnDisk.size should be >= 1
      }
    }
  }

  "Level.collapseSmallSegments" should {
    "collapse small Segments to 50% of the size when the Segment's size was reduced by deleting 50% of it's key-values" in {
      //disable throttling so that it does not automatically collapse small Segments
      val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))
      val keyValues = randomIntKeyValuesMemory(1000)
      level.putKeyValues(keyValues).assertGet
      //dispatch another put request so that existing Segment gets split
      level.putKeyValues(Slice(keyValues.head)).assertGet

      val segmentCountBeforeDelete = level.segmentsCount()
      segmentCountBeforeDelete > 1 shouldBe true

      val keyValuesNoDeleted = ListBuffer.empty[KeyValue]
      val deleteEverySecond =
        keyValues.zipWithIndex flatMap {
          case (keyValue, index) =>
            if (index % 2 == 0)
              Some(Memory.Remove(keyValue.key))
            else {
              keyValuesNoDeleted += keyValue
              None
            }
        }
      //delete half of the key values which will create small Segments
      level.putKeyValues(Slice(deleteEverySecond.toArray)).assertGet

      level.collapseAllSmallSegments(1000).assertGet
      //since every second key-value was delete, the number of Segments is reduced to half
      level.segmentFilesInAppendix shouldBe (segmentCountBeforeDelete / 2)
      assertReads(Slice(keyValuesNoDeleted.toArray), level)

    }

    "collapse all small Segments into one of the existing small Segments, if the Segment was reopened with a larger segment size" in {
      if (memory) {
        //memory Level cannot be reopened.
      } else {
        //disable throttling so that it does not automatically collapse small Segments
        val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))

        val keyValues = randomIntKeyValuesMemory(1000)
        level.putKeyValues(keyValues).assertGet
        //dispatch another push to trigger split
        level.putKeyValues(Slice(keyValues.head)).assertGet

        level.segmentsCount() > 1 shouldBe true

        //reopen the Level with larger min segment size
        //      if (storageConfig.diskFileSystem) {
        val reopenLevel = level.reopen(segmentSize = 20.mb)
        reopenLevel.collapseAllSmallSegments(1000).assertGet

        //resulting segments is 1
        eventually {
          level.segmentFilesOnDisk should have size 1
        }
        //can still read Segments
        assertReads(keyValues, reopenLevel)
        val reopen2 = reopenLevel.reopen
        eventual(assertReads(keyValues, reopen2))
      }
    }
  }

  "Level.clearExpiredKeyValues" should {
    "clear expired key-values" in {
      //this test is similar as the above collapsing small Segment test.
      //Remove or expiring key-values should have the same result
      val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))
      val expiryAt = 2.seconds.fromNow
      val keyValues = randomIntKeyValuesMemory(1000, nonValue = true)
      level.putKeyValues(keyValues).assertGet
      //dispatch another put request so that existing Segment gets split
      level.putKeyValues(Slice(keyValues.head)).assertGet
      val segmentCountBeforeDelete = level.segmentsCount()
      segmentCountBeforeDelete > 1 shouldBe true

      val keyValuesNotExpired = ListBuffer.empty[KeyValue]
      val expireEverySecond =
        keyValues.zipWithIndex flatMap {
          case (keyValue, index) =>
            if (index % 2 == 0)
              Some(Memory.Remove(keyValue.key, expiryAt + index.millisecond))
            else {
              keyValuesNotExpired += keyValue
              None
            }
        }

      //delete half of the key values which will create small Segments
      level.putKeyValues(Slice(expireEverySecond.toArray)).assertGet

      sleep(5.seconds)
      level.collapseAllSmallSegments(1000).assertGet
      level.segmentFilesInAppendix shouldBe (segmentCountBeforeDelete / 2)

      assertReads(Slice(keyValuesNotExpired.toArray), level)
    }
  }

  "Level.copy" should {
    "copy segments" in {
      val level = TestLevel(throttle = (_) => Throttle(Duration.Zero, 0))
      level.isEmpty shouldBe true

      val keyValues1 = randomIntKeyStringValues()
      val keyValues2 = randomIntKeyStringValues()
      val segments = Iterable(TestSegment(keyValues1).assertGet, TestSegment(keyValues2).assertGet)
      val copiedSegments = level.copy(segments).assertGet

      val allKeyValues = Slice((keyValues1 ++ keyValues2).toArray).updateStats

      level.isEmpty shouldBe true //copy function does not write to appendix.

      if (persistent) level.segmentFilesOnDisk should not be empty

      Segment.getAllKeyValues(0.1, copiedSegments).assertGet shouldBe allKeyValues
    }

    "fail copying Segments if it failed to copy one of the Segments" in {
      val level = TestLevel()
      level.isEmpty shouldBe true

      val segment1 = TestSegment().assertGet
      val segment2 = TestSegment().assertGet

      segment2.delete.assertGet // delete segment2 so there is a failure in copying Segments

      val segments = Iterable(segment1, segment2)
      level.copy(segments).failed.assertGet shouldBe a[NoSuchFileException]

      level.isEmpty shouldBe true
      if (persistent) level.reopen.isEmpty shouldBe true
    }

    "copy Map" in {
      val level = TestLevel(throttle = (_) => Throttle(Duration.Zero, 0))
      level.isEmpty shouldBe true

      val keyValues = randomIntKeyValuesMemory(keyValuesCount)
      val copiedSegments = level.copy(TestMap(keyValues)).assertGet

      level.isEmpty shouldBe true //copy function does not write to appendix.

      if (persistent) level.segmentFilesOnDisk should not be empty

      Segment.getAllKeyValues(0.1, copiedSegments).assertGet shouldBe keyValues
    }

  }

  "Level.putKeyValues" should {
    "write key values to target segments and update appendix" in {
      val level = TestLevel(segmentSize = 10.mb, throttle = (_) => Throttle(Duration.Zero, 0))

      val targetSegmentKeyValues = randomIntKeyStringValues()
      val targetSegment = TestSegment(keyValues = targetSegmentKeyValues, path = testSegmentFile.resolveSibling("10.seg")).assertGet

      val keyValues = randomIntKeyValuesMemory()
      val function = PrivateMethod[Try[Unit]]('putKeyValues)
      (level invokePrivate function(keyValues, Seq(targetSegment), None)).assertGet

      targetSegment.existsOnDisk shouldBe false //target Segment should be deleted

      assertGet(keyValues, level)
      assertGet(targetSegmentKeyValues, level)
      level.takeSmallSegments(10) should not be empty //min segment size is 10.mb

      if (persistent) {
        val reopen = level.reopen
        assertGet(keyValues, reopen)
        assertGet(targetSegmentKeyValues, reopen)
      }
    }

    "fail put if writing one KeyValue fails" in {
      val level = TestLevel(segmentSize = 10.mb)

      val targetSegmentKeyValues = randomIntKeyStringValues()
      val targetSegment = TestSegment(keyValues = targetSegmentKeyValues).assertGet

      val keyValues: Slice[KeyValue] = Slice.create[KeyValue](3) //null KeyValue will throw an exception and the put should be reverted
      keyValues.add(Memory.Put(123))
      keyValues.add(Memory.Put(1234, 12345))
      keyValues.add(Persistent.Put(1235, None, null, 10, 10, 10, 10, 10)) //give it a null Reader so that it fails reading the value.

      val function = PrivateMethod[Try[Unit]]('putKeyValues)
      val failed = level invokePrivate function(keyValues, Iterable(targetSegment), None)
      failed.isFailure shouldBe true
      failed.failed.get shouldBe a[NullPointerException]

      level.get(123).assertGetOpt.isEmpty shouldBe true
      level.get(1234).assertGetOpt.isEmpty shouldBe true
    }
  }

  "Level.removeSegments" should {
    "remove segments from disk and remove them from appendix" in {
      val level = TestLevel(segmentSize = 1.kb)
      level.putKeyValues(randomIntKeyValuesMemory(keyValuesCount)).assertGet

      level.removeSegments(level.segmentsInLevel()).assertGet

      level.isEmpty shouldBe true

      if (persistent) {
        level.segmentFilesOnDisk shouldBe empty
        level.reopen.isEmpty shouldBe true
      }
    }

  }

  "Level.buildNewMapEntry" should {
    import swaydb.core.map.serializer.AppendixMapEntryWriter._

    "build MapEntry.Put map for the first created Segment" in {
      val level = TestLevel()

      val map = TestSegment(Slice(Transient.Put(1, "value1"), Transient.Put(2, "value2")).updateStats).assertGet
      val actualMapEntry = level.buildNewMapEntry(Slice(map), originalSegmentMayBe = None, initialMapEntry = None).assertGet
      val expectedMapEntry = MapEntry.Put[Slice[Byte], Segment](map.minKey, map)

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])
    }

    "build MapEntry.Put map for the newly merged Segments and not add MapEntry.Remove map " +
      "for original Segment as it's minKey is replace by one of the new Segment" in {
      val level = TestLevel()

      val originalSegment = TestSegment(Slice(Transient.Put(1, "value"), Transient.Put(5, "value")).updateStats).assertGet
      val mergedSegment1 = TestSegment(Slice(Transient.Put(1, "value"), Transient.Put(5, "value")).updateStats).assertGet
      val mergedSegment2 = TestSegment(Slice(Transient.Put(6, "value"), Transient.Put(10, "value")).updateStats).assertGet
      val mergedSegment3 = TestSegment(Slice(Transient.Put(11, "value"), Transient.Put(15, "value")).updateStats).assertGet

      val actualMapEntry = level.buildNewMapEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), Some(originalSegment), initialMapEntry = None).assertGet

      val expectedMapEntry =
        MapEntry.Put[Slice[Byte], Segment](1, mergedSegment1) ++
          MapEntry.Put[Slice[Byte], Segment](6, mergedSegment2) ++
          MapEntry.Put[Slice[Byte], Segment](11, mergedSegment3)

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])

    }

    "build MapEntry.Put map for the newly merged Segments and also add Remove map entry for original map when all minKeys are unique" in {
      val level = TestLevel()

      val originalSegment = TestSegment(Slice(Transient.Put(0, "value"), Transient.Put(5, "value")).updateStats).assertGet
      val mergedSegment1 = TestSegment(Slice(Transient.Put(1, "value"), Transient.Put(5, "value")).updateStats).assertGet
      val mergedSegment2 = TestSegment(Slice(Transient.Put(6, "value"), Transient.Put(10, "value")).updateStats).assertGet
      val mergedSegment3 = TestSegment(Slice(Transient.Put(11, "value"), Transient.Put(15, "value")).updateStats).assertGet

      val expectedMapEntry =
        MapEntry.Put[Slice[Byte], Segment](1, mergedSegment1) ++
          MapEntry.Put[Slice[Byte], Segment](6, mergedSegment2) ++
          MapEntry.Put[Slice[Byte], Segment](11, mergedSegment3) ++
          MapEntry.Remove[Slice[Byte]](0)

      val actualMapEntry = level.buildNewMapEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), Some(originalSegment), initialMapEntry = None).assertGet

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])
    }
  }

}