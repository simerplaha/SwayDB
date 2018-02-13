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

import java.nio.file.NoSuchFileException

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.core.{TestBase, TestQueues}
import swaydb.core.actor.TestActor
import swaydb.core.data.Transient.Delete
import swaydb.core.data._
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.level.actor.LevelAPI
import swaydb.core.level.actor.LevelCommand.{PushSegments, PushSegmentsResponse}
import swaydb.core.map.serializer.{KeyValuesMapSerializer, SegmentsMapSerializer}
import swaydb.core.map.{Map, MapEntry}
import swaydb.core.segment.Segment
import swaydb.core.util.Extension
import swaydb.core.util.FileUtil._
import swaydb.core.util.PipeOps._
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

class LevelWriteSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = TestQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: (PersistentReadOnly, Segment) => Unit = TestQueues.keyValueLimiter

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

      level.segments shouldBe empty
      level.removeDeletedRecords shouldBe true
    }

    "report error if appendix file and folder does not exists" in {
      if (persistent) {
        //create a non empty level
        val level = TestLevel()
        level.putKeyValues(randomIntKeyValues(keyValuesCount)).assertGet

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

  "Level.deleteOrphanSegments" should {
    "delete segments that are not in the appendix" in {
      if (memory) {
        // memory Level do not have orphan Segments
      } else {
        val level = TestLevel()
        level.putKeyValues(randomIntKeyValues()).assertGet
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
        //every level folder has 3 orphan Segments plus 1 valid Segment
        level.segmentFilesOnDisk should have size (level.dirs.size * 3) + 1

        val function = PrivateMethod[Unit]('deleteOrphanSegments)
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
      level.putKeyValues(randomIntKeyValues(2000)).assertGet

      val function = PrivateMethod[Long]('largestSegmentId)
      val largeSegmentId = level invokePrivate function()

      largeSegmentId shouldBe level.segments.map(_.path.fileId.assertGet._1).max
    }

    "return 0 when the Level empty" in {
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

    "fail forward if lower level does not exists" in {
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
      val level = TestLevel()
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
      val keyValues = randomIntKeyStringValues(keyValuesCount * 3, valueSize = 1000)

      //create a
      val (keyValues1, keyValues2, keyValues3) =
        keyValues
          .splitAt(keyValues.size / 3)
          .==> {
            case (split1, split2) =>
              val (two, three) = split2.splitAt(split2.size / 2)
              (split1.updateStats, two.updateStats, three.updateStats)
          }
      //create a level with key-values
      level.putKeyValues(keyValues2.updateStats).assertGet
      level.isEmpty shouldBe false

      val segments = Seq(TestSegment(keyValues1).assertGet, TestSegment(keyValues3).assertGet)
      level.put(segments).assertGet

      assertReads(keyValues, level)
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
        val keyValues = randomIntKeyStringValues(keyValuesCount, valueSize = 1000)

        val level = TestLevel(throttle = (_) => Throttle(Duration.Zero, 0), segmentSize = 1.byte, levelStorage = storage)

        level.putKeyValues(keyValues).assertGet
        level.segmentsCount() shouldBe 100
        assertDistribution

        //write the same key-values again so that all Segments are updated. This should still maintain the Segment distribution
        level.putKeyValues(keyValues).assertGet
        assertDistribution

        //shuffle key-values should still maintain distribution order
        Random.shuffle(keyValues.grouped(10).map(_.updateStats)).foreach {
          keyValues =>
            level.putKeyValues(keyValues).assertGet
        }
        assertDistribution

        //delete some key-values
        Random.shuffle(keyValues.grouped(10).map(_.updateStats)).take(2).foreach {
          keyValues =>
            val deleteKeyValues = Slice(keyValues.map(keyValue => Delete(keyValue.key)).toArray).updateStats
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

  }

  "Level.putMap" should {
    implicit val serializer = KeyValuesMapSerializer(ordering)

    val map =
      if (persistent)
        Map.persistent(randomIntDirectory, true, true, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      else
        Map.memory()

    val keyValues = randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true)
    keyValues foreach {
      keyValue =>
        val valueType = if (keyValue.isDelete) ValueType.Remove else ValueType.Add
        map.add(keyValue.key, (valueType, keyValue.getOrFetchValue.assertGetOpt))
    }

    "create a segment to an empty Level with no lower level" in {
      val level = TestLevel()
      level.putMap(map).assertGet
      //since this is a new Segment and Level has no sub-level, all the deleted key-values will get removed.
      assertReads(keyValues.filterNot(_.isDelete), level)

      //deleted key-values do not exist.
      keyValues.filter(_.isDelete) foreach {
        deleted =>
          level.get(deleted.key).assertGetOpt shouldBe empty
      }
    }

    "create a segment to a non empty Level with no lower level" in {
      val level = TestLevel()

      //creating a Segment with existing string key-values
      val existingKeyValues = Array(KeyValue("one", "one"), KeyValue("two", "two"), KeyValue("three", "three"), Delete("four"))
      val sortedExistingKeyValues =
        Slice(Array(KeyValue("one", "one"), KeyValue("two", "two"), KeyValue("three", "three"), Delete("four"))
          .sorted(ordering.on[KeyValue](_.key)))
          .updateStats
      level.putKeyValues(sortedExistingKeyValues).assertGet

      //put a new map
      level.putMap(map).assertGet
      assertGet(keyValues.filter(_.notDelete), level)

      level.get("one").assertGetOpt shouldBe existingKeyValues(0)
      level.get("two").assertGetOpt shouldBe existingKeyValues(1)
      level.get("three").assertGetOpt shouldBe existingKeyValues(2)
      level.get("four").assertGetOpt shouldBe empty
    }
  }

  "Level.put KeyValues" should {
    "write a key-values to the Level" in {
      val level = TestLevel()

      val keyValues = randomIntKeyStringValues()
      level.putKeyValues(keyValues).assertGet

      level.putKeyValues(Slice(keyValues.head)).assertGet

      if (persistent)
        assertReads(keyValues, level.reopen)
    }

    "return an empty level if all the key values in the Level were deleted and if Level is the only Level" in {
      val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyStringValues(keyValuesCount)
      level.putKeyValues(keyValues).assertGet
      //do another put so a merge occurs resulting in split
      level.putKeyValues(Slice(keyValues.head)).assertGet

      val deleteKeyValues = Slice.create[KeyValue](keyValues.size * 2)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Delete(keyValue.key, previous = deleteKeyValues.lastOption, falsePositiveRate = 0.1)
      }
      //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
      //these delete keys should also be removed
      val lastKeyValuesId = keyValues.last.key.read[Int] + 1
      (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
        id =>
          deleteKeyValues add Delete(id, previous = deleteKeyValues.lastOption, falsePositiveRate = 0.1)
      }

      level.putKeyValues(deleteKeyValues).assertGet
      level.segmentFilesInAppendix shouldBe 0

      level.isEmpty shouldBe true
      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were deleted but it has lower level" in {
      val level = TestLevel(nextLevel = Some(TestLevel()))

      val keyValues = randomIntKeyStringValues()
      level.putKeyValues(keyValues).assertGet

      val deleteKeyValues = Slice.create[KeyValue](keyValues.size)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Transient.Delete(keyValue.key, previous = deleteKeyValues.lastOption, falsePositiveRate = 0.1)
      }

      level.putKeyValues(deleteKeyValues).assertGet
      level.isEmpty shouldBe false
      assertReads(deleteKeyValues, level)
    }
  }

  "Level.collapseSmallSegments" should {
    "collapse small Segments to 50% of the size when the Segment's size was reduced by deleting 50% of it's key-values" in {
      //disable throttling so that it does not automatically collapse small Segments
      val level = TestLevel(segmentSize = 1.kb, throttle = (_) => Throttle(Duration.Zero, 0))
      val keyValues = randomIntKeyValues(1000)
      level.putKeyValues(keyValues).assertGet
      //dispatch another put request so that existing Segment gets split
      level.putKeyValues(Slice(keyValues.head)).assertGet
      val segmentCountBeforeDelete = level.segmentsCount()
      segmentCountBeforeDelete > 1 shouldBe true

      val keyValuesNoDeleted = ListBuffer.empty[KeyValue]
      val deleteEverySecond =
        keyValues.zipWithIndex.flatMap {
          case (keyValue, index) =>
            if (index % 2 == 0)
              Some(Delete(keyValue.key))
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

        val keyValues = randomIntKeyValues(1000)
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
        assertReads(keyValues, reopenLevel.reopen)
      }
    }
  }

  "Level.addAsNewSegments" should {
    "write key values as new segments" in {
      val level = TestLevel()
      level.isEmpty shouldBe true

      val keyValues = randomIntKeyStringValues()
      val function = PrivateMethod[Try[Unit]]('addAsNewSegments)
      (level invokePrivate function(keyValues, None)).assertGet

      level.isEmpty shouldBe false
      assertReads(keyValues, level)
      //if it's a persistent Level, reopen to ensure data stays intact
      if (persistent) {
        val reopenLevel = level.reopen
        reopenLevel.isEmpty shouldBe false
        assertReads(keyValues, reopenLevel.reopen)
      }
    }
  }

  "Level.copy" should {
    "copy segments" in {
      if (memory) {
        //memory Segments cannot be copied
      } else {
        val level = TestLevel(throttle = (_) => Throttle(Duration.Zero, 0))
        level.isEmpty shouldBe true

        val keyValues1 = randomIntKeyStringValues()
        val keyValues2 = randomIntKeyStringValues()
        val segments = Iterable(TestSegment(keyValues1).assertGet, TestSegment(keyValues2).assertGet)
        val function = PrivateMethod[Try[Unit]]('copy)
        (level invokePrivate function(segments)).assertGet

        val allKeyValues = Slice((keyValues1 ++ keyValues2).toArray).updateStats

        level.isEmpty shouldBe false
        assertGet(allKeyValues, level)
        assertGet(allKeyValues, level.reopen)
      }
    }

    "fail copying if it failed to copy one of the Segments" in {
      if (memory) {
        //memory Segments cannot be copied
      } else {
        val level = TestLevel()
        level.isEmpty shouldBe true

        val segment1 = TestSegment().assertGet
        val segment2 = TestSegment().assertGet

        segment2.delete.assertGet // delete segment2 so there is a failure in copying Segments

        val segments = Iterable(segment1, segment2)
        val function = PrivateMethod[Try[Unit]]('copy)
        val failed = level invokePrivate function(segments)
        failed.isFailure shouldBe true
        failed.failed.get shouldBe a[NoSuchFileException]

        level.isEmpty shouldBe true
        level.reopen.isEmpty shouldBe true
      }
    }
  }

  "Level.putKeyValues" should {
    "write key values to target segments and update appendix" in {
      val level = TestLevel(segmentSize = 10.mb, throttle = (_) => Throttle(Duration.Zero, 0))

      val targetSegmentKeyValues = randomIntKeyStringValues()
      val targetSegment = TestSegment(keyValues = targetSegmentKeyValues, path = testSegmentFile.resolveSibling("10.seg")).assertGet

      val keyValues = randomIntKeyStringValues()
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

      val keyValueMock = mock[Delete]

      val keyValues: Slice[KeyValue] = Slice.create[KeyValue](3) //null KeyValue will throw an exception and the put should be reverted
      keyValues.add(KeyValue(123))
      keyValues.add(KeyValue(1234, 12345))
      keyValues.add(keyValueMock)

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
      level.putKeyValues(randomIntKeyStringValues(10000)).assertGet

      level.removeSegments(level.segments).assertGet

      level.isEmpty shouldBe true

      if (persistent) {
        level.segmentFilesOnDisk shouldBe empty
        level.reopen.isEmpty shouldBe true
      }
    }

  }

  "Level.buildNewMapEntry" should {

    implicit val serializer: SegmentsMapSerializer =
      SegmentsMapSerializer(
        removeDeletedRecords = false,
        mmapSegmentsOnRead = true,
        mmapSegmentsOnWrite = true,
        cacheKeysOnCreate = false
      )

    "build MapEntry.Add map for the first created Segment" in {
      val level = TestLevel()

      val map = TestSegment(Slice(KeyValue(1, "value1"), KeyValue(2, "value2")).updateStats).assertGet
      val actualMapEntry = level.buildNewMapEntry(Slice(map), initialMapEntry = None).assertGet
      val expectedMapEntry = MapEntry.Add[Slice[Byte], Segment](map.minKey, map)

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.read[Int])
    }

    "build MapEntry.Add map for the newly merged Segments and not add MapEntry.Remove map " +
      "for original Segment as it's minKey is replace by one of the new Segment" in {
      val level = TestLevel()

      val originalSegment = TestSegment(Slice(KeyValue(1, "value"), KeyValue(5, "value")).updateStats).assertGet
      val mergedSegment1 = TestSegment(Slice(KeyValue(1, "value"), KeyValue(5, "value")).updateStats).assertGet
      val mergedSegment2 = TestSegment(Slice(KeyValue(6, "value"), KeyValue(10, "value")).updateStats).assertGet
      val mergedSegment3 = TestSegment(Slice(KeyValue(11, "value"), KeyValue(15, "value")).updateStats).assertGet

      val actualMapEntry = level.buildNewMapEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), Some(originalSegment), initialMapEntry = None).assertGet

      val expectedMapEntry =
        MapEntry.Add[Slice[Byte], Segment](1, mergedSegment1) ++
          MapEntry.Add[Slice[Byte], Segment](6, mergedSegment2) ++
          MapEntry.Add[Slice[Byte], Segment](11, mergedSegment3)

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.read[Int])

    }

    "build MapEntry.Add map for the newly merged Segments and also add Remove map entry for original map when all minKeys are unique" in {
      val level = TestLevel()

      val originalSegment = TestSegment(Slice(KeyValue(0, "value"), KeyValue(5, "value")).updateStats).assertGet
      val mergedSegment1 = TestSegment(Slice(KeyValue(1, "value"), KeyValue(5, "value")).updateStats).assertGet
      val mergedSegment2 = TestSegment(Slice(KeyValue(6, "value"), KeyValue(10, "value")).updateStats).assertGet
      val mergedSegment3 = TestSegment(Slice(KeyValue(11, "value"), KeyValue(15, "value")).updateStats).assertGet

      val expectedMapEntry =
        MapEntry.Add[Slice[Byte], Segment](1, mergedSegment1) ++
          MapEntry.Add[Slice[Byte], Segment](6, mergedSegment2) ++
          MapEntry.Add[Slice[Byte], Segment](11, mergedSegment3) ++
          MapEntry.Remove[Slice[Byte], Segment](0)

      val actualMapEntry = level.buildNewMapEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), Some(originalSegment), initialMapEntry = None).assertGet

      actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.read[Int]) shouldBe
        expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.read[Int])
    }
  }

}