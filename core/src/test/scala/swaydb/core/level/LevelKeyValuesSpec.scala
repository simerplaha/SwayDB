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

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class LevelKeyValuesSpec0 extends LevelKeyValuesSpec

class LevelKeyValuesSpec1 extends LevelKeyValuesSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelKeyValuesSpec2 extends LevelKeyValuesSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelKeyValuesSpec3 extends LevelKeyValuesSpec {
  override def inMemoryStorage = true
}

sealed trait LevelKeyValuesSpec extends TestBase with MockFactory with PrivateMethodTester {

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

  "put KeyValues" should {
    "write a key-values to the Level" in {
      val level = TestLevel()

      val keyValues = randomPutKeyValues(startId = Some(1))
      level.putKeyValuesTest(keyValues).runIO
      level.putKeyValuesTest(Slice(keyValues.head)).runIO

      level.segmentsInLevel() foreach {
        segment =>
          segment.createdInLevel.runIO shouldBe level.levelNumber
          segment.isGrouped.runIO shouldBe groupingStrategy.isDefined
      }

      assertReads(keyValues, level)

      if (persistent) {
        val reopen = level.reopen
        assertReads(keyValues, reopen)
      }
    }

    "return an empty level if all the key values in the Level were REMOVED and if Level is the only Level" in {
      val level = TestLevel(segmentSize = 1.kb)

      val keyValues = randomPutKeyValues(keyValuesCount)
      level.putKeyValuesTest(keyValues).runIO

      val deleteKeyValues = Slice.create[KeyValue.ReadOnly](keyValues.size * 2)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Memory.remove(keyValue.key)
      }
      //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
      //these delete keys should also be removed
      val lastKeyValuesId = keyValues.last.key.read[Int] + 10000
      (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
        id =>
          deleteKeyValues add Memory.remove(id, randomly(expiredDeadline()))
      }

      level.putKeyValuesTest(deleteKeyValues).runIO
      level.segmentFilesInAppendix shouldBe 0

      level.isEmpty shouldBe true
      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were REMOVED but it has lower level" in {
      val level = TestLevel(nextLevel = Some(TestLevel()))

      val keyValues = randomPutKeyValues()
      level.putKeyValuesTest(keyValues).runIO

      val deleteKeyValues = Slice.create[KeyValue.ReadOnly](keyValues.size)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Memory.remove(keyValue.key)
      }

      level.putKeyValuesTest(deleteKeyValues).runIO
      level.isEmpty shouldBe false
      keyValues foreach {
        keyValue =>
          level.get(keyValue.key).runIO shouldBe empty
      }
    }

    "return an empty level if all the key values in the Level were REMOVED by RANGE and if Level is the only Level" in {
      val level = TestLevel(segmentSize = 1.kb)

      val keyValues = randomPutKeyValues(keyValuesCount)
      level.putKeyValuesTest(keyValues).runIO

      level.putKeyValuesTest(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, None, Value.remove(None)))).runIO
      level.segmentFilesInAppendix shouldBe 0

      level.isEmpty shouldBe true
      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were REMOVED by RANGE but it has a lower Level" in {
      val level = TestLevel(segmentSize = 1.kb, nextLevel = Some(TestLevel()))

      val keyValues = randomPutKeyValues(keyValuesCount)
      level.putKeyValuesTest(keyValues).runIO
      val segmentsCountBeforeRemove = level.segmentFilesInAppendix

      level.putKeyValuesTest(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, None, Value.remove(None)))).runIO
      level.segmentFilesInAppendix shouldBe segmentsCountBeforeRemove

      level.isEmpty shouldBe false
      if (persistent) {
        level.reopen.isEmpty shouldBe false
        level.segmentFilesOnDisk should have size segmentsCountBeforeRemove
      }
    }

    "return an empty level if all the key values in the Level were EXPIRED and if Level is the only Level" in {
      val level = TestLevel(segmentSize = 1.kb)

      val keyValues = randomPutKeyValues(keyValuesCount)
      level.putKeyValuesTest(keyValues).runIO

      val deleteKeyValues = Slice.create[KeyValue.ReadOnly](keyValues.size * 2)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Memory.remove(keyValue.key, 1.seconds)
      }
      //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
      //these delete keys should also be removed
      val lastKeyValuesId = keyValues.last.key.read[Int] + 1
      (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
        id =>
          deleteKeyValues add Memory.remove(id, randomly(expiredDeadline()))
      }

      level.nextLevel shouldBe empty

      level.putKeyValuesTest(deleteKeyValues).runIO

      sleep(2.seconds)

      level.segmentsInLevel() foreach {
        segment =>
          level.refresh(segment).runIO
      }

      //expired key-values return empty after 2.seconds
      keyValues foreach {
        keyValue =>
          level.get(keyValue.key).runIO shouldBe empty
      }

      level.segmentFilesInAppendix shouldBe 0

      level.isEmpty shouldBe true

      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were EXPIRED and if Level has a lower Level" in {
      val level = TestLevel(segmentSize = 1.kb, nextLevel = Some(TestLevel()))

      val keyValues = randomPutKeyValues(keyValuesCount)
      level.putKeyValuesTest(keyValues).runIO

      val deleteKeyValues = Slice.create[KeyValue.ReadOnly](keyValues.size * 2)
      keyValues foreach {
        keyValue =>
          deleteKeyValues add Memory.remove(keyValue.key, 0.seconds)
      }
      //also add another set of Delete key-values where the keys do not belong to the Level but since there is no lower level
      //these delete keys should also be removed
      val lastKeyValuesId = keyValues.last.key.read[Int] + 1
      (lastKeyValuesId until keyValues.size + lastKeyValuesId) foreach {
        id =>
          deleteKeyValues add Memory.remove(id, randomly(expiredDeadline()))
      }

      level.putKeyValuesTest(deleteKeyValues).runIO

      //expired key-values return empty.
      keyValues foreach {
        keyValue =>
          level.get(keyValue.key).runIO shouldBe empty
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
      val level = TestLevel(segmentSize = 1.kb)

      val keyValues = randomPutKeyValues(keyValuesCount)
      level.putKeyValuesTest(keyValues).runIO

      level.putKeyValuesTest(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, None, Value.remove(2.seconds.fromNow)))).runIO

      //expired key-values return empty after 2.seconds
      eventual(5.seconds) {
        keyValues foreach {
          keyValue =>
            level.get(keyValue.key).runIO shouldBe empty
        }
      }

      level.segmentsInLevel() foreach {
        segment =>
          level.refresh(segment).runIO
      }

      level.segmentFilesInAppendix shouldBe 0

      level.isEmpty shouldBe true

      if (persistent) {
        level.reopen.isEmpty shouldBe true
        level.segmentFilesOnDisk shouldBe empty
      }
    }

    "not return an empty level if all the key values in the Level were EXPIRED by RANGE and if Level has a last Level" in {
      val level = TestLevel(segmentSize = 1.kb, nextLevel = Some(TestLevel()))

      val keyValues = randomPutKeyValues(keyValuesCount)
      level.putKeyValuesTest(keyValues).runIO

      level.putKeyValuesTest(Slice(Memory.Range(keyValues.head.key, keyValues.last.key.readInt() + 1, None, Value.remove(2.seconds.fromNow)))).runIO

      //expired key-values return empty after 2.seconds
      eventual(5.seconds) {
        keyValues foreach {
          keyValue =>
            level.get(keyValue.key).runIO shouldBe empty
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

  "putKeyValues" should {
    "write key values to target segments and update appendix" in {
      val level = TestLevel(segmentSize = 10.mb)

      val targetSegmentKeyValues = randomIntKeyStringValues()
      val targetSegment = TestSegment(keyValues = targetSegmentKeyValues, path = testSegmentFile.resolveSibling("10.seg")).runIO

      val keyValues = randomPutKeyValues()
      val function = PrivateMethod[IO[swaydb.Error.Segment, Unit]]('putKeyValues)
      (level invokePrivate function(keyValues, Seq(targetSegment), None)).runIO

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
      val targetSegment = TestSegment(keyValues = targetSegmentKeyValues).runIO

      val keyValues: Slice[KeyValue] = Slice.create[KeyValue](3) //null KeyValue will throw an exception and the put should be reverted
      keyValues.add(Memory.put(123))
      keyValues.add(Memory.put(1234, 12345))
      keyValues.add(Persistent.Put(1235, None, null, Time.empty, 10, 10, 10, 10, 10, 0, false)) //give it a null Reader so that it fails reading the value.

      val function = PrivateMethod[IO[swaydb.Error.Segment, Unit]]('putKeyValues)
      val failed = level invokePrivate function(keyValues, Iterable(targetSegment), None)
      failed.isFailure shouldBe true
      failed.failed.get.exception shouldBe a[NullPointerException]

      level.get(123).runIO.isEmpty shouldBe true
      level.get(1234).runIO.isEmpty shouldBe true
    }
  }
}
