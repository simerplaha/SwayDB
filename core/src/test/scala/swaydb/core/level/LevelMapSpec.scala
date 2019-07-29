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
import org.scalatest.OptionValues._
import org.scalatest.PrivateMethodTester
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.map.{Map, MapEntry, SkipListMerger}
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.ExecutionContext

class LevelMapSpec0 extends LevelMapSpec

class LevelMapSpec1 extends LevelMapSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelMapSpec2 extends LevelMapSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelMapSpec3 extends LevelMapSpec {
  override def inMemoryStorage = true
}

sealed trait LevelMapSpec extends TestBase with MockFactory with PrivateMethodTester {

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

  "putMap on a single Level" should {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader._
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val merged: SkipListMerger[Slice[Byte], Memory.SegmentResponse] = LevelZeroSkipListMerger

    val map =
      if (persistent)
        Map.persistent[Slice[Byte], Memory.SegmentResponse](
          folder = randomIntDirectory,
          mmap = true,
          flushOnOverflow = true,
          fileSize = 1.mb,
          initialWriteCount = 0,
          dropCorruptedTailEntries = false
        ).valueIOGet.item
      else
        Map.memory[Slice[Byte], Memory.SegmentResponse]()

    val keyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false)
    keyValues foreach {
      keyValue =>
        map.write(MapEntry.Put(keyValue.key, keyValue.asInstanceOf[Memory.SegmentResponse]))
    }

    "succeed" when {
      "writing to an empty Level" in {
        val level = TestLevel()
        level.put(map).value
        //since this is a new Segment and Level has no sub-level, all the deleted key-values will value removed.
        val (deletedKeyValues, otherKeyValues) = keyValues.partition(_.isInstanceOf[Memory.Remove])

        assertReads(otherKeyValues, level)

        //deleted key-values do not exist.
        deletedKeyValues foreach {
          deleted =>
            level.get(deleted.key).value shouldBe empty
        }
      }

      "writing to a non empty Level" in {
        val level = TestLevel()

        //creating a Segment with existing string key-values
        val existingKeyValues = Array(Memory.put("one", "one"), Memory.put("two", "two"), Memory.put("three", "three"))

        val sortedExistingKeyValues =
          Slice(
            Array(
              //also randomly set expired deadline for Remove.
              Memory.put("one", "one"), Memory.put("two", "two"), Memory.put("three", "three"), Memory.remove("four", randomly(expiredDeadline()))
            ).sorted(keyOrder.on[KeyValue](_.key)))

        level.putKeyValuesTest(sortedExistingKeyValues).valueIOGet

        //put a new map
        level.put(map).value
        assertGet(keyValues.filterNot(_.isInstanceOf[Memory.Remove]), level)

        level.get("one").value.value shouldBe existingKeyValues(0)
        level.get("two").value.value shouldBe existingKeyValues(1)
        level.get("three").value.value shouldBe existingKeyValues(2)
        level.get("four").value shouldBe empty
      }
    }
  }

  "putMap on two Level" should {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader._
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val merged: SkipListMerger[Slice[Byte], Memory.SegmentResponse] = LevelZeroSkipListMerger

    val map =
      if (persistent)
        Map.persistent[Slice[Byte], Memory.SegmentResponse](
          folder = randomIntDirectory,
          mmap = true,
          flushOnOverflow = true,
          fileSize = 1.mb,
          initialWriteCount = 0,
          dropCorruptedTailEntries = false).valueIOGet.item
      else
        Map.memory[Slice[Byte], Memory.SegmentResponse]()

    val keyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false)
    keyValues foreach {
      keyValue =>
        map.write(MapEntry.Put(keyValue.key, keyValue.asInstanceOf[Memory.SegmentResponse]))
    }

    "succeed" when {
      "writing to an empty Level by copying to last Level" in {
        val nextLevel = mock[NextLevel]

        nextLevel.isTrash _ expects() returning false

        (nextLevel.isCopyable(_: Map[Slice[Byte], Memory.SegmentResponse])) expects * onCall {
          putMap: Map[Slice[Byte], Memory.SegmentResponse] =>
            putMap.pathOption shouldBe map.pathOption
            true
        }

        (nextLevel.put(_: Map[Slice[Byte], Memory.SegmentResponse])(_: ExecutionContext)) expects(*, *) onCall {
          (putMap: Map[Slice[Byte], Memory.SegmentResponse], _) =>
            putMap.pathOption shouldBe map.pathOption
            IO.Deferred.unit
        }

        val level = TestLevel(nextLevel = Some(nextLevel))
        level.put(map).value
        assertGetNoneFromThisLevelOnly(keyValues, level) //because nextLevel is a mock.
      }

      "writing to non empty Levels by copying to last Level if key-values do not overlap upper Level" in {
        val nextLevel = mock[NextLevel]

        val lastLevelKeyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false, startId = Some(1)).map(_.asInstanceOf[Memory.SegmentResponse])
        val map = TestMap(lastLevelKeyValues)

        nextLevel.isTrash _ expects() returning false

        (nextLevel.isCopyable(_: Map[Slice[Byte], Memory.SegmentResponse])) expects * onCall {
          putMap: Map[Slice[Byte], Memory.SegmentResponse] =>
            putMap.pathOption shouldBe map.pathOption
            true
        }

        (nextLevel.put(_: Map[Slice[Byte], Memory.SegmentResponse])(_: ExecutionContext)) expects(*, *) onCall {
          (putMap: Map[Slice[Byte], Memory.SegmentResponse], _) =>
            putMap.pathOption shouldBe map.pathOption
            IO.Deferred.unit
        }

        val level = TestLevel(nextLevel = Some(nextLevel))
        val keyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false, startId = Some(lastLevelKeyValues.last.key.readInt() + 1000)).toTransient
        level.putKeyValues(keyValues, Seq(TestSegment(keyValues).valueIOGet), None).valueIOGet

        level.put(map).value
        assertGetNoneFromThisLevelOnly(lastLevelKeyValues, level) //because nextLevel is a mock.
        assertGetFromThisLevelOnly(keyValues, level)
      }
    }
  }
}
