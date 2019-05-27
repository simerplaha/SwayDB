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

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.core.CommonAssertions._
import swaydb.core.IOAssert._
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

class LevelWriteMapSpec0 extends LevelWriteMapSpec

class LevelWriteMapSpec1 extends LevelWriteMapSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelWriteMapSpec2 extends LevelWriteMapSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelWriteMapSpec3 extends LevelWriteMapSpec {
  override def inMemoryStorage = true
}

sealed trait LevelWriteMapSpec extends TestBase with MockFactory with PrivateMethodTester {

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

  "putMap" should {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader._
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val merged: SkipListMerger[Slice[Byte], Memory.SegmentResponse] = LevelZeroSkipListMerger

    val map =
      if (persistent)
        Map.persistent[Slice[Byte], Memory.SegmentResponse](randomIntDirectory, true, true, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      else
        Map.memory[Slice[Byte], Memory.SegmentResponse]()

    val keyValues = randomPutKeyValues(keyValuesCount, addRandomRemoves = true, addRandomPutDeadlines = false)
    keyValues foreach {
      keyValue =>
        map.write(MapEntry.Put(keyValue.key, keyValue.asInstanceOf[Memory.SegmentResponse]))
    }

    "create a segment to an empty Level with no lower level" in {
      val level = TestLevel()
      level.put(map).assertGet
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
      val existingKeyValues = Array(Memory.put("one", "one"), Memory.put("two", "two"), Memory.put("three", "three"))

      val sortedExistingKeyValues =
        Slice(
          Array(
            //also randomly set expired deadline for Remove.
            Memory.put("one", "one"), Memory.put("two", "two"), Memory.put("three", "three"), Memory.remove("four", randomly(expiredDeadline()))
          ).sorted(keyOrder.on[KeyValue](_.key)))

      level.putKeyValues(sortedExistingKeyValues).assertGet

      //put a new map
      level.put(map).assertGet
      assertGet(keyValues.filterNot(_.isInstanceOf[Memory.Remove]), level)

      level.get("one").assertGet shouldBe existingKeyValues(0)
      level.get("two").assertGet shouldBe existingKeyValues(1)
      level.get("three").assertGet shouldBe existingKeyValues(2)
      level.get("four").assertGetOpt shouldBe empty
    }
  }
}
