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
import org.scalatest.EitherValues._
import org.scalatest.OptionValues._
import org.scalatest.PrivateMethodTester
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data._
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.map.{Map, MapEntry, SkipListMerger}
import swaydb.core.segment.ReadState
import swaydb.core.{TestBase, TestSweeper, TestTimer}
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

  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeper.Enabled = TestSweeper.fileSweeper
  implicit val memorySweeperImplicitSweeper: Option[MemorySweeper.All] = TestSweeper.memorySweeperMax
  implicit val skipListMerger = LevelZeroSkipListMerger

  "putMap on a single Level" should {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader._
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val merged: SkipListMerger[Slice[Byte], Memory] = LevelZeroSkipListMerger

    val map =
      if (persistent)
        Map.persistent[Slice[Byte], Memory](
          folder = randomIntDirectory,
          mmap = true,
          flushOnOverflow = true,
          fileSize = 1.mb,
          dropCorruptedTailEntries = false
        ).runRandomIO.right.value.item
      else
        Map.memory[Slice[Byte], Memory]()

    val keyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false)
    keyValues foreach {
      keyValue =>
        map.write(MapEntry.Put(keyValue.key, keyValue.asInstanceOf[Memory]))
    }

    "succeed" when {
      "writing to an empty Level" in {
        val level = TestLevel()
        level.put(map).right.right.value.right.value
        //since this is a new Segment and Level has no sub-level, all the deleted key-values will value removed.
        val (deletedKeyValues, otherKeyValues) = keyValues.partition(_.isInstanceOf[Memory.Remove])

        assertReads(otherKeyValues, level)

        //deleted key-values do not exist.
        deletedKeyValues foreach {
          deleted =>
            level.get(deleted.key, ReadState.random).runRandomIO.right.value shouldBe empty
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

        level.putKeyValuesTest(sortedExistingKeyValues).runRandomIO.right.value

        //put a new map
        level.put(map).right.right.value.right.value
        assertGet(keyValues.filterNot(_.isInstanceOf[Memory.Remove]), level)

        level.get("one", ReadState.random).runRandomIO.right.value.value shouldBe existingKeyValues(0)
        level.get("two", ReadState.random).runRandomIO.right.value.value shouldBe existingKeyValues(1)
        level.get("three", ReadState.random).runRandomIO.right.value.value shouldBe existingKeyValues(2)
        level.get("four", ReadState.random).runRandomIO.right.value shouldBe empty
      }
    }
  }

  "putMap on two Level" should {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader._
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
    implicit val merged: SkipListMerger[Slice[Byte], Memory] = LevelZeroSkipListMerger

    val map =
      if (persistent)
        Map.persistent[Slice[Byte], Memory](
          folder = randomIntDirectory,
          mmap = true,
          flushOnOverflow = true,
          fileSize = 1.mb,
          dropCorruptedTailEntries = false).runRandomIO.right.value.item
      else
        Map.memory[Slice[Byte], Memory]()

    val keyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false)
    keyValues foreach {
      keyValue =>
        map.write(MapEntry.Put(keyValue.key, keyValue.asInstanceOf[Memory]))
    }

    "succeed" when {
      "writing to an empty Level by copying to last Level" in {
        val nextLevel = mock[NextLevel]

        nextLevel.isTrash _ expects() returning false

        (nextLevel.isCopyable(_: Map[Slice[Byte], Memory])) expects * onCall {
          putMap: Map[Slice[Byte], Memory] =>
            putMap.pathOption shouldBe map.pathOption
            true
        }

        (nextLevel.put(_: Map[Slice[Byte], Memory])(_: ExecutionContext)) expects(*, *) onCall {
          (putMap: Map[Slice[Byte], Memory], _) =>
            putMap.pathOption shouldBe map.pathOption
            IO.unitUnit
        }

        val level = TestLevel(nextLevel = Some(nextLevel))
        level.put(map).right.right.value.right.value
        assertGetNoneFromThisLevelOnly(keyValues, level) //because nextLevel is a mock.
      }

      "writing to non empty Levels by copying to last Level if key-values do not overlap upper Level" in {
        val nextLevel = mock[NextLevel]

        val lastLevelKeyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false, startId = Some(1)).map(_.asInstanceOf[Memory])
        val map = TestMap(lastLevelKeyValues)

        nextLevel.isTrash _ expects() returning false

        (nextLevel.isCopyable(_: Map[Slice[Byte], Memory])) expects * onCall {
          putMap: Map[Slice[Byte], Memory] =>
            putMap.pathOption shouldBe map.pathOption
            true
        }

        (nextLevel.put(_: Map[Slice[Byte], Memory])(_: ExecutionContext)) expects(*, *) onCall {
          (putMap: Map[Slice[Byte], Memory], _) =>
            putMap.pathOption shouldBe map.pathOption
            IO.unitUnit
        }

        val level = TestLevel(nextLevel = Some(nextLevel))
        val keyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false, startId = Some(lastLevelKeyValues.last.key.readInt() + 1000)).toTransient
        level.putKeyValues(keyValues, Seq(TestSegment(keyValues)), None).runRandomIO.right.value

        level.put(map).right.right.value.right.value
        assertGetNoneFromThisLevelOnly(lastLevelKeyValues, level) //because nextLevel is a mock.
        assertGetFromThisLevelOnly(keyValues, level)
      }
    }
  }
}
