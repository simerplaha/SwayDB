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

package swaydb.core.map.serializer

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.TestBase
import swaydb.core.data.{Memory, Transient, Value}
import swaydb.core.io.reader.Reader
import swaydb.core.map.MapEntry
import swaydb.core.util.TryUtil
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._
import swaydb.data.order.KeyOrder
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TryAssert._

class Level0MapEntrySpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  "MapEntryWriterLevel0 & MapEntryReaderLevel0" should {

    "Write random entries" in {

      def assert[V <: Memory.SegmentResponse](addEntry: MapEntry.Put[Slice[Byte], V])(implicit writer: MapEntryWriter[MapEntry.Put[Slice[Byte], V]],
                                                                                      reader: MapEntryReader[MapEntry.Put[Slice[Byte], V]]) = {
        val slice = Slice.create[Byte](addEntry.entryBytesSize)
        addEntry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        reader.read(Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe addEntry

        import LevelZeroMapEntryReader.Level0Reader
        val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.SegmentResponse]](Reader(slice)).assertGet
        readEntry shouldBe addEntry

        val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)
        readEntry applyTo skipList
        val scalaSkipList = skipList.asScala

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (addEntry.value.key: Slice[Byte])
        headValue shouldBe addEntry.value
      }

      val keyValues =
        randomizedKeyValues(
          count = 1000,
          addPut = true,
          addRandomRemoves = true,
          addRandomRangeRemoves = true,
          addRandomUpdates = true,
          addRandomFunctions = true,
          addRandomRanges = true,
          addRandomPendingApply = true,
          addRandomRemoveDeadlines = true,
          addRandomPutDeadlines = true,
          addRandomExpiredPutDeadlines = true,
          addRandomUpdateDeadlines = true,
          addRandomGroups = false
        )

      keyValues foreach {
        case keyValue: Transient.Remove =>
          import LevelZeroMapEntryWriter.Level0RemoveWriter
          import LevelZeroMapEntryReader.Level0RemoveReader
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Remove]))

        case keyValue: Transient.Put =>
          import LevelZeroMapEntryWriter.Level0PutWriter
          import LevelZeroMapEntryReader.Level0PutReader
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Put]))

        case keyValue: Transient.Update =>
          import LevelZeroMapEntryWriter.Level0UpdateWriter
          import LevelZeroMapEntryReader.Level0UpdateReader
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Update]))

        case keyValue: Transient.Function =>
          import LevelZeroMapEntryWriter.Level0FunctionWriter
          import LevelZeroMapEntryReader.Level0FunctionReader
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Function]))

        case keyValue: Transient.PendingApply =>
          import LevelZeroMapEntryWriter.Level0PendingApplyWriter
          import LevelZeroMapEntryReader.Level0PendingApplyReader
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.PendingApply]))

        case keyValue: Transient.Range =>
          import LevelZeroMapEntryWriter.Level0RangeWriter
          import LevelZeroMapEntryReader.Level0RangeReader
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Range]))

        case keyValue: Transient.Group =>
          fail("Groups are not added to Level0")
      }

    }

    "write, remove & update key-value" in {
      import LevelZeroMapEntryWriter.Level0PutWriter
      import LevelZeroMapEntryWriter.Level0UpdateWriter
      import LevelZeroMapEntryWriter.Level0RemoveWriter
      import LevelZeroMapEntryWriter.Level0RangeWriter

      val put1 = Memory.put(1, randomStringOption, randomDeadlineOption)
      val put2 = Memory.put(2, randomStringOption, randomDeadlineOption)
      val put3 = Memory.put(3, randomStringOption, randomDeadlineOption)
      val put4 = Memory.put(4, randomStringOption, randomDeadlineOption)
      val put5 = Memory.put(5, randomStringOption, randomDeadlineOption)

      val remove1 = Memory.remove(1, randomDeadlineOption)
      val remove2 = Memory.remove(2, randomDeadlineOption)

      val update1 = Memory.update(3, randomStringOption, randomDeadlineOption)

      val range1 = randomRangeKeyValue(6, 7)
      val range2 = randomRangeKeyValue(7, 8)
      val range3 = randomRangeKeyValue(8, 9)
      val range4 = randomRangeKeyValue(9, 10)
      val range5 = randomRangeKeyValue(10, 11)
      val range6 = randomRangeKeyValue(11, 12)

      val entry: MapEntry[Slice[Byte], Memory.SegmentResponse] =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, put1): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, put2) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](1, remove1) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, put3) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, remove2) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, put4) ++
          MapEntry.Put[Slice[Byte], Memory.Put](5, put5) ++
          MapEntry.Put[Slice[Byte], Memory.Update](3, update1) ++
          MapEntry.Put[Slice[Byte], Memory.Range](6, range1) ++
          MapEntry.Put[Slice[Byte], Memory.Range](7, range2) ++
          MapEntry.Put[Slice[Byte], Memory.Range](8, range3) ++
          MapEntry.Put[Slice[Byte], Memory.Range](9, range4) ++
          MapEntry.Put[Slice[Byte], Memory.Range](10, range5) ++
          MapEntry.Put[Slice[Byte], Memory.Range](11, range6)

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.SegmentResponse]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala
      assertSkipList()

      def assertSkipList() = {
        scalaSkipList should have size 11
        scalaSkipList.get(1).assertGet shouldBe remove1
        scalaSkipList.get(2).assertGet shouldBe remove2
        scalaSkipList.get(3).assertGet shouldBe update1
        scalaSkipList.get(4).assertGet shouldBe put4
        scalaSkipList.get(5).assertGet shouldBe put5
        scalaSkipList.get(6).assertGet shouldBe range1
        scalaSkipList.get(7).assertGet shouldBe range2
        scalaSkipList.get(8).assertGet shouldBe range3
        scalaSkipList.get(9).assertGet shouldBe range4
        scalaSkipList.get(10).assertGet shouldBe range5
        scalaSkipList.get(11).assertGet shouldBe range6
      }
      //write skip list to bytes should result in the same skip list as before
      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      val bytes = MapCodec.write[Slice[Byte], Memory.SegmentResponse](skipList)
      val recoveryResult = MapCodec.read[Slice[Byte], Memory.SegmentResponse](bytes, false).assertGet
      recoveryResult.result shouldBe TryUtil.successUnit

      val readEntries = recoveryResult.item.assertGet
      //clear and apply new skipList and the result should be the same as previous.
      skipList.clear()
      readEntries applyTo skipList
      assertSkipList()
    }
  }
}
