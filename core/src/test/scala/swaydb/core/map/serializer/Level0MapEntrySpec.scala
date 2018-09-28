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
import swaydb.core.data.{Memory, Value}
import swaydb.core.io.reader.Reader
import swaydb.core.map.MapEntry
import swaydb.core.util.TryUtil
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._

class Level0MapEntrySpec extends TestBase {

  override implicit val ordering = KeyOrder.default

  "MapEntryWriterLevel0 & MapEntryReaderLevel0" should {

    "write Put key value" in {
      val put = Memory.Put(1, randomStringOption, randomDeadlineOption)

      import LevelZeroMapEntryWriter.Level0PutWriter
      val addEntry = MapEntry.Put[Slice[Byte], Memory.Put](1, put)

      val slice = Slice.create[Byte](addEntry.entryBytesSize)
      addEntry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0PutReader
      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Put]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe addEntry

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.Response]](Reader(slice)).assertGet
      readEntry shouldBe addEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe (1: Slice[Byte])
      headValue shouldBe put
    }

    "write remove key-value" in {
      val remove = Memory.Remove(1, randomDeadlineOption)

      import LevelZeroMapEntryWriter.Level0RemoveWriter
      val entry = MapEntry.Put[Slice[Byte], Memory.Remove](1, remove)

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0RemoveReader
      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Remove]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe entry

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.Response]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe (1: Slice[Byte])
      headValue shouldBe remove
    }

    "write Update key value" in {
      val update = Memory.Update(1, randomStringOption, randomDeadlineOption)

      import LevelZeroMapEntryWriter.Level0UpdateWriter
      val addEntry = MapEntry.Put[Slice[Byte], Memory.Update](1, update)

      val slice = Slice.create[Byte](addEntry.entryBytesSize)
      addEntry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0UpdateReader
      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Update]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe addEntry

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.Response]](Reader(slice)).assertGet
      readEntry shouldBe addEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe (1: Slice[Byte])
      headValue shouldBe update
    }

    "write range key-value" in {
      import LevelZeroMapEntryWriter.Level0RangeWriter

      def writeRange(inputRange: Memory.Range) = {
        val entry = MapEntry.Put[Slice[Byte], Memory.Range](0, inputRange)

        val slice = Slice.create[Byte](entry.entryBytesSize)
        entry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        import LevelZeroMapEntryReader.Level0RangeReader
        MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Range]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe entry

        import LevelZeroMapEntryReader.Level0Reader
        val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.Response]](Reader(slice)).assertGet
        readEntry shouldBe entry

        val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
        readEntry applyTo skipList
        val scalaSkipList = skipList.asScala

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (0: Slice[Byte])
        headValue shouldBe inputRange
      }

      (1 to 100) foreach {
        _ =>
          writeRange(randomRangeKeyValue(0, 1))
      }
    }

    "write, remove & update key-value" in {
      import LevelZeroMapEntryWriter.Level0PutWriter
      import LevelZeroMapEntryWriter.Level0UpdateWriter
      import LevelZeroMapEntryWriter.Level0RemoveWriter
      import LevelZeroMapEntryWriter.Level0RangeWriter

      val put1 = Memory.Put(1, randomStringOption, randomDeadlineOption)
      val put2 = Memory.Put(2, randomStringOption, randomDeadlineOption)
      val put3 = Memory.Put(3, randomStringOption, randomDeadlineOption)
      val put4 = Memory.Put(4, randomStringOption, randomDeadlineOption)
      val put5 = Memory.Put(5, randomStringOption, randomDeadlineOption)

      val remove1 = Memory.Remove(1, randomDeadlineOption)
      val remove2 = Memory.Remove(2, randomDeadlineOption)

      val update1 = Memory.Update(3, randomStringOption, randomDeadlineOption)

      val range1 = randomRangeKeyValue(6, 7)
      val range2 = randomRangeKeyValue(7, 8)
      val range3 = randomRangeKeyValue(8, 9)
      val range4 = randomRangeKeyValue(9, 10)
      val range5 = randomRangeKeyValue(10, 11)
      val range6 = randomRangeKeyValue(11, 12)

      val entry: MapEntry[Slice[Byte], Memory.Response] =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, put1): MapEntry[Slice[Byte], Memory.Response]) ++
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
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.Response]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
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
      import LevelZeroMapEntryWriter.Level0PutValueWriter
      val bytes = MapCodec.write[Slice[Byte], Memory.Response](skipList)
      val recoveryResult = MapCodec.read[Slice[Byte], Memory.Response](bytes, false).assertGet
      recoveryResult.result shouldBe TryUtil.successUnit

      val readEntries = recoveryResult.item.assertGet
      //clear and apply new skipList and the result should be the same as previous.
      skipList.clear()
      readEntries applyTo skipList
      assertSkipList()
    }
  }
}
