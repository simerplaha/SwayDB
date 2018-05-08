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
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._

class Level0MapEntrySpec extends TestBase {

  implicit val ordering = KeyOrder.default

  "MapEntryWriterLevel0 & MapEntryReaderLevel0" should {

    "write Put key value" in {
      import LevelZeroMapEntryWriter.Level0PutWriter
      val addEntry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.Put(1, 1))

      val slice = Slice.create[Byte](addEntry.entryBytesSize)
      addEntry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0PutReader
      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Put]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe addEntry

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory]](Reader(slice)).assertGet
      readEntry shouldBe addEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe (1: Slice[Byte])
      headValue shouldBe Memory.Put(1, 1)
    }

    "write remove key-value" in {
      import LevelZeroMapEntryWriter.Level0RemoveWriter
      val entry = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.Remove(1))

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0RemoveReader
      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Remove]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe entry

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe (1: Slice[Byte])
      headValue shouldBe Memory.Remove(1)
    }

    "write Update key value" in {
      import LevelZeroMapEntryWriter.Level0UpdateWriter
      val addEntry = MapEntry.Put[Slice[Byte], Memory.Update](1, Memory.Update(1, 1))

      val slice = Slice.create[Byte](addEntry.entryBytesSize)
      addEntry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0UpdateReader
      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Update]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe addEntry

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory]](Reader(slice)).assertGet
      readEntry shouldBe addEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe (1: Slice[Byte])
      headValue shouldBe Memory.Update(1, 1)
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
        val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory]](Reader(slice)).assertGet
        readEntry shouldBe entry

        val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
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

      val entry: MapEntry[Slice[Byte], Memory] =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.Put(1, 1)): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.Put(2, 2)) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.Remove(1)) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.Put(3, 3)) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.Remove(2)) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.Put(4, 4)) ++
          MapEntry.Put[Slice[Byte], Memory.Put](5, Memory.Put(5, 5)) ++
          MapEntry.Put[Slice[Byte], Memory.Update](3, Memory.Update(3, "three")) ++
          MapEntry.Put[Slice[Byte], Memory.Range](6, Memory.Range(6, 7, None, Value.Update(6))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](7, Memory.Range(7, 8, Some(Value.Put("7")), Value.Update(7))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](8, Memory.Range(8, 9, Some(Value.Remove(None)), Value.Update(8))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](9, Memory.Range(9, 10, None, Value.Remove(None))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 11, Some(Value.Put("10")), Value.Remove(None))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](11, Memory.Range(11, 12, Some(Value.Remove(None)), Value.Remove(None)))

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala
      assertSkipList()

      def assertSkipList() = {
        scalaSkipList should have size 11
        scalaSkipList.get(1).assertGet shouldBe Memory.Remove(1)
        scalaSkipList.get(2).assertGet shouldBe Memory.Remove(2)
        scalaSkipList.get(3).assertGet shouldBe Memory.Update(3, "three")
        scalaSkipList.get(4).assertGet shouldBe Memory.Put(4, 4)
        scalaSkipList.get(5).assertGet shouldBe Memory.Put(5, 5)
        scalaSkipList.get(6).assertGet shouldBe Memory.Range(6, 7, None, Value.Update(6))
        scalaSkipList.get(7).assertGet shouldBe Memory.Range(7, 8, Some(Value.Put("7")), Value.Update(7))
        scalaSkipList.get(8).assertGet shouldBe Memory.Range(8, 9, Some(Value.Remove(None)), Value.Update(8))
        scalaSkipList.get(9).assertGet shouldBe Memory.Range(9, 10, None, Value.Remove(None))
        scalaSkipList.get(10).assertGet shouldBe Memory.Range(10, 11, Some(Value.Put("10")), Value.Remove(None))
        scalaSkipList.get(11).assertGet shouldBe Memory.Range(11, 12, Some(Value.Remove(None)), Value.Remove(None))
      }
      //write skip list to bytes should result in the same skip list as before
      import LevelZeroMapEntryWriter.Level0PutValueWriter
      val bytes = MapCodec.write[Slice[Byte], Memory](skipList)
      val crcEntries = MapCodec.read[Slice[Byte], Memory](bytes, false).assertGet.item.assertGet
      skipList.clear()
      crcEntries applyTo skipList
      assertSkipList()
    }
  }
}
