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
import swaydb.core.data.Value
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

    "write add key value of ValueType.Add" in {
      import LevelZeroMapEntryWriter.Level0AddWriter
      val addEntry = MapEntry.Add[Slice[Byte], Value.Put](1, Value.Put(1))

      val slice = Slice.create[Byte](addEntry.entryBytesSize)
      addEntry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0AddReader
      MapEntryReader.read[MapEntry.Add[Slice[Byte], Value.Put]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe addEntry

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Value]](Reader(slice)).assertGet
      readEntry shouldBe addEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe (1: Slice[Byte])
      headValue shouldBe Value.Put(1)
    }

    "write remove key-value" in {
      import LevelZeroMapEntryWriter.Level0RemoveWriter
      val entry = MapEntry.Add[Slice[Byte], Value.Remove](1, Value.Remove)

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0RemoveReader
      MapEntryReader.read[MapEntry.Add[Slice[Byte], Value.Remove]](Reader(slice.drop(ByteSizeOf.int))).assertGet shouldBe entry

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Value]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe (1: Slice[Byte])
      headValue shouldBe Value.Remove
    }

    "write and remove key-value" in {
      import LevelZeroMapEntryWriter.Level0AddWriter
      import LevelZeroMapEntryWriter.Level0RemoveWriter

      val entry: MapEntry[Slice[Byte], Value] =
        (MapEntry.Add[Slice[Byte], Value.Put](1, Value.Put(1)): MapEntry[Slice[Byte], Value]) ++
          MapEntry.Add[Slice[Byte], Value.Put](2, Value.Put(2)) ++
          MapEntry.Add[Slice[Byte], Value.Remove](1, Value.Remove) ++
          MapEntry.Add[Slice[Byte], Value.Put](3, Value.Put(3)) ++
          MapEntry.Add[Slice[Byte], Value.Remove](2, Value.Remove) ++
          MapEntry.Add[Slice[Byte], Value.Put](4, Value.Put(4)) ++
          MapEntry.Add[Slice[Byte], Value.Put](5, Value.Put(5))

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Value]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala
      assertSkipList()

      def assertSkipList() = {
        scalaSkipList should have size 5
        scalaSkipList.get(1).assertGet shouldBe Value.Remove
        scalaSkipList.get(2).assertGet shouldBe Value.Remove
        scalaSkipList.get(3).assertGet shouldBe Value.Put(3)
        scalaSkipList.get(4).assertGet shouldBe Value.Put(4)
        scalaSkipList.get(5).assertGet shouldBe Value.Put(5)
      }
      //write skip list to bytes should result in the same skip list as before
      import LevelZeroMapEntryWriter.Level0AddValueWriter
      val bytes = MapCodec.write[Slice[Byte], Value](skipList)
      val crcEntries = MapCodec.read[Slice[Byte], Value](bytes, false).assertGet.item.assertGet
      skipList.clear()
      crcEntries applyTo skipList
      assertSkipList()
    }
  }
}
