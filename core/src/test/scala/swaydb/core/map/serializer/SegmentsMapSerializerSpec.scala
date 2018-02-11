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
import swaydb.core.data.{KeyValue, PersistentReadOnly}
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.map.MapEntry
import swaydb.core.segment.Segment
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class SegmentsMapSerializerSpec extends TestBase {
  implicit val ordering = KeyOrder.default
  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: (PersistentReadOnly, Segment) => Unit = keyValueLimiter

  "SegmentsPersistentMapsSerializer" should {

    implicit val serializer = SegmentsMapSerializer(removeDeletedRecords = true, mmapSegmentsOnRead = true, mmapSegmentsOnWrite = false, cacheKeysOnCreate = false)

    "write Add segment entry to slice" in {
      val keyValues = randomIntKeyValues(count = 10)
      val segment = TestSegment(keyValues, path = randomDir).assertGet

      val mapEntry = MapEntry.Add(segment.minKey, segment)

      val slice = Slice.create[Byte](mapEntry.entryBytesSize)

      mapEntry writeTo slice
      //this ensures that bytesRequiredFor is returning the correct size
      slice.isFull shouldBe true

      serializer.read(Reader(slice)).assertGet shouldBe mapEntry
    }

    "write Remove segment entry to slice" in {
      val mapEntry = MapEntry.Remove[Slice[Byte], Segment](123)

      val slice = Slice.create[Byte](mapEntry.entryBytesSize)

      mapEntry writeTo slice
      slice.isFull shouldBe true

      val readEntry = serializer.read(Reader(slice)).assertGet
      readEntry shouldBe mapEntry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)
      readEntry applyTo skipList
      skipList shouldBe empty
    }

    "write a single add and single remove segment entry" in {
      val keyValues = randomIntKeyValues(count = 10)
      val segment = TestSegment(keyValues, path = randomDir).assertGet

      val addRemoveEntry: MapEntry[Slice[Byte], Segment] = MapEntry.Add(segment.minKey, segment) ++ MapEntry.Remove(segment.minKey)
      val addRemoveSlice = Slice.create[Byte](addRemoveEntry.entryBytesSize)
      addRemoveEntry writeTo addRemoveSlice
      //this ensures that bytesRequiredFor is returning the correct size
      addRemoveSlice.isFull shouldBe true
      serializer.read(Reader(addRemoveSlice)).assertGet shouldBe addRemoveEntry

      val removeAddEntry: MapEntry[Slice[Byte], Segment] = MapEntry.Remove(segment.minKey) ++ MapEntry.Add(segment.minKey, segment)
      val removeAddSlice = Slice.create[Byte](removeAddEntry.entryBytesSize)
      removeAddEntry writeTo removeAddSlice
      removeAddSlice.isFull shouldBe true
      serializer.read(Reader(removeAddSlice)).assertGet shouldBe removeAddEntry

      val segment2 = TestSegment(keyValues, path = randomDir).assertGet
      val addAddEntry: MapEntry[Slice[Byte], Segment] = MapEntry.Add(segment.minKey, segment) ++ MapEntry.Add(segment.minKey, segment2)
      val addAddSlice = Slice.create[Byte](addAddEntry.entryBytesSize)
      addAddEntry writeTo addAddSlice
      addAddSlice.isFull shouldBe true
      serializer.read(Reader(addAddSlice)).assertGet shouldBe addAddEntry
    }

    "write multiple entries" in {
      val segment1 = TestSegment(Slice(KeyValue(1, 1)), path = randomDir).assertGet
      val segment2 = TestSegment(Slice(KeyValue(2, 2)), path = randomDir).assertGet
      val segment3 = TestSegment(Slice(KeyValue(3, 3)), path = randomDir).assertGet

      val entry: MapEntry[Slice[Byte], Segment] =
        MapEntry.Add(segment1.minKey, segment1) ++
          MapEntry.Add(segment2.minKey, segment2) ++
          MapEntry.Add(segment3.minKey, segment3) ++
          MapEntry.Remove(segment2.minKey)

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      //this ensures that bytesRequiredFor is returning the correct size
      slice.isFull shouldBe true

      serializer.read(Reader(slice)).assertGet shouldBe entry
    }

    "report corruption" in {
      val segment1 = TestSegment(Slice(KeyValue(1, 1)), path = randomDir).assertGet
      val segment2 = TestSegment(Slice(KeyValue(2, 2)), path = randomDir).assertGet
      val segment3 = TestSegment(Slice(KeyValue(3, 3)), path = randomDir).assertGet

      val entry: MapEntry[Slice[Byte], Segment] =
        MapEntry.Add(segment1.minKey, segment1) ++
          MapEntry.Add(segment2.minKey, segment2) ++
          MapEntry.Add(segment3.minKey, segment3) ++
          MapEntry.Remove(segment2.minKey)

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      //this ensures that bytesRequiredFor is returning the correct size
      slice.isFull shouldBe true

      serializer.read(Reader(slice.dropRight(1))).failed.assertGet shouldBe a[ArrayIndexOutOfBoundsException]
    }
  }
}
