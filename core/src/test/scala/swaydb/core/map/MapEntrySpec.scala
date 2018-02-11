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

package swaydb.core.map

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.TestBase
import swaydb.core.data.{KeyValueReadOnly, PersistentReadOnly}
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.SegmentsMapSerializer
import swaydb.core.segment.Segment
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MapEntrySpec extends TestBase {

  implicit val ordering = KeyOrder.default
  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: (PersistentReadOnly, Segment) => Unit = keyValueLimiter

  implicit val mapFormatter = SegmentsMapSerializer(removeDeletedRecords = false, mmapSegmentsOnRead = true, mmapSegmentsOnWrite = false, cacheKeysOnCreate = false)

  val keyValues = randomIntKeyValues(count = 10)
  val segment = TestSegment(keyValues).assertGet

  "Add and Remove mapEntries" should {
    "add and remove items from skiplist" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)

      MapEntry.Add(segment.minKey, segment) applyTo skipList
      skipList should have size 1
      skipList.get(segment.minKey) shouldBe segment

      MapEntry.Remove(segment.minKey) applyTo skipList
      skipList shouldBe empty
      Option(skipList.get(segment.minKey)) shouldBe empty
    }

    "add item to skipList and remove when batched" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)

      (MapEntry.Add(segment.minKey, segment) ++ MapEntry.Remove(segment.minKey)) applyTo skipList
      skipList shouldBe empty
      Option(skipList.get(segment.minKey)) shouldBe empty

    }
  }

  "Single MapEntry.Add" should {
    "write and read bytes" in {
      val mapEntry = MapEntry.Add(segment.minKey, segment)
      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      val readMapEntry = mapFormatter.read(Reader(bytes)).assertGet
      readMapEntry shouldBe mapEntry
    }
  }

  "Single MapEntry.Remove" should {
    "write and read bytes" in {

      val mapEntry = MapEntry.Remove(segment.minKey)
      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps!
      mapFormatter.read(Reader(bytes)).assertGet shouldBe mapEntry
    }
  }

  "Combined MapEntry.Add and MapEntry.Remove" should {
    "write and read bytes" in {
      val map1 = TestSegment(randomIntKeyValues()).assertGet
      val map2 = TestSegment(randomIntKeyValues()).assertGet

      val mapEntry =
        MapEntry.Add(map1.minKey, map1) ++
          MapEntry.Remove(map1.minKey) ++
          MapEntry.Add(map2.minKey, map2)

      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps!

      val readMapEntry = mapFormatter.read(Reader(bytes)).assertGet

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)
      readMapEntry applyTo skipList
      skipList should have size 1
      skipList.firstKey() shouldBe map2.minKey
      skipList.firstEntry().getValue.path shouldBe map2.path
    }
  }

  "10000 map entries" can {
    "written and read" in {
      val initialMapEntry: MapEntry[Slice[Byte], Segment] = MapEntry.Add(0, segment)
      val mapEntry =
        Range.inclusive(1, 10000).foldLeft(initialMapEntry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Add[Slice[Byte], Segment](i, segment)
        }

      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps!

      val readMapEntry = mapFormatter.read(Reader(bytes)).assertGet

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)
      readMapEntry applyTo skipList
      skipList should have size 10001
    }
  }
}
