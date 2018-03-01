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

import swaydb.core.data.{PersistentReadOnly, Value}
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer._
import swaydb.core.segment.Segment
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._

class MapEntrySpec extends TestBase {

  implicit val ordering = KeyOrder.default
  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: (PersistentReadOnly, Segment) => Unit = TestLimitQueues.keyValueLimiter

  val appendixReader = AppendixMapEntryReader(false, true, true, false)

  val keyValues = randomIntKeyValues(count = 10)
  val segment = TestSegment(keyValues).assertGet

  "MapEntry" should {
    "add Level0 single Add entry to skipList" in {
      import LevelZeroMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)

      MapEntry.Add[Slice[Byte], Value.Put](1, Value.Put(Some("one"))) applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe Value.Put(Some("one"))
    }

    "add Level0 single remove entry to skipList" in {
      import LevelZeroMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      MapEntry.Add[Slice[Byte], Value.Remove](1, Value.Remove) applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe Value.Remove
    }

    "batch multiple Level0 key-value to skipList" in {
      import LevelZeroMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)

      (MapEntry.Add[Slice[Byte], Value.Put](1, Value.Put(Some("one"))): MapEntry[Slice[Byte], Value]) ++
        MapEntry.Add[Slice[Byte], Value.Put](2, Value.Put(Some("two"))) ++
        MapEntry.Add[Slice[Byte], Value.Put](3, Value.Put(Some("three"))) ++
        MapEntry.Add[Slice[Byte], Value.Remove](2, Value.Remove) ++
        MapEntry.Add[Slice[Byte], Value.Put](4, Value.Put(Some("four"))) applyTo skipList

      skipList should have size 4
      skipList.get(1: Slice[Byte]) shouldBe Value.Put(Some("one"))
      skipList.get(2: Slice[Byte]) shouldBe Value.Remove
      skipList.get(3: Slice[Byte]) shouldBe Value.Put(Some("three"))
      skipList.get(4: Slice[Byte]) shouldBe Value.Put(Some("four"))
    }

    "add Appendix single add entry to skipList" in {
      import AppendixMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)

      MapEntry.Add[Slice[Byte], Segment](1, segment) applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe segment
    }

    "remove Appendix entry from skipList" in {
      import AppendixMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)

      MapEntry.Add[Slice[Byte], Segment](1, segment) applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe segment

      MapEntry.Remove[Slice[Byte]](1) applyTo skipList
      skipList shouldBe empty
    }

    "batch multiple appendix entries to skipList" in {
      import AppendixMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)
      val segment1 = TestSegment().assertGet
      val segment2 = TestSegment().assertGet
      val segment3 = TestSegment().assertGet
      val segment4 = TestSegment().assertGet

      (MapEntry.Add[Slice[Byte], Segment](1, segment1): MapEntry[Slice[Byte], Segment]) ++
        MapEntry.Add[Slice[Byte], Segment](2, segment2) ++
        MapEntry.Add[Slice[Byte], Segment](3, segment3) ++
        MapEntry.Remove[Slice[Byte]](2) ++
        MapEntry.Add[Slice[Byte], Segment](4, segment4) applyTo skipList

      skipList should have size 3
      skipList.get(1: Slice[Byte]) shouldBe segment1
      skipList.get(2: Slice[Byte]) should be(null)
      skipList.get(3: Slice[Byte]) shouldBe segment3
      skipList.get(4: Slice[Byte]) shouldBe segment4
    }

  }

  "MapEntry.Add" should {
    "write and read bytes for a single Level0 key-value" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val mapEntry = MapEntry.Add[Slice[Byte], Value.Put](1, Value.Put(Some("one")))
      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Add[Slice[Byte], Value.Put]](bytes.drop(ByteSizeOf.int)).assertGet shouldBe mapEntry
      MapEntryReader.read[MapEntry[Slice[Byte], Value]](bytes).assertGet shouldBe mapEntry
    }

    "write and read bytes for a single Appendix" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val mapEntry = MapEntry.Add[Slice[Byte], Segment](segment.minKey, segment)
      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Add[Slice[Byte], Segment]](bytes.drop(1)).assertGet shouldBe mapEntry
      MapEntryReader.read[MapEntry[Slice[Byte], Segment]](bytes).assertGet shouldBe mapEntry
    }
  }

  "MapEntry.Remove" should {
    "write and read bytes for a single Level0 key-values entry" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val mapEntry = MapEntry.Add[Slice[Byte], Value.Remove](1, Value.Remove)
      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Add[Slice[Byte], Value.Remove]](bytes.drop(ByteSizeOf.int)).assertGet shouldBe mapEntry
      MapEntryReader.read[MapEntry[Slice[Byte], Value]](bytes).assertGet shouldBe mapEntry
    }

    "write and read bytes for single Appendix entry" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      //do remove
      val mapEntry = MapEntry.Remove[Slice[Byte]](segment.minKey)
      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Remove[Slice[Byte]]](bytes.drop(1)).assertGet.key shouldBe mapEntry.key
      MapEntryReader.read[MapEntry[Slice[Byte], Segment]](bytes).assertGet shouldBe mapEntry
    }
  }

  "MapEntry" should {
    "batch write multiple key-values for Level0" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val entry =
        (MapEntry.Add[Slice[Byte], Value.Put](1, Value.Put(Some("one"))): MapEntry[Slice[Byte], Value]) ++
          MapEntry.Add[Slice[Byte], Value.Put](2, Value.Put(Some("two"))) ++
          MapEntry.Add[Slice[Byte], Value.Put](3, Value.Put(Some("three"))) ++
          MapEntry.Add[Slice[Byte], Value.Remove](2, Value.Remove) ++
          MapEntry.Add[Slice[Byte], Value.Put](4, Value.Put(Some("four")))

      val bytes = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry[Slice[Byte], Value]](bytes).assertGet shouldBe entry
    }
  }

  "10000 map entries" can {

    "be written and read for Level0" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val initialMapEntry: MapEntry[Slice[Byte], Value] = MapEntry.Add(0, Value.Put(Some(0)))
      var mapEntry =
        Range.inclusive(1, 10000).foldLeft(initialMapEntry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Add[Slice[Byte], Value.Put](i, Value.Put(Some(i)))
        }
      mapEntry =
        Range.inclusive(5000, 10000).foldLeft(mapEntry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Add[Slice[Byte], Value.Remove](i, Value.Remove)
        }

      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps!

      val readMapEntry = MapEntryReader.read[MapEntry[Slice[Byte], Value]](Reader(bytes)).assertGet

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      readMapEntry applyTo skipList
      skipList should have size 10001
      skipList.firstKey() shouldBe (0: Slice[Byte])
      skipList.lastKey() shouldBe (10000: Slice[Byte])
      skipList.subMap(0, true, 4999, true).asScala.foreach(_._2.isInstanceOf[Value.Put] shouldBe true)
      skipList.subMap(5000, true, 10000, true).asScala.foreach(_._2.isInstanceOf[Value.Remove] shouldBe true)
    }

    "be written and read for Appendix" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val initialMapEntry: MapEntry[Slice[Byte], Segment] = MapEntry.Add(0, segment)
      var mapEntry =
        Range.inclusive(1, 10000).foldLeft(initialMapEntry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Add[Slice[Byte], Segment](i, segment)
        }
      mapEntry =
        Range.inclusive(5000, 10000).foldLeft(mapEntry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Remove[Slice[Byte]](i)
        }

      val bytes = Slice.create[Byte](mapEntry.entryBytesSize)
      mapEntry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps!

      val readMapEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(bytes)).assertGet

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)
      readMapEntry applyTo skipList
      skipList should have size 5000
      skipList.firstKey() shouldBe (0: Slice[Byte])
      skipList.lastKey() shouldBe (4999: Slice[Byte])
    }
  }
}
