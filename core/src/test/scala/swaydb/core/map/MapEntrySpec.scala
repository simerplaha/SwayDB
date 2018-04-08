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

import swaydb.core.data.{Memory, Persistent, Value}
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
  implicit val keyValuesLimitImplicitLimiter: (Persistent, Segment) => Unit = TestLimitQueues.keyValueLimiter

  val appendixReader = AppendixMapEntryReader(false, true, true)

  val keyValues = randomIntKeyValues(count = 10)
  val segment = TestSegment(keyValues).assertGet

  "MapEntry" should {
    "put Level0 single Put entry to skipList" in {
      import LevelZeroMapEntryWriter._
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      val entry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.Put(1, Some("one")))
      entry.hasRange shouldBe false

      entry applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe Memory.Put(1, Some("one"))
    }

    "put Level0 single Remote entry to skipList" in {
      import LevelZeroMapEntryWriter._
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      val entry = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.Remove(1))
      entry.hasRange shouldBe false

      entry applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe Memory.Remove(1)
    }

    "put Level0 single Put Range entry to skipList" in {
      import LevelZeroMapEntryWriter._
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      val range1 = Memory.Range(1, 10, Some(Value.Put("one")), Value.Put("range one"))
      val entry1 = MapEntry.Put[Slice[Byte], Memory.Range](1, range1)
      entry1.hasRange shouldBe true

      entry1 applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe range1

      val range2 = Memory.Range(2, 10, None, Value.Put("range one"))
      val entry2 = MapEntry.Put[Slice[Byte], Memory.Range](2, range2)
      entry2.hasRange shouldBe true

      entry2 applyTo skipList
      skipList should have size 2
      skipList.get(2: Slice[Byte]) shouldBe range2
    }

    "put Level0 single Remove Range entry to skipList" in {
      import LevelZeroMapEntryWriter._
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      val range1 = Memory.Range(1, 10, Some(Value.Remove), Value.Remove)
      val entry1 = MapEntry.Put[Slice[Byte], Memory.Range](1, range1)
      entry1.hasRange shouldBe true

      entry1 applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe range1

      val range2 = Memory.Range(2, 10, None, Value.Remove)
      val entry2 = MapEntry.Put[Slice[Byte], Memory.Range](2, range2)
      entry2.hasRange shouldBe true

      entry2 applyTo skipList
      skipList should have size 2
      skipList.get(2: Slice[Byte]) shouldBe range2
    }

    "batch multiple Level0 key-value to skipList" in {
      import LevelZeroMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      val entry =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.Put(1, Some("one"))): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.Put(2, Some("two"))) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.Put(3, Some("three"))) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.Remove(2)) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.Put(4, Some("four"))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](5, Memory.Range(5, 10, None, Value.Put(10))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](11, Memory.Range(11, 20, Some(Value.Put(20)), Value.Put(20))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](21, Memory.Range(21, 30, None, Value.Remove)) ++
          MapEntry.Put[Slice[Byte], Memory.Range](31, Memory.Range(31, 40, Some(Value.Put(20)), Value.Remove))

      entry applyTo skipList

      entry.hasRange shouldBe true

      skipList should have size 8
      skipList.get(1: Slice[Byte]) shouldBe Memory.Put(1, Some("one"))
      skipList.get(2: Slice[Byte]) shouldBe Memory.Remove(2)
      skipList.get(3: Slice[Byte]) shouldBe Memory.Put(3, Some("three"))
      skipList.get(4: Slice[Byte]) shouldBe Memory.Put(4, Some("four"))
      skipList.get(5: Slice[Byte]) shouldBe Memory.Range(5, 10, None, Value.Put(10))
      skipList.get(11: Slice[Byte]) shouldBe Memory.Range(11, 20, Some(Value.Put(20)), Value.Put(20))
      skipList.get(21: Slice[Byte]) shouldBe Memory.Range(21, 30, None, Value.Remove)
      skipList.get(31: Slice[Byte]) shouldBe Memory.Range(31, 40, Some(Value.Put(20)), Value.Remove)
    }

    "add Appendix single Put entry to skipList" in {
      import AppendixMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)

      val entry = MapEntry.Put[Slice[Byte], Segment](1, segment)
      entry.hasRange shouldBe false

      entry applyTo skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe segment
    }

    "remove Appendix entry from skipList" in {
      import AppendixMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](ordering)

      val entry = MapEntry.Put[Slice[Byte], Segment](1, segment)
      entry.hasRange shouldBe false

      entry applyTo skipList
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

      val entry =
        (MapEntry.Put[Slice[Byte], Segment](1, segment1): MapEntry[Slice[Byte], Segment]) ++
          MapEntry.Put[Slice[Byte], Segment](2, segment2) ++
          MapEntry.Put[Slice[Byte], Segment](3, segment3) ++
          MapEntry.Remove[Slice[Byte]](2) ++
          MapEntry.Put[Slice[Byte], Segment](4, segment4)

      entry.hasRange shouldBe false

      entry applyTo skipList

      skipList should have size 3
      skipList.get(1: Slice[Byte]) shouldBe segment1
      skipList.get(2: Slice[Byte]) should be(null)
      skipList.get(3: Slice[Byte]) shouldBe segment3
      skipList.get(4: Slice[Byte]) shouldBe segment4
    }

  }

  "MapEntry.Put" should {
    "write and read bytes for a single Level0 key-value" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val entry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.Put(1, Some("one")))
      entry.hasRange shouldBe false

      val bytes = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Put]](bytes.drop(ByteSizeOf.int)).assertGet shouldBe entry
      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).assertGet shouldBe entry
    }

    "write and read bytes for a single Appendix" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val entry = MapEntry.Put[Slice[Byte], Segment](segment.minKey, segment)
      entry.hasRange shouldBe false

      val bytes = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Put[Slice[Byte], Segment]](bytes.drop(1)).assertGet shouldBe entry
      MapEntryReader.read[MapEntry[Slice[Byte], Segment]](bytes).assertGet shouldBe entry
    }
  }

  "MapEntry.Remove" should {
    "write and read bytes for a single Level0 key-values entry" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val entry = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.Remove(1))
      entry.hasRange shouldBe false

      val bytes = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Remove]](bytes.drop(ByteSizeOf.int)).assertGet shouldBe entry
      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).assertGet shouldBe entry
    }

    "write and read bytes for single Appendix entry" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      //do remove
      val entry = MapEntry.Remove[Slice[Byte]](segment.minKey)
      entry.hasRange shouldBe false

      val bytes = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Remove[Slice[Byte]]](bytes.drop(1)).assertGet.key shouldBe entry.key
      MapEntryReader.read[MapEntry[Slice[Byte], Segment]](bytes).assertGet shouldBe entry
    }
  }

  "MapEntry" should {
    "batch write multiple key-values for Level0" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val entry =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.Put(1, Some("one"))): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.Put(2, Some("two"))) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.Put(3, Some("three"))) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.Remove(2)) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.Put(4, Some("four")))

      entry.hasRange shouldBe false

      val bytes = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).assertGet shouldBe entry
    }
  }

  "10000 map entries" can {

    "be written and read for Level0" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val initialEntry: MapEntry[Slice[Byte], Memory] = MapEntry.Put(0, Memory.Put(0, Some(0)))
      var entry =
        Range.inclusive(1, 10000).foldLeft(initialEntry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Put[Slice[Byte], Memory.Put](i, Memory.Put(i, Some(i)))
        }
      entry =
        Range.inclusive(5000, 10000).foldLeft(entry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Put[Slice[Byte], Memory.Remove](i, Memory.Remove(i))
        }

      entry.hasRange shouldBe false

      val bytes = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps!

      val readMapEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory]](Reader(bytes)).assertGet

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      readMapEntry applyTo skipList
      skipList should have size 10001
      skipList.firstKey() shouldBe (0: Slice[Byte])
      skipList.lastKey() shouldBe (10000: Slice[Byte])
      skipList.subMap(0, true, 4999, true).asScala.foreach(_._2.isInstanceOf[Memory.Put] shouldBe true)
      skipList.subMap(5000, true, 10000, true).asScala.foreach(_._2.isInstanceOf[Memory.Remove] shouldBe true)
    }

    "be written and read for Appendix" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val initialEntry: MapEntry[Slice[Byte], Segment] = MapEntry.Put[Slice[Byte], Segment](0, segment)
      var entry =
        Range.inclusive(1, 10000).foldLeft(initialEntry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Put[Slice[Byte], Segment](i, segment)
        }
      entry =
        Range.inclusive(5000, 10000).foldLeft(entry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Remove[Slice[Byte]](i)
        }

      entry.hasRange shouldBe false

      val bytes = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo bytes
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
