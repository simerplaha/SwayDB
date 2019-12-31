///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.map
//
//
//import org.scalatest.OptionValues._
//import swaydb.Error.Map.ExceptionHandler
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.actor.{FileSweeper, MemorySweeper}
//import swaydb.core.data.{Memory, Value}
//import swaydb.core.io.file.BlockCache
//import swaydb.core.io.reader.Reader
//import swaydb.core.map.serializer._
//import swaydb.core.segment.Segment
//import swaydb.core.segment.format.a.block.SegmentIO
//import swaydb.core.util.SkipList
//import swaydb.core.{TestBase, TestSweeper, TestTimer}
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.{Slice, SliceOption}
//import swaydb.data.util.ByteSizeOf
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.jdk.CollectionConverters._
//import scala.concurrent.duration._
//
//class MapEntrySpec extends TestBase {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit def testTimer: TestTimer = TestTimer.Empty
//  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeper.Enabled = TestSweeper.fileSweeper
//  implicit val memorySweeperImplicitSweeper: Option[MemorySweeper.All] = TestSweeper.memorySweeperMax
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit def segmentIO: SegmentIO = SegmentIO.random
//  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache
//
//  val appendixReader = AppendixMapEntryReader(true, true)
//
//  val keyValues = randomKeyValues(count = 10)
//
//  "MapEntry" should {
//    "set hasRemoveDeadline to true if Put has remove deadline" in {
//      import LevelZeroMapEntryWriter._
//      MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1, 1.second.fromNow)).hasRemoveDeadline shouldBe true
//    }
//
//    "set hasRemoveDeadline to false if Put has remove deadline" in {
//      import LevelZeroMapEntryWriter._
//      MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1)).hasRemoveDeadline shouldBe false
//    }
//
//    "set hasUpdate to true if Put has update" in {
//      import LevelZeroMapEntryWriter._
//      val entry = MapEntry.Put[Slice[Byte], Memory.Update](1, Memory.update(1, 1))
//      entry.hasRemoveDeadline shouldBe false
//      entry.hasUpdate shouldBe true
//    }
//
//    "set hasUpdate to false if Put does not have update" in {
//      import LevelZeroMapEntryWriter._
//
//      val entry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, 1))
//      entry.hasRemoveDeadline shouldBe false
//      entry.hasUpdate shouldBe false
//
//      val entry2 = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
//      entry2.hasRemoveDeadline shouldBe false
//      entry2.hasUpdate shouldBe false
//    }
//
//    "put Level0 single Put entry to skipList" in {
//      import LevelZeroMapEntryWriter._
//      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
//
//      val entry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("one")))
//      entry.hasRange shouldBe false
//
//      entry applyTo skipList
//      skipList should have size 1
//      skipList.get(1: Slice[Byte]).value shouldBe Memory.put(1, Some("one"))
//    }
//
//    "put Level0 single Remote entry to skipList" in {
//      import LevelZeroMapEntryWriter._
//      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
//
//      val entry = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
//      entry.hasRange shouldBe false
//
//      entry applyTo skipList
//      skipList should have size 1
//      skipList.get(1: Slice[Byte]).value shouldBe Memory.remove(1)
//    }
//
//    "put Level0 single Put Range entry to skipList" in {
//      import LevelZeroMapEntryWriter._
//      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
//
//      val range1 = Memory.Range(1, 10, Value.put("one"), Value.update("range one"))
//      val entry1 = MapEntry.Put[Slice[Byte], Memory.Range](1, range1)
//      entry1.hasRange shouldBe true
//
//      entry1 applyTo skipList
//      skipList should have size 1
//      skipList.get(1: Slice[Byte]).value shouldBe range1
//
//      val range2 = Memory.Range(2, 10, Value.FromValue.Null, Value.update("range one"))
//      val entry2 = MapEntry.Put[Slice[Byte], Memory.Range](2, range2)
//      entry2.hasRange shouldBe true
//
//      entry2 applyTo skipList
//      skipList should have size 2
//      skipList.get(2: Slice[Byte]).value shouldBe range2
//    }
//
//    "put Level0 single Remove Range entry to skipList" in {
//      import LevelZeroMapEntryWriter._
//      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
//
//      val range1 = Memory.Range(1, 10, Value.remove(None), Value.remove(None))
//      val entry1 = MapEntry.Put[Slice[Byte], Memory.Range](1, range1)
//      entry1.hasRange shouldBe true
//
//      entry1 applyTo skipList
//      skipList should have size 1
//      skipList.get(1: Slice[Byte]).value shouldBe range1
//
//      val range2 = Memory.Range(2, 10, Value.FromValue.Null, Value.remove(None))
//      val entry2 = MapEntry.Put[Slice[Byte], Memory.Range](2, range2)
//      entry2.hasRange shouldBe true
//
//      entry2 applyTo skipList
//      skipList should have size 2
//      skipList.get(2: Slice[Byte]).value shouldBe range2
//    }
//
//    "batch multiple Level0 key-value to skipList" in {
//      import LevelZeroMapEntryWriter._
//
//      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
//
//      val entry =
//        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("one"))): MapEntry[Slice[Byte], Memory]) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, Some("two"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, Some("three"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, Some("four"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Range](5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))) ++
//          MapEntry.Put[Slice[Byte], Memory.Range](11, Memory.Range(11, 20, Value.put(20), Value.update(20))) ++
//          MapEntry.Put[Slice[Byte], Memory.Range](21, Memory.Range(21, 30, Value.FromValue.Null, Value.remove(None))) ++
//          MapEntry.Put[Slice[Byte], Memory.Range](31, Memory.Range(31, 40, Value.put(20), Value.remove(None)))
//
//      entry applyTo skipList
//
//      entry.hasRange shouldBe true
//
//      skipList should have size 8
//      skipList.get(1: Slice[Byte]).value shouldBe Memory.put(1, Some("one"))
//      skipList.get(2: Slice[Byte]).value shouldBe Memory.remove(2)
//      skipList.get(3: Slice[Byte]).value shouldBe Memory.put(3, Some("three"))
//      skipList.get(4: Slice[Byte]).value shouldBe Memory.put(4, Some("four"))
//      skipList.get(5: Slice[Byte]).value shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
//      skipList.get(11: Slice[Byte]).value shouldBe Memory.Range(11, 20, Value.put(20), Value.update(20))
//      skipList.get(21: Slice[Byte]).value shouldBe Memory.Range(21, 30, Value.FromValue.Null, Value.remove(None))
//      skipList.get(31: Slice[Byte]).value shouldBe Memory.Range(31, 40, Value.put(20), Value.remove(None))
//    }
//
//    "add Appendix single Put entry to skipList" in {
//      import AppendixMapEntryWriter._
//      val segment = TestSegment(keyValues)
//
//      val skipList = SkipList.concurrent[SliceOption[Byte], SegmentOptional, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
//
//      val entry = MapEntry.Put[Slice[Byte], Segment](1, segment)
//      entry.hasRange shouldBe false
//
//      entry applyTo skipList
//      skipList should have size 1
//      skipList.get(1: Slice[Byte]).value shouldBe segment
//
//      segment.close.runRandomIO.right.value
//    }
//
//    "remove Appendix entry from skipList" in {
//      import AppendixMapEntryWriter._
//      val segment = TestSegment(keyValues)
//
//      val skipList = SkipList.concurrent[SliceOption[Byte], SegmentOptional, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
//
//      val entry = MapEntry.Put[Slice[Byte], Segment](1, segment)
//      entry.hasRange shouldBe false
//
//      entry applyTo skipList
//      skipList should have size 1
//      skipList.get(1: Slice[Byte]).value shouldBe segment
//
//      MapEntry.Remove[Slice[Byte]](1) applyTo skipList
//      skipList shouldBe empty
//
//      segment.close.runRandomIO.right.value
//    }
//
//    "batch multiple appendix entries to skipList" in {
//      import AppendixMapEntryWriter._
//
//      val skipList = SkipList.concurrent[SliceOption[Byte], SegmentOptional, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
//      val segment1 = TestSegment().runRandomIO.right.value
//      val segment2 = TestSegment().runRandomIO.right.value
//      val segment3 = TestSegment().runRandomIO.right.value
//      val segment4 = TestSegment().runRandomIO.right.value
//
//      val entry =
//        (MapEntry.Put[Slice[Byte], Segment](1, segment1): MapEntry[Slice[Byte], Segment]) ++
//          MapEntry.Put[Slice[Byte], Segment](2, segment2) ++
//          MapEntry.Put[Slice[Byte], Segment](3, segment3) ++
//          MapEntry.Remove[Slice[Byte]](2) ++
//          MapEntry.Put[Slice[Byte], Segment](4, segment4)
//
//      entry.hasRange shouldBe false
//
//      entry applyTo skipList
//
//      skipList should have size 3
//      skipList.get(1: Slice[Byte]).value shouldBe segment1
//      skipList.get(2: Slice[Byte]) shouldBe empty
//      skipList.get(3: Slice[Byte]).value shouldBe segment3
//      skipList.get(4: Slice[Byte]).value shouldBe segment4
//
//      segment1.close.runRandomIO.right.value
//      segment2.close.runRandomIO.right.value
//      segment3.close.runRandomIO.right.value
//      segment4.close.runRandomIO.right.value
//    }
//  }
//
//  "MapEntry.Put" should {
//    "write and read bytes for a single Level0 key-value" in {
//      import LevelZeroMapEntryReader._
//      import LevelZeroMapEntryWriter._
//
//      val entry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("one")))
//      entry.hasRange shouldBe false
//
//      val bytes = Slice.create[Byte](entry.entryBytesSize)
//      entry writeTo bytes
//      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.
//
//      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Put]](bytes.drop(ByteSizeOf.int)).runRandomIO.right.value.value shouldBe entry
//      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value.value shouldBe entry
//    }
//
//    "write and read bytes for a single Appendix" in {
//      import AppendixMapEntryWriter._
//      import appendixReader._
//      val segment = TestSegment(keyValues)
//
//      val entry = MapEntry.Put[Slice[Byte], Segment](segment.minKey, segment)
//      entry.hasRange shouldBe false
//
//      val bytes = Slice.create[Byte](entry.entryBytesSize)
//      entry writeTo bytes
//      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.
//
//      MapEntryReader.read[MapEntry.Put[Slice[Byte], Segment]](bytes.drop(1)).runRandomIO.right.value.value shouldBe entry
//      MapEntryReader.read[MapEntry[Slice[Byte], Segment]](bytes).runRandomIO.right.value.value shouldBe entry
//
//      segment.close.runRandomIO.right.value
//    }
//  }
//
//  "MapEntry.Remove" should {
//    "write and read bytes for a single Level0 key-values entry" in {
//      import LevelZeroMapEntryReader._
//      import LevelZeroMapEntryWriter._
//
//      val entry = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
//      entry.hasRange shouldBe false
//
//      val bytes = Slice.create[Byte](entry.entryBytesSize)
//      entry writeTo bytes
//      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.
//
//      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Remove]](bytes.drop(ByteSizeOf.int)).runRandomIO.right.value.value shouldBe entry
//      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value.value shouldBe entry
//    }
//
//    "write and read bytes for single Appendix entry" in {
//      import AppendixMapEntryWriter._
//      import appendixReader._
//      val segment = TestSegment(keyValues)
//
//      //do remove
//      val entry = MapEntry.Remove[Slice[Byte]](segment.minKey)
//      entry.hasRange shouldBe false
//
//      val bytes = Slice.create[Byte](entry.entryBytesSize)
//      entry writeTo bytes
//      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.
//
//      MapEntryReader.read[MapEntry.Remove[Slice[Byte]]](bytes.drop(1)).runRandomIO.right.value.value.key shouldBe entry.key
//      MapEntryReader.read[MapEntry[Slice[Byte], Segment]](bytes).runRandomIO.right.value.value shouldBe entry
//
//      segment.close.runRandomIO.right.value
//    }
//  }
//
//  "MapEntry" should {
//    "batch write multiple key-values for Level0" in {
//      import LevelZeroMapEntryReader._
//      import LevelZeroMapEntryWriter._
//
//      val entry =
//        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("one"))): MapEntry[Slice[Byte], Memory]) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, Some("two"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, Some("three"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, Some("four")))
//
//      entry.hasRange shouldBe false
//
//      val bytes = Slice.create[Byte](entry.entryBytesSize)
//      entry writeTo bytes
//      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.
//
//      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value.value shouldBe entry
//    }
//  }
//
//  "10000 map entries" can {
//
//    "be written and read for Level0" in {
//      import LevelZeroMapEntryReader._
//      import LevelZeroMapEntryWriter._
//      import swaydb.Error.Map.ExceptionHandler
//
//      val initialEntry: MapEntry[Slice[Byte], Memory] = MapEntry.Put(0, Memory.put(0, Some(0)))
//      var entry =
//        Range.inclusive(1, 10000).foldLeft(initialEntry) {
//          case (previousEntry, i) =>
//            previousEntry ++ MapEntry.Put[Slice[Byte], Memory.Put](i, Memory.put(i, Some(i)))
//        }
//      entry =
//        Range.inclusive(5000, 10000).foldLeft(entry) {
//          case (previousEntry, i) =>
//            previousEntry ++ MapEntry.Put[Slice[Byte], Memory.Remove](i, Memory.remove(i))
//        }
//
//      entry.hasRange shouldBe false
//
//      val bytes = Slice.create[Byte](entry.entryBytesSize)
//      entry writeTo bytes
//      bytes.isFull shouldBe true //fully written! No gaps!
//
//      val readMapEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory]](Reader(bytes)).runRandomIO.right.value.value
//
//      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
//      readMapEntry applyTo skipList
//      skipList should have size 10001
//      skipList.headKey.value shouldBe (0: Slice[Byte])
//      skipList.lastKey.value shouldBe (10000: Slice[Byte])
//      skipList.subMap(0, true, 4999, true).asScala.foreach(_._2.isInstanceOf[Memory.Put] shouldBe true)
//      skipList.subMap(5000, true, 10000, true).asScala.foreach(_._2.isInstanceOf[Memory.Remove] shouldBe true)
//    }
//
//    "be written and read for Appendix" in {
//      import AppendixMapEntryWriter._
//      import appendixReader._
//      import swaydb.Error.Map.ExceptionHandler
//
//      val segment = TestSegment(keyValues)
//
//      val initialEntry: MapEntry[Slice[Byte], Segment] = MapEntry.Put[Slice[Byte], Segment](0, segment)
//      var entry =
//        Range.inclusive(1, 10000).foldLeft(initialEntry) {
//          case (previousEntry, i) =>
//            previousEntry ++ MapEntry.Put[Slice[Byte], Segment](i, segment)
//        }
//      entry =
//        Range.inclusive(5000, 10000).foldLeft(entry) {
//          case (previousEntry, i) =>
//            previousEntry ++ MapEntry.Remove[Slice[Byte]](i)
//        }
//
//      entry.hasRange shouldBe false
//
//      val bytes = Slice.create[Byte](entry.entryBytesSize)
//      entry writeTo bytes
//      bytes.isFull shouldBe true //fully written! No gaps!
//
//      val readMapEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(bytes)).runRandomIO.right.value.value
//
//      val skipList = SkipList.concurrent[SliceOption[Byte], SegmentOptional, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
//      readMapEntry applyTo skipList
//      skipList should have size 5000
//      skipList.headKey.value shouldBe (0: Slice[Byte])
//      skipList.lastKey.value shouldBe (4999: Slice[Byte])
//
//      segment.close.runRandomIO.right.value
//    }
//  }
//
//  "distinct" should {
//    "remove older entries when all key-values are duplicate" in {
//      import LevelZeroMapEntryWriter._
//
//      val oldEntries =
//        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("old"))): MapEntry[Slice[Byte], Memory]) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, Some("old"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, Some("old"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("old")) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, Some("old")))
//
//      val newEntries =
//        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("new"))): MapEntry[Slice[Byte], Memory]) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, Some("new"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, Some("new"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, Some("new")))
//
//      MapEntry.distinct(newEntries, oldEntries).entries shouldBe newEntries.entries
//    }
//
//    "remove older duplicate entries" in {
//      import LevelZeroMapEntryWriter._
//
//      val oldEntries =
//        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("old"))): MapEntry[Slice[Byte], Memory]) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, Some("old"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("old")) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](5, Memory.put(4, Some("old")))
//
//      val newEntries =
//        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("new"))): MapEntry[Slice[Byte], Memory]) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, Some("new"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, Some("new"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, Some("new")))
//
//      val expected =
//        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some("new"))): MapEntry[Slice[Byte], Memory]) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, Some("new"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, Some("new"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, Some("new"))) ++
//          MapEntry.Put[Slice[Byte], Memory.Put](5, Memory.put(4, Some("old")))
//
//      MapEntry.distinct(newEntries, oldEntries).entries shouldBe expected.entries
//    }
//  }
//}
