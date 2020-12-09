/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.map

import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, MemoryOption, Value}
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer._
import swaydb.core.segment.{Segment, SegmentReadIO, SegmentOption}
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent}
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestTimer}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.util.{ByteSizeOf, OperatingSystem}
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class MapEntrySpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO: SegmentReadIO = SegmentReadIO.random

  val keyValues = randomKeyValues(count = 10)

  "MapEntry" should {
    "set hasRemoveDeadline to true if Put has remove deadline" in {
      import LevelZeroMapEntryWriter._
      MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1, 1.second.fromNow)).hasRemoveDeadline shouldBe true
    }

    "set hasRemoveDeadline to false if Put has remove deadline" in {
      import LevelZeroMapEntryWriter._
      MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1)).hasRemoveDeadline shouldBe false
    }

    "set hasUpdate to true if Put has update" in {
      import LevelZeroMapEntryWriter._
      val entry = MapEntry.Put[Slice[Byte], Memory.Update](1, Memory.update(1, 1))
      entry.hasRemoveDeadline shouldBe false
      entry.hasUpdate shouldBe true
    }

    "set hasUpdate to false if Put does not have update" in {
      import LevelZeroMapEntryWriter._

      val entry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, 1))
      entry.hasRemoveDeadline shouldBe false
      entry.hasUpdate shouldBe false

      val entry2 = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
      entry2.hasRemoveDeadline shouldBe false
      entry2.hasUpdate shouldBe false
    }

    "put Level0 single Put entry to skipList" in {
      import LevelZeroMapEntryWriter._
      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val entry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "one"))
      entry.hasRange shouldBe false

      entry applyBatch skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe Memory.put(1, "one")
    }

    "put Level0 single Remote entry to skipList" in {
      import LevelZeroMapEntryWriter._
      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val entry = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
      entry.hasRange shouldBe false

      entry applyBatch skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe Memory.remove(1)
    }

    "put Level0 single Put Range entry to skipList" in {
      import LevelZeroMapEntryWriter._
      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val range1 = Memory.Range(1, 10, Value.put("one"), Value.update("range one"))
      val entry1 = MapEntry.Put[Slice[Byte], Memory.Range](1, range1)
      entry1.hasRange shouldBe true

      entry1 applyBatch skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe range1

      val range2 = Memory.Range(2, 10, Value.FromValue.Null, Value.update("range one"))
      val entry2 = MapEntry.Put[Slice[Byte], Memory.Range](2, range2)
      entry2.hasRange shouldBe true

      entry2 applyBatch skipList
      skipList should have size 2
      skipList.get(2: Slice[Byte]) shouldBe range2
    }

    "put Level0 single Remove Range entry to skipList" in {
      import LevelZeroMapEntryWriter._
      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val range1 = Memory.Range(1, 10, Value.remove(None), Value.remove(None))
      val entry1 = MapEntry.Put[Slice[Byte], Memory.Range](1, range1)
      entry1.hasRange shouldBe true

      entry1 applyBatch skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe range1

      val range2 = Memory.Range(2, 10, Value.FromValue.Null, Value.remove(None))
      val entry2 = MapEntry.Put[Slice[Byte], Memory.Range](2, range2)
      entry2.hasRange shouldBe true

      entry2 applyBatch skipList
      skipList should have size 2
      skipList.get(2: Slice[Byte]) shouldBe range2
    }

    "batch multiple Level0 key-value to skipList" in {
      import LevelZeroMapEntryWriter._

      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val entry =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "one")): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "two")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "three")) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "four")) ++
          MapEntry.Put[Slice[Byte], Memory.Range](5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](11, Memory.Range(11, 20, Value.put(20), Value.update(20))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](21, Memory.Range(21, 30, Value.FromValue.Null, Value.remove(None))) ++
          MapEntry.Put[Slice[Byte], Memory.Range](31, Memory.Range(31, 40, Value.put(20), Value.remove(None)))

      entry applyBatch skipList

      entry.hasRange shouldBe true

      skipList should have size 8
      skipList.get(1: Slice[Byte]) shouldBe Memory.put(1, "one")
      skipList.get(2: Slice[Byte]) shouldBe Memory.remove(2)
      skipList.get(3: Slice[Byte]) shouldBe Memory.put(3, "three")
      skipList.get(4: Slice[Byte]) shouldBe Memory.put(4, "four")
      skipList.get(5: Slice[Byte]) shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
      skipList.get(11: Slice[Byte]) shouldBe Memory.Range(11, 20, Value.put(20), Value.update(20))
      skipList.get(21: Slice[Byte]) shouldBe Memory.Range(21, 30, Value.FromValue.Null, Value.remove(None))
      skipList.get(31: Slice[Byte]) shouldBe Memory.Range(31, 40, Value.put(20), Value.remove(None))
    }

    "add Appendix single Put entry to skipList" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixMapEntryWriter._
          val segment = TestSegment(keyValues)

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)

          val entry = MapEntry.Put[Slice[Byte], Segment](1, segment)
          entry.hasRange shouldBe false

          entry applyBatch skipList
          skipList should have size 1
          skipList.get(1: Slice[Byte]).getS shouldBe segment
      }
    }

    "remove Appendix entry from skipList" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixMapEntryWriter._
          val segment = TestSegment(keyValues)

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)

          val entry = MapEntry.Put[Slice[Byte], Segment](1, segment)
          entry.hasRange shouldBe false

          entry applyBatch skipList
          skipList should have size 1
          skipList.get(1: Slice[Byte]).getS shouldBe segment

          MapEntry.Remove[Slice[Byte]](1) applyBatch skipList
          skipList shouldBe empty
      }
    }

    "batch multiple appendix entries to skipList" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixMapEntryWriter._

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          val segment1 = TestSegment().runRandomIO.right.value
          val segment2 = TestSegment().runRandomIO.right.value
          val segment3 = TestSegment().runRandomIO.right.value
          val segment4 = TestSegment().runRandomIO.right.value

          val entry =
            (MapEntry.Put[Slice[Byte], Segment](1, segment1): MapEntry[Slice[Byte], Segment]) ++
              MapEntry.Put[Slice[Byte], Segment](2, segment2) ++
              MapEntry.Put[Slice[Byte], Segment](3, segment3) ++
              MapEntry.Remove[Slice[Byte]](2) ++
              MapEntry.Put[Slice[Byte], Segment](4, segment4)

          entry.hasRange shouldBe false

          entry applyBatch skipList

          skipList should have size 3
          skipList.get(1: Slice[Byte]).getS shouldBe segment1
          skipList.get(2: Slice[Byte]).toOptionS shouldBe empty
          skipList.get(3: Slice[Byte]).getS shouldBe segment3
          skipList.get(4: Slice[Byte]).getS shouldBe segment4
      }
    }
  }

  "MapEntry.Put" should {
    "write and read bytes for a single Level0 key-value" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val entry = MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "one"))
      entry.hasRange shouldBe false

      val bytes = Slice.of[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Put]](bytes.drop(ByteSizeOf.byte)).runRandomIO.right.value shouldBe entry
      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value shouldBe entry
    }

    "write and read bytes for a single Appendix" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader =
            AppendixMapEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              removeDeletes = false,
              segmentRefCacheWeight = randomByte()
            )

          import AppendixMapEntryWriter._
          import appendixReader._
          val segment = TestSegment(keyValues)

          val entry = MapEntry.Put[Slice[Byte], Segment](segment.minKey, segment)
          entry.hasRange shouldBe false

          val bytes = Slice.of[Byte](entry.entryBytesSize)
          entry writeTo bytes
          bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

          MapEntryReader.read[MapEntry.Put[Slice[Byte], Segment]](bytes.drop(1)).runRandomIO.right.value shouldBe entry
          MapEntryReader.read[MapEntry[Slice[Byte], Segment]](bytes).runRandomIO.right.value shouldBe entry
      }
    }
  }

  "MapEntry.Remove" should {
    "write and read bytes for a single Level0 key-values entry" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val entry = MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
      entry.hasRange shouldBe false

      val bytes = Slice.of[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry.Put[Slice[Byte], Memory.Remove]](bytes.drop(ByteSizeOf.byte)).runRandomIO.right.value shouldBe entry
      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value shouldBe entry
    }

    "write and read bytes for single Appendix entry" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader =
            AppendixMapEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              removeDeletes = false,
              segmentRefCacheWeight = randomByte()
            )

          import AppendixMapEntryWriter._
          import appendixReader._

          val segment = TestSegment(keyValues)

          //do remove
          val entry = MapEntry.Remove[Slice[Byte]](segment.minKey)
          entry.hasRange shouldBe false

          val bytes = Slice.of[Byte](entry.entryBytesSize)
          entry writeTo bytes
          bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

          MapEntryReader.read[MapEntry.Remove[Slice[Byte]]](bytes.drop(1)).runRandomIO.right.value.key shouldBe entry.key
          MapEntryReader.read[MapEntry[Slice[Byte], Segment]](bytes).runRandomIO.right.value shouldBe entry
      }
    }
  }

  "MapEntry" should {
    "batch write multiple key-values for Level0" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val entry =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "one")): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "two")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "three")) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "four"))

      entry.hasRange shouldBe false

      val bytes = Slice.of[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      MapEntryReader.read[MapEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value shouldBe entry
    }
  }

  "10000 map entries" can {

    "be written and read for Level0" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val initialEntry: MapEntry[Slice[Byte], Memory] = MapEntry.Put(0, Memory.put(0, 0))
      var entry =
        Range.inclusive(1, 10000).foldLeft(initialEntry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Put[Slice[Byte], Memory.Put](i, Memory.put(i, i))
        }
      entry =
        Range.inclusive(5000, 10000).foldLeft(entry) {
          case (previousEntry, i) =>
            previousEntry ++ MapEntry.Put[Slice[Byte], Memory.Remove](i, Memory.remove(i))
        }

      entry.hasRange shouldBe false

      val bytes = Slice.of[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps!

      val readMapEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory]](Reader(bytes)).runRandomIO.right.value

      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      readMapEntry applyBatch skipList
      skipList should have size 10001
      skipList.headKey.getC shouldBe (0: Slice[Byte])
      skipList.lastKey.getC shouldBe (10000: Slice[Byte])
      skipList.subMap(0, true, 4999, true).foreach(_._2.isInstanceOf[Memory.Put] shouldBe true)
      skipList.subMap(5000, true, 10000, true).foreach(_._2.isInstanceOf[Memory.Remove] shouldBe true)
    }

    "be written and read for Appendix" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader =
            AppendixMapEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              removeDeletes = false,
              segmentRefCacheWeight = randomByte()
            )
          import AppendixMapEntryWriter._
          import appendixReader._

          val keyValues = randomKeyValues(100, startId = Some(0))

          val segments: Slice[Segment] =
            keyValues.groupedSlice(keyValues.size) map {
              keyValues =>
                TestSegment(keyValues)
            }

          segments.zipWithIndex foreach {
            case (segment, index) =>
              segment.minKey.readInt() shouldBe index
              segment.getKeyValueCount() shouldBe 1
          }

          val initialEntry: MapEntry[Slice[Byte], Segment] = MapEntry.Put[Slice[Byte], Segment](0, segments.head)

          var entry =
            (1 until 100).foldLeft(initialEntry) {
              case (previousEntry, i) =>
                val segment = segments(i)
                previousEntry ++ MapEntry.Put[Slice[Byte], Segment](segment.minKey, segment)
            }

          entry =
            (50 until 100).foldLeft(entry) {
              case (previousEntry, i) =>
                val segment = segments(i)
                previousEntry ++ MapEntry.Remove[Slice[Byte]](segment.minKey)
            }

          entry.hasRange shouldBe false

          val bytes = Slice.of[Byte](entry.entryBytesSize)
          entry writeTo bytes
          bytes.isFull shouldBe true //fully written! No gaps!

          val readMapEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(bytes))

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          readMapEntry applyBatch skipList
          skipList should have size 50
          skipList.headKey.getC shouldBe (0: Slice[Byte])
          skipList.lastKey.getC shouldBe (49: Slice[Byte])
      }
    }
  }

  "distinct" should {
    "remove older entries when all key-values are duplicate" in {
      import LevelZeroMapEntryWriter._

      val oldEntries =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "old")): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "old")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "old")) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("old")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "old"))

      val newEntries =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "new")): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "new")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "new")) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "new"))

      MapEntry.distinct(newEntries, oldEntries).entries shouldBe newEntries.entries
    }

    "remove older duplicate entries" in {
      import LevelZeroMapEntryWriter._

      val oldEntries =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "old")): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "old")) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("old")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](5, Memory.put(4, "old"))

      val newEntries =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "new")): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "new")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "new")) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "new"))

      val expected =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "new")): MapEntry[Slice[Byte], Memory]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "new")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "new")) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "new")) ++
          MapEntry.Put[Slice[Byte], Memory.Put](5, Memory.put(4, "old"))

      MapEntry.distinct(newEntries, oldEntries).entries shouldBe expected.entries
    }
  }
}
