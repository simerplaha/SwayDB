/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.log

import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, MemoryOption, Value}
import swaydb.core.io.reader.Reader
import swaydb.core.log.serializer._
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.skiplist.SkipListConcurrent
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestTimer}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.slice.{Slice, SliceOption}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.utils.{ByteSizeOf, OperatingSystem}

import scala.concurrent.duration._

class LogEntrySpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO: SegmentReadIO = SegmentReadIO.random

  val keyValues = randomKeyValues(count = 10)

  "LogEntry" should {
    "set hasRemoveDeadline to true if Put has remove deadline" in {
      import LevelZeroLogEntryWriter._
      LogEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1, 1.second.fromNow)).hasRemoveDeadline shouldBe true
    }

    "set hasRemoveDeadline to false if Put has remove deadline" in {
      import LevelZeroLogEntryWriter._
      LogEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1)).hasRemoveDeadline shouldBe false
    }

    "set hasUpdate to true if Put has update" in {
      import LevelZeroLogEntryWriter._
      val entry = LogEntry.Put[Slice[Byte], Memory.Update](1, Memory.update(1, 1))
      entry.hasRemoveDeadline shouldBe false
      entry.hasUpdate shouldBe true
    }

    "set hasUpdate to false if Put does not have update" in {
      import LevelZeroLogEntryWriter._

      val entry = LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, 1))
      entry.hasRemoveDeadline shouldBe false
      entry.hasUpdate shouldBe false

      val entry2 = LogEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
      entry2.hasRemoveDeadline shouldBe false
      entry2.hasUpdate shouldBe false
    }

    "put Level0 single Put entry to skipList" in {
      import LevelZeroLogEntryWriter._
      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val entry = LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "one"))
      entry.hasRange shouldBe false

      entry applyBatch skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe Memory.put(1, "one")
    }

    "put Level0 single Remote entry to skipList" in {
      import LevelZeroLogEntryWriter._
      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val entry = LogEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
      entry.hasRange shouldBe false

      entry applyBatch skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe Memory.remove(1)
    }

    "put Level0 single Put Range entry to skipList" in {
      import LevelZeroLogEntryWriter._
      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val range1 = Memory.Range(1, 10, Value.put("one"), Value.update("range one"))
      val entry1 = LogEntry.Put[Slice[Byte], Memory.Range](1, range1)
      entry1.hasRange shouldBe true

      entry1 applyBatch skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe range1

      val range2 = Memory.Range(2, 10, Value.FromValue.Null, Value.update("range one"))
      val entry2 = LogEntry.Put[Slice[Byte], Memory.Range](2, range2)
      entry2.hasRange shouldBe true

      entry2 applyBatch skipList
      skipList should have size 2
      skipList.get(2: Slice[Byte]) shouldBe range2
    }

    "put Level0 single Remove Range entry to skipList" in {
      import LevelZeroLogEntryWriter._
      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val range1 = Memory.Range(1, 10, Value.remove(None), Value.remove(None))
      val entry1 = LogEntry.Put[Slice[Byte], Memory.Range](1, range1)
      entry1.hasRange shouldBe true

      entry1 applyBatch skipList
      skipList should have size 1
      skipList.get(1: Slice[Byte]) shouldBe range1

      val range2 = Memory.Range(2, 10, Value.FromValue.Null, Value.remove(None))
      val entry2 = LogEntry.Put[Slice[Byte], Memory.Range](2, range2)
      entry2.hasRange shouldBe true

      entry2 applyBatch skipList
      skipList should have size 2
      skipList.get(2: Slice[Byte]) shouldBe range2
    }

    "batch multiple Level0 key-value to skipList" in {
      import LevelZeroLogEntryWriter._

      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val entry =
        (LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "one")): LogEntry[Slice[Byte], Memory]) ++
          LogEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "two")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "three")) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)) ++
          LogEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "four")) ++
          LogEntry.Put[Slice[Byte], Memory.Range](5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))) ++
          LogEntry.Put[Slice[Byte], Memory.Range](11, Memory.Range(11, 20, Value.put(20), Value.update(20))) ++
          LogEntry.Put[Slice[Byte], Memory.Range](21, Memory.Range(21, 30, Value.FromValue.Null, Value.remove(None))) ++
          LogEntry.Put[Slice[Byte], Memory.Range](31, Memory.Range(31, 40, Value.put(20), Value.remove(None)))

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
          import AppendixLogEntryWriter._
          val segment = TestSegment(keyValues)

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)

          val entry = LogEntry.Put[Slice[Byte], Segment](1, segment)
          entry.hasRange shouldBe false

          entry applyBatch skipList
          skipList should have size 1
          skipList.get(1: Slice[Byte]).getS shouldBe segment
      }
    }

    "remove Appendix entry from skipList" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixLogEntryWriter._
          val segment = TestSegment(keyValues)

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)

          val entry = LogEntry.Put[Slice[Byte], Segment](1, segment)
          entry.hasRange shouldBe false

          entry applyBatch skipList
          skipList should have size 1
          skipList.get(1: Slice[Byte]).getS shouldBe segment

          LogEntry.Remove[Slice[Byte]](1) applyBatch skipList
          skipList shouldBe empty
      }
    }

    "batch multiple appendix entries to skipList" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixLogEntryWriter._

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          val segment1 = TestSegment().runRandomIO.right.value
          val segment2 = TestSegment().runRandomIO.right.value
          val segment3 = TestSegment().runRandomIO.right.value
          val segment4 = TestSegment().runRandomIO.right.value

          val entry =
            (LogEntry.Put[Slice[Byte], Segment](1, segment1): LogEntry[Slice[Byte], Segment]) ++
              LogEntry.Put[Slice[Byte], Segment](2, segment2) ++
              LogEntry.Put[Slice[Byte], Segment](3, segment3) ++
              LogEntry.Remove[Slice[Byte]](2) ++
              LogEntry.Put[Slice[Byte], Segment](4, segment4)

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

  "LogEntry.Put" should {
    "write and read bytes for a single Level0 key-value" in {
      import LevelZeroLogEntryReader._
      import LevelZeroLogEntryWriter._

      val entry = LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "one"))
      entry.hasRange shouldBe false

      val bytes = Slice.of[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      LogEntryReader.read[LogEntry.Put[Slice[Byte], Memory.Put]](bytes.drop(ByteSizeOf.byte)).runRandomIO.right.value shouldBe entry
      LogEntryReader.read[LogEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value shouldBe entry
    }

    "write and read bytes for a single Appendix" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader =
            AppendixLogEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              segmentRefCacheLife = randomSegmentRefCacheLife()
            )

          import AppendixLogEntryWriter._
          import appendixReader._
          val segment = TestSegment(keyValues)

          val entry = LogEntry.Put[Slice[Byte], Segment](segment.minKey, segment)
          entry.hasRange shouldBe false

          val bytes = Slice.of[Byte](entry.entryBytesSize)
          entry writeTo bytes
          bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

          LogEntryReader.read[LogEntry.Put[Slice[Byte], Segment]](bytes.drop(1)).runRandomIO.right.value shouldBe entry
          LogEntryReader.read[LogEntry[Slice[Byte], Segment]](bytes).runRandomIO.right.value shouldBe entry
      }
    }
  }

  "LogEntry.Remove" should {
    "write and read bytes for a single Level0 key-values entry" in {
      import LevelZeroLogEntryReader._
      import LevelZeroLogEntryWriter._

      val entry = LogEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))
      entry.hasRange shouldBe false

      val bytes = Slice.of[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      LogEntryReader.read[LogEntry.Put[Slice[Byte], Memory.Remove]](bytes.drop(ByteSizeOf.byte)).runRandomIO.right.value shouldBe entry
      LogEntryReader.read[LogEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value shouldBe entry
    }

    "write and read bytes for single Appendix entry" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader =
            AppendixLogEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              segmentRefCacheLife = randomSegmentRefCacheLife()
            )

          import AppendixLogEntryWriter._
          import appendixReader._

          val segment = TestSegment(keyValues)

          //do remove
          val entry = LogEntry.Remove[Slice[Byte]](segment.minKey)
          entry.hasRange shouldBe false

          val bytes = Slice.of[Byte](entry.entryBytesSize)
          entry writeTo bytes
          bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

          LogEntryReader.read[LogEntry.Remove[Slice[Byte]]](bytes.drop(1)).runRandomIO.right.value.key shouldBe entry.key
          LogEntryReader.read[LogEntry[Slice[Byte], Segment]](bytes).runRandomIO.right.value shouldBe entry
      }
    }
  }

  "LogEntry" should {
    "batch write multiple key-values for Level0" in {
      import LevelZeroLogEntryReader._
      import LevelZeroLogEntryWriter._

      val entry =
        (LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "one")): LogEntry[Slice[Byte], Memory]) ++
          LogEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "two")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "three")) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)) ++
          LogEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "four"))

      entry.hasRange shouldBe false

      val bytes = Slice.of[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps! This ensures that the size calculations are correct.

      LogEntryReader.read[LogEntry[Slice[Byte], Memory]](bytes).runRandomIO.right.value shouldBe entry
    }
  }

  "10000 log entries" can {

    "be written and read for Level0" in {
      import LevelZeroLogEntryReader._
      import LevelZeroLogEntryWriter._

      val initialEntry: LogEntry[Slice[Byte], Memory] = LogEntry.Put(0, Memory.put(0, 0))
      var entry =
        Range.inclusive(1, 10000).foldLeft(initialEntry) {
          case (previousEntry, i) =>
            previousEntry ++ LogEntry.Put[Slice[Byte], Memory.Put](i, Memory.put(i, i))
        }
      entry =
        Range.inclusive(5000, 10000).foldLeft(entry) {
          case (previousEntry, i) =>
            previousEntry ++ LogEntry.Put[Slice[Byte], Memory.Remove](i, Memory.remove(i))
        }

      entry.hasRange shouldBe false

      val bytes = Slice.of[Byte](entry.entryBytesSize)
      entry writeTo bytes
      bytes.isFull shouldBe true //fully written! No gaps!

      val readLogEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](Reader(bytes)).runRandomIO.right.value

      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      readLogEntry applyBatch skipList
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
            AppendixLogEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              segmentRefCacheLife = randomSegmentRefCacheLife()
            )
          import AppendixLogEntryWriter._
          import appendixReader._

          val keyValues = randomKeyValues(100, startId = Some(0))

          val segments: Slice[Segment] =
            keyValues.groupedSlice(keyValues.size) mapToSlice {
              keyValues =>
                TestSegment(keyValues)
            }

          segments.zipWithIndex foreach {
            case (segment, index) =>
              segment.minKey.readInt() shouldBe index
              segment.keyValueCount shouldBe 1
          }

          val initialEntry: LogEntry[Slice[Byte], Segment] = LogEntry.Put[Slice[Byte], Segment](0, segments.head)

          var entry =
            (1 until 100).foldLeft(initialEntry) {
              case (previousEntry, i) =>
                val segment = segments(i)
                previousEntry ++ LogEntry.Put[Slice[Byte], Segment](segment.minKey, segment)
            }

          entry =
            (50 until 100).foldLeft(entry) {
              case (previousEntry, i) =>
                val segment = segments(i)
                previousEntry ++ LogEntry.Remove[Slice[Byte]](segment.minKey)
            }

          entry.hasRange shouldBe false

          val bytes = Slice.of[Byte](entry.entryBytesSize)
          entry writeTo bytes
          bytes.isFull shouldBe true //fully written! No gaps!

          val readLogEntry = LogEntryReader.read[LogEntry[Slice[Byte], Segment]](Reader(bytes))

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          readLogEntry applyBatch skipList
          skipList should have size 50
          skipList.headKey.getC shouldBe (0: Slice[Byte])
          skipList.lastKey.getC shouldBe (49: Slice[Byte])
      }
    }
  }

  "distinct" should {
    "remove older entries when all key-values are duplicate" in {
      import LevelZeroLogEntryWriter._

      val oldEntries =
        (LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "old")): LogEntry[Slice[Byte], Memory]) ++
          LogEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "old")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "old")) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("old")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "old"))

      val newEntries =
        (LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "new")): LogEntry[Slice[Byte], Memory]) ++
          LogEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "new")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "new")) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "new"))

      LogEntry.distinct(newEntries, oldEntries).entries shouldBe newEntries.entries
    }

    "remove older duplicate entries" in {
      import LevelZeroLogEntryWriter._

      val oldEntries =
        (LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "old")): LogEntry[Slice[Byte], Memory]) ++
          LogEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "old")) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("old")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](5, Memory.put(4, "old"))

      val newEntries =
        (LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "new")): LogEntry[Slice[Byte], Memory]) ++
          LogEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "new")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "new")) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "new"))

      val expected =
        (LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, "new")): LogEntry[Slice[Byte], Memory]) ++
          LogEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, "new")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](3, Memory.put(3, "new")) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove("new")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](4, Memory.put(4, "new")) ++
          LogEntry.Put[Slice[Byte], Memory.Put](5, Memory.put(4, "old"))

      LogEntry.distinct(newEntries, oldEntries).entries shouldBe expected.entries
    }
  }
}
