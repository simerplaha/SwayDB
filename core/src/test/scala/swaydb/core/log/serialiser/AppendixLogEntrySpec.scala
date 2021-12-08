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

package swaydb.core.log.serialiser

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.config.MMAP
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.file.reader.Reader
import swaydb.core.log.{ALogSpec, LogEntry}
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.{ASegmentSpec, Segment, SegmentOption}
import swaydb.core.skiplist.SkipListConcurrent
import swaydb.core.{ACoreSpec, TestSweeper, TestForceSave}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.{Slice, SliceOption}
import swaydb.utils.OperatingSystem

class AppendixLogEntrySpec extends ASegmentSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO: SegmentReadIO = SegmentReadIO.random

  "LogEntryWriterAppendix & LogEntryReaderAppendix" should {

    "write Add segment" in {
      TestSweeper {
        implicit sweeper =>
          import sweeper._

          val segment = TestSegment()

          val appendixReader =
            AppendixLogEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              segmentRefCacheLife = randomSegmentRefCacheLife()
            )

          import AppendixLogEntryWriter.AppendixPutWriter
          val entry = LogEntry.Put[Slice[Byte], Segment](segment.minKey, segment)

          val slice = Slice.allocate[Byte](entry.entryBytesSize)
          entry writeTo slice
          slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

          import appendixReader.AppendixPutReader
          LogEntryReader.read[LogEntry.Put[Slice[Byte], Segment]](Reader(slice.drop(1))) shouldBe entry

          import appendixReader.AppendixReader
          val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Segment]](Reader(slice))
          readEntry shouldBe entry

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          readEntry applyBatch skipList
          val scalaSkipList = skipList.toIterable

          scalaSkipList should have size 1
          val (headKey, headValue) = scalaSkipList.head
          headKey shouldBe segment.minKey
          headValue shouldBe segment
      }
    }

    "write Remove Segment" in {
      TestSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader = AppendixLogEntryReader(
            mmapSegment =
              MMAP.On(
                deleteAfterClean = OperatingSystem.isWindows,
                forceSave = TestForceSave.mmap()
              ),
            segmentRefCacheLife = randomSegmentRefCacheLife()
          )

          import AppendixLogEntryWriter.AppendixRemoveWriter
          val entry = LogEntry.Remove[Slice[Byte]](1)

          val slice = Slice.allocate[Byte](entry.entryBytesSize)
          entry writeTo slice
          slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

          import appendixReader.AppendixRemoveReader
          LogEntryReader.read[LogEntry.Remove[Slice[Byte]]](Reader(slice.drop(1))).key shouldBe entry.key

          import appendixReader.AppendixReader
          val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Segment]](Reader(slice))
          readEntry shouldBe entry

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          readEntry applyBatch skipList
          skipList shouldBe empty
      }
    }

    "write and remove key-value" in {
      TestSweeper {
        implicit sweeper =>
          import AppendixLogEntryWriter.{AppendixPutWriter, AppendixRemoveWriter}
          import sweeper._

          val appendixReader = AppendixLogEntryReader(
            mmapSegment =
              MMAP.On(
                deleteAfterClean = OperatingSystem.isWindows,
                forceSave = TestForceSave.mmap()
              ),
            segmentRefCacheLife = randomSegmentRefCacheLife()
          )

          val segment1 = TestSegment()
          val segment2 = TestSegment()
          val segment3 = TestSegment()
          val segment4 = TestSegment()
          val segment5 = TestSegment()

          val entry: LogEntry[Slice[Byte], Segment] =
            (LogEntry.Put[Slice[Byte], Segment](segment1.minKey, segment1): LogEntry[Slice[Byte], Segment]) ++
              LogEntry.Put[Slice[Byte], Segment](segment2.minKey, segment2) ++
              LogEntry.Remove[Slice[Byte]](segment1.minKey) ++
              LogEntry.Put[Slice[Byte], Segment](segment3.minKey, segment3) ++
              LogEntry.Put[Slice[Byte], Segment](segment4.minKey, segment4) ++
              LogEntry.Remove[Slice[Byte]](segment2.minKey) ++
              LogEntry.Put[Slice[Byte], Segment](segment5.minKey, segment5)

          val slice = Slice.allocate[Byte](entry.entryBytesSize)
          entry writeTo slice
          slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

          import appendixReader.AppendixReader
          val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Segment]](Reader(slice))
          readEntry shouldBe entry

          val skipList = SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          readEntry applyBatch skipList

          def scalaSkipList = skipList.toIterable

          assertSkipList()

          def assertSkipList() = {
            scalaSkipList should have size 3
            scalaSkipList.get(segment1.minKey) shouldBe empty
            scalaSkipList.get(segment2.minKey) shouldBe empty
            scalaSkipList.get(segment3.minKey).value shouldBe segment3
            scalaSkipList.get(segment4.minKey).value shouldBe segment4
            scalaSkipList.get(segment5.minKey).value shouldBe segment5
          }

          //write skip list to bytes should result in the same skip list as before
          val bytes = LogEntrySerialiser.write[Slice[Byte], Segment](skipList.iterator)
          val crcEntries = LogEntrySerialiser.read[Slice[Byte], Segment](bytes, false).value.item.value
          skipList.clear()
          crcEntries applyBatch skipList
          assertSkipList()
      }
    }
  }
}
