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

package swaydb.core.map.serializer

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.core.map.MapEntry
import swaydb.core.segment.{Segment, SegmentIO, SegmentOption}
import swaydb.core.util.skiplist.SkipList
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.util.OperatingSystem
import swaydb.serializers.Default._
import swaydb.serializers._

class AppendixMapEntrySpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO: SegmentIO = SegmentIO.random

  "MapEntryWriterAppendix & MapEntryReaderAppendix" should {

    "write Add segment" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val segment = TestSegment()
          val appendixReader = AppendixMapEntryReader(MMAP.Enabled(OperatingSystem.isWindows))

          import AppendixMapEntryWriter.AppendixPutWriter
          val entry = MapEntry.Put[Slice[Byte], Segment](segment.minKey, segment)

          val slice = Slice.create[Byte](entry.entryBytesSize)
          entry writeTo slice
          slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

          import appendixReader.AppendixPutReader
          MapEntryReader.read[MapEntry.Put[Slice[Byte], Segment]](Reader(slice.drop(1))) shouldBe entry

          import appendixReader.AppendixReader
          val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(slice))
          readEntry shouldBe entry

          val skipList = SkipList.concurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          readEntry applyTo skipList
          val scalaSkipList = skipList.asScala

          scalaSkipList should have size 1
          val (headKey, headValue) = scalaSkipList.head
          headKey shouldBe segment.minKey
          headValue shouldBe segment
      }
    }

    "write Remove Segment" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val appendixReader = AppendixMapEntryReader(MMAP.Enabled(OperatingSystem.isWindows))

          import AppendixMapEntryWriter.AppendixRemoveWriter
          val entry = MapEntry.Remove[Slice[Byte]](1)

          val slice = Slice.create[Byte](entry.entryBytesSize)
          entry writeTo slice
          slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

          import appendixReader.AppendixRemoveReader
          MapEntryReader.read[MapEntry.Remove[Slice[Byte]]](Reader(slice.drop(1))).key shouldBe entry.key

          import appendixReader.AppendixReader
          val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(slice))
          readEntry shouldBe entry

          val skipList = SkipList.concurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          readEntry applyTo skipList
          skipList shouldBe empty
      }
    }

    "write and remove key-value" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          import AppendixMapEntryWriter.{AppendixPutWriter, AppendixRemoveWriter}

          val appendixReader = AppendixMapEntryReader(MMAP.Enabled(OperatingSystem.isWindows))

          val segment1 = TestSegment()
          val segment2 = TestSegment()
          val segment3 = TestSegment()
          val segment4 = TestSegment()
          val segment5 = TestSegment()

          val entry: MapEntry[Slice[Byte], Segment] =
            (MapEntry.Put[Slice[Byte], Segment](segment1.minKey, segment1): MapEntry[Slice[Byte], Segment]) ++
              MapEntry.Put[Slice[Byte], Segment](segment2.minKey, segment2) ++
              MapEntry.Remove[Slice[Byte]](segment1.minKey) ++
              MapEntry.Put[Slice[Byte], Segment](segment3.minKey, segment3) ++
              MapEntry.Put[Slice[Byte], Segment](segment4.minKey, segment4) ++
              MapEntry.Remove[Slice[Byte]](segment2.minKey) ++
              MapEntry.Put[Slice[Byte], Segment](segment5.minKey, segment5)

          val slice = Slice.create[Byte](entry.entryBytesSize)
          entry writeTo slice
          slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

          import appendixReader.AppendixReader
          val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(slice))
          readEntry shouldBe entry

          val skipList = SkipList.concurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null)(keyOrder)
          readEntry applyTo skipList

          def scalaSkipList = skipList.asScala

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
          import appendixReader.AppendixReader
          val bytes = MapCodec.write[Slice[Byte], Segment](skipList)
          val crcEntries = MapCodec.read[Slice[Byte], Segment](bytes, false).value.item.value
          skipList.clear()
          crcEntries applyTo skipList
          assertSkipList()
      }
    }
  }
}
