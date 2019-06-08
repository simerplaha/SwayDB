/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.map.serializer

import java.util.concurrent.ConcurrentSkipListMap
import swaydb.core.data.Persistent
import swaydb.core.data.Persistent
import swaydb.core.io.reader.Reader
import swaydb.core.map.MapEntry
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.Segment
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.slice.Slice
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._
import scala.collection.JavaConverters._
import swaydb.core.io.file.DBFile

class AppendixMapEntrySpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def compression = randomGroupingStrategyOption(randomNextInt(1000))

  val appendixReader = AppendixMapEntryReader(true, true)
  val segment = TestSegment().assertGet

  "MapEntryWriterAppendix & MapEntryReaderAppendix" should {

    "write Add segment" in {
      import AppendixMapEntryWriter.AppendixPutWriter
      val entry = MapEntry.Put[Slice[Byte], Segment](segment.minKey, segment)

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import appendixReader.AppendixPutReader
      MapEntryReader.read[MapEntry.Put[Slice[Byte], Segment]](Reader(slice.drop(1))).assertGet shouldBe entry

      import appendixReader.AppendixReader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](keyOrder)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala

      scalaSkipList should have size 1
      val (headKey, headValue) = scalaSkipList.head
      headKey shouldBe segment.minKey
      headValue shouldBe segment
    }

    "write Remove Segment" in {
      import AppendixMapEntryWriter.AppendixRemoveWriter
      val entry = MapEntry.Remove[Slice[Byte]](1)

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import appendixReader.AppendixRemoveReader
      MapEntryReader.read[MapEntry.Remove[Slice[Byte]]](Reader(slice.drop(1))).assertGet.key shouldBe entry.key

      import appendixReader.AppendixReader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](keyOrder)
      readEntry applyTo skipList
      skipList shouldBe empty

    }

    "write and remove key-value" in {
      import AppendixMapEntryWriter.{AppendixPutWriter, AppendixRemoveWriter}

      val segment1 = TestSegment().assertGet
      val segment2 = TestSegment().assertGet
      val segment3 = TestSegment().assertGet
      val segment4 = TestSegment().assertGet
      val segment5 = TestSegment().assertGet

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
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Segment]](Reader(slice)).assertGet
      readEntry shouldBe entry

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Segment](keyOrder)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala
      assertSkipList()

      def assertSkipList() = {
        scalaSkipList should have size 3
        scalaSkipList.get(segment1.minKey) shouldBe empty
        scalaSkipList.get(segment2.minKey) shouldBe empty
        scalaSkipList.get(segment3.minKey).assertGet shouldBe segment3
        scalaSkipList.get(segment4.minKey).assertGet shouldBe segment4
        scalaSkipList.get(segment5.minKey).assertGet shouldBe segment5
      }
      //write skip list to bytes should result in the same skip list as before
      import appendixReader.AppendixReader
      val bytes = MapCodec.write[Slice[Byte], Segment](skipList)
      val crcEntries = MapCodec.read[Slice[Byte], Segment](bytes, false).assertGet.item.assertGet
      skipList.clear()
      crcEntries applyTo skipList
      assertSkipList()
    }
  }
}
