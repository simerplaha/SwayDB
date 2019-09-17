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

import org.scalatest.OptionValues._
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data.{Memory, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.map.MapEntry
import swaydb.core.util.SkipList
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._

class Level0MapEntrySpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  "MapEntryWriterLevel0 & MapEntryReaderLevel0" should {

    "Write random entries" in {
      import swaydb.Error.Map.ExceptionHandler

      def assert[V <: Memory.SegmentResponse](addEntry: MapEntry.Put[Slice[Byte], V])(implicit writer: MapEntryWriter[MapEntry.Put[Slice[Byte], V]],
                                                                                      reader: MapEntryReader[MapEntry.Put[Slice[Byte], V]]) = {
        val slice = Slice.create[Byte](addEntry.entryBytesSize)
        addEntry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        reader.read(Reader(slice.drop(ByteSizeOf.int))).runRandomIO.right.value.value shouldBe addEntry

        import LevelZeroMapEntryReader.Level0Reader
        val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.SegmentResponse]](Reader[swaydb.Error.Map](slice)).runRandomIO.right.value.value
        readEntry shouldBe addEntry

        val skipList = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
        readEntry applyTo skipList
        val scalaSkipList = skipList.asScala

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (addEntry.value.key: Slice[Byte])
        headValue shouldBe addEntry.value
      }

      val keyValues =
        randomizedKeyValues(
          count = 1000,
          addPut = true,
          addRemoves = true,
          addRangeRemoves = true,
          addUpdates = true,
          addFunctions = true,
          addRanges = true,
          addPendingApply = true,
          addRemoveDeadlines = true,
          addPutDeadlines = true,
          addExpiredPutDeadlines = true,
          addUpdateDeadlines = true
        )

      keyValues foreach {
        case keyValue: Transient.Remove =>
          import LevelZeroMapEntryReader.Level0RemoveReader
          import LevelZeroMapEntryWriter.Level0RemoveWriter
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Remove]))

        case keyValue: Transient.Put =>
          import LevelZeroMapEntryReader.Level0PutReader
          import LevelZeroMapEntryWriter.Level0PutWriter
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Put]))

        case keyValue: Transient.Update =>
          import LevelZeroMapEntryReader.Level0UpdateReader
          import LevelZeroMapEntryWriter.Level0UpdateWriter
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Update]))

        case keyValue: Transient.Function =>
          import LevelZeroMapEntryReader.Level0FunctionReader
          import LevelZeroMapEntryWriter.Level0FunctionWriter
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Function]))

        case keyValue: Transient.PendingApply =>
          import LevelZeroMapEntryReader.Level0PendingApplyReader
          import LevelZeroMapEntryWriter.Level0PendingApplyWriter
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.PendingApply]))

        case keyValue: Transient.Range =>
          import LevelZeroMapEntryReader.Level0RangeReader
          import LevelZeroMapEntryWriter.Level0RangeWriter
          assert(MapEntry.Put(keyValue.key, keyValue.toMemory.asInstanceOf[Memory.Range]))
      }
    }

    "write, remove & update key-value" in {
      import LevelZeroMapEntryWriter.{Level0PutWriter, Level0RangeWriter, Level0RemoveWriter, Level0UpdateWriter}
      import swaydb.Error.Map.ExceptionHandler

      val put1 = Memory.put(1, randomStringOption, randomDeadlineOption)
      val put2 = Memory.put(2, randomStringOption, randomDeadlineOption)
      val put3 = Memory.put(3, randomStringOption, randomDeadlineOption)
      val put4 = Memory.put(4, randomStringOption, randomDeadlineOption)
      val put5 = Memory.put(5, randomStringOption, randomDeadlineOption)

      val remove1 = Memory.remove(1, randomDeadlineOption)
      val remove2 = Memory.remove(2, randomDeadlineOption)

      val update1 = Memory.update(3, randomStringOption, randomDeadlineOption)

      val range1 = randomRangeKeyValue(6, 7)
      val range2 = randomRangeKeyValue(7, 8)
      val range3 = randomRangeKeyValue(8, 9)
      val range4 = randomRangeKeyValue(9, 10)
      val range5 = randomRangeKeyValue(10, 11)
      val range6 = randomRangeKeyValue(11, 12)

      val entry: MapEntry[Slice[Byte], Memory.SegmentResponse] =
        (MapEntry.Put[Slice[Byte], Memory.Put](1, put1): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
          MapEntry.Put[Slice[Byte], Memory.Put](2, put2) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](1, remove1) ++
          MapEntry.Put[Slice[Byte], Memory.Put](3, put3) ++
          MapEntry.Put[Slice[Byte], Memory.Remove](2, remove2) ++
          MapEntry.Put[Slice[Byte], Memory.Put](4, put4) ++
          MapEntry.Put[Slice[Byte], Memory.Put](5, put5) ++
          MapEntry.Put[Slice[Byte], Memory.Update](3, update1) ++
          MapEntry.Put[Slice[Byte], Memory.Range](6, range1) ++
          MapEntry.Put[Slice[Byte], Memory.Range](7, range2) ++
          MapEntry.Put[Slice[Byte], Memory.Range](8, range3) ++
          MapEntry.Put[Slice[Byte], Memory.Range](9, range4) ++
          MapEntry.Put[Slice[Byte], Memory.Range](10, range5) ++
          MapEntry.Put[Slice[Byte], Memory.Range](11, range6)

      val slice = Slice.create[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import LevelZeroMapEntryReader.Level0Reader
      val readEntry = MapEntryReader.read[MapEntry[Slice[Byte], Memory.SegmentResponse]](Reader[swaydb.Error.Map](slice)).runRandomIO.right.value.value
      readEntry shouldBe entry

      val skipList = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
      readEntry applyTo skipList
      val scalaSkipList = skipList.asScala
      assertSkipList()

      def assertSkipList() = {
        scalaSkipList should have size 11
        scalaSkipList.get(1).value shouldBe remove1
        scalaSkipList.get(2).value shouldBe remove2
        scalaSkipList.get(3).value shouldBe update1
        scalaSkipList.get(4).value shouldBe put4
        scalaSkipList.get(5).value shouldBe put5
        scalaSkipList.get(6).value shouldBe range1
        scalaSkipList.get(7).value shouldBe range2
        scalaSkipList.get(8).value shouldBe range3
        scalaSkipList.get(9).value shouldBe range4
        scalaSkipList.get(10).value shouldBe range5
        scalaSkipList.get(11).value shouldBe range6
      }
      //write skip list to bytes should result in the same skip list as before
      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      val bytes = MapCodec.write[Slice[Byte], Memory.SegmentResponse](skipList)
      val recoveryResult = MapCodec.read[Slice[Byte], Memory.SegmentResponse](bytes, false).runRandomIO.right.value
      recoveryResult.result shouldBe IO.unit

      val readEntries = recoveryResult.item.value
      //clear and apply new skipList and the result should be the same as previous.
      skipList.clear()
      readEntries applyTo skipList
      assertSkipList()
    }
  }
}
