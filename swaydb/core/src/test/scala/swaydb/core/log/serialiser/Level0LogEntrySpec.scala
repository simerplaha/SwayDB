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
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IO
import swaydb.core.log.LogEntry
import swaydb.core.log.timer.TestTimer
import swaydb.core.log.LogTestKit._
import swaydb.core.segment.data.{Memory, MemoryOption}
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.TestCoreFunctionStore
import swaydb.core.skiplist.SkipListConcurrent
import swaydb.effect.IOValues._
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.{Slice, SliceOption, SliceReader}
import swaydb.slice.order.KeyOrder
import swaydb.slice.SliceTestKit._
import swaydb.testkit.TestKit._
import swaydb.utils.ByteSizeOf

class Level0LogEntrySpec extends AnyWordSpec {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty

  "LogEntryWriterLevel0 & LogEntryReaderLevel0" should {

    "Write random entries" in {
      def assert[V <: Memory](addEntry: LogEntry.Put[Slice[Byte], V])(implicit writer: LogEntryWriter[LogEntry.Put[Slice[Byte], V]],
                                                                      reader: LogEntryReader[LogEntry.Put[Slice[Byte], V]]) = {
        val slice = Slice.allocate[Byte](addEntry.entryBytesSize)
        addEntry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        reader.read(SliceReader(slice.drop(ByteSizeOf.byte))).runRandomIO.get shouldBe addEntry

        import MemoryLogEntryReader.KeyValueLogEntryPutReader
        val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.get
        readEntry shouldBe addEntry

        val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        readEntry applyBatch skipList
        val scalaSkipList = skipList.toIterable

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (addEntry.value.key: Slice[Byte])
        headValue shouldBe addEntry.value
      }

      implicit val testCoreFunctionStore: TestCoreFunctionStore = TestCoreFunctionStore()

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
        case keyValue: Memory.Remove =>
          import MemoryLogEntryReader.RemoveLogEntryPutReader
          import MemoryLogEntryWriter.RemoveLogEntryPutWriter
          assert(LogEntry.Put(keyValue.key, keyValue))

        case keyValue: Memory.Put =>
          import MemoryLogEntryReader.PutLogEntryPutReader
          import MemoryLogEntryWriter.PutLogEntryPutWriter
          assert(LogEntry.Put(keyValue.key, keyValue))

        case keyValue: Memory.Update =>
          import MemoryLogEntryReader.UpdateLogEntryPutReader
          import MemoryLogEntryWriter.UpdateLogEntryPutWriter
          assert(LogEntry.Put(keyValue.key, keyValue))

        case keyValue: Memory.Function =>
          import MemoryLogEntryReader.FunctionLogEntryPutReader
          import MemoryLogEntryWriter.FunctionLogEntryPutWriter
          assert(LogEntry.Put(keyValue.key, keyValue))

        case keyValue: Memory.PendingApply =>
          import MemoryLogEntryReader.PendingApplyLogEntryPutReader
          import MemoryLogEntryWriter.PendingApplyLogEntryPutWriter
          assert(LogEntry.Put(keyValue.key, keyValue))

        case keyValue: Memory.Range =>
          import MemoryLogEntryReader.RangeLogEntryPutReader
          import MemoryLogEntryWriter.RangeLogEntryPutWriter
          assert(LogEntry.Put(keyValue.key, keyValue))
      }
    }

    "write, remove & update key-value" in {
      import MemoryLogEntryWriter.{PutLogEntryPutWriter, RangeLogEntryPutWriter, RemoveLogEntryPutWriter, UpdateLogEntryPutWriter}
      import swaydb.Error.Log.ExceptionHandler

      val put1 = Memory.put(1, randomStringOption(), randomDeadlineOption())
      val put2 = Memory.put(2, randomStringOption(), randomDeadlineOption())
      val put3 = Memory.put(3, randomStringOption(), randomDeadlineOption())
      val put4 = Memory.put(4, randomStringOption(), randomDeadlineOption())
      val put5 = Memory.put(5, randomStringOption(), randomDeadlineOption())

      val remove1 = Memory.remove(1, randomDeadlineOption())
      val remove2 = Memory.remove(2, randomDeadlineOption())

      val update1 = Memory.update(3, randomStringOption(), randomDeadlineOption())

      val range1 = randomRangeKeyValue(6, 7)
      val range2 = randomRangeKeyValue(7, 8)
      val range3 = randomRangeKeyValue(8, 9)
      val range4 = randomRangeKeyValue(9, 10)
      val range5 = randomRangeKeyValue(10, 11)
      val range6 = randomRangeKeyValue(11, 12)

      val entry: LogEntry[Slice[Byte], Memory] =
        (LogEntry.Put[Slice[Byte], Memory.Put](1, put1): LogEntry[Slice[Byte], Memory]) ++
          LogEntry.Put[Slice[Byte], Memory.Put](2, put2) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](1, remove1) ++
          LogEntry.Put[Slice[Byte], Memory.Put](3, put3) ++
          LogEntry.Put[Slice[Byte], Memory.Remove](2, remove2) ++
          LogEntry.Put[Slice[Byte], Memory.Put](4, put4) ++
          LogEntry.Put[Slice[Byte], Memory.Put](5, put5) ++
          LogEntry.Put[Slice[Byte], Memory.Update](3, update1) ++
          LogEntry.Put[Slice[Byte], Memory.Range](6, range1) ++
          LogEntry.Put[Slice[Byte], Memory.Range](7, range2) ++
          LogEntry.Put[Slice[Byte], Memory.Range](8, range3) ++
          LogEntry.Put[Slice[Byte], Memory.Range](9, range4) ++
          LogEntry.Put[Slice[Byte], Memory.Range](10, range5) ++
          LogEntry.Put[Slice[Byte], Memory.Range](11, range6)

      val slice = Slice.allocate[Byte](entry.entryBytesSize)
      entry writeTo slice
      slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

      import MemoryLogEntryReader.KeyValueLogEntryPutReader
      val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.get
      readEntry shouldBe entry

      val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      readEntry applyBatch skipList

      def scalaSkipList = skipList.toIterable

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
      import MemoryLogEntryWriter.LogEntryPutWriter
      val bytes = LogEntryParser.write[Slice[Byte], Memory](skipList.iterator)
      val recoveryResult = LogEntryParser.read[Slice[Byte], Memory](bytes, false).runRandomIO.get
      recoveryResult.result shouldBe IO.unit

      val readEntries = recoveryResult.item.value
      //clear and apply new skipList and the result should be the same as previous.
      skipList.clear()
      readEntries applyBatch skipList
      assertSkipList()
    }
  }
}
