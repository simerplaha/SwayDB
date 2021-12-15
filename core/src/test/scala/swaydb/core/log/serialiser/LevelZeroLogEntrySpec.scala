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
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Error.Log.ExceptionHandler
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.log.LogEntry
import swaydb.core.segment.data.{Memory, MemoryOption}
import swaydb.core.skiplist.SkipListConcurrent
import swaydb.core.{ACoreSpec, TestTimer}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.order.KeyOrder
import swaydb.slice.{Slice, SliceOption, SliceReader}
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._
import swaydb.utils.ByteSizeOf

class LevelZeroLogEntrySpec extends AnyWordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default

  implicit def testTimer: TestTimer = TestTimer.random

  "LogEntryWriterLevel0 & LogEntryReaderLevel0" should {

    "write Put key value" in {
      runThis(100.times) {
        val put = Memory.put(1, randomStringOption, randomDeadlineOption())

        import LevelZeroLogEntryWriter.Level0PutWriter
        val addEntry = LogEntry.Put[Slice[Byte], Memory.Put](1, put)

        val slice = Slice.allocate[Byte](addEntry.entryBytesSize)
        addEntry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        import LevelZeroLogEntryReader.Level0PutReader
        LogEntryReader.read[LogEntry.Put[Slice[Byte], Memory.Put]](SliceReader(slice.drop(ByteSizeOf.byte))).runRandomIO.right.value shouldBe addEntry

        import LevelZeroLogEntryReader.Level0Reader
        val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.right.value
        readEntry shouldBe addEntry

        val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        readEntry applyBatch skipList
        val scalaSkipList = skipList.toIterable

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (1: Slice[Byte])
        headValue shouldBe put
      }
    }

    "write remove key-value" in {
      runThis(100.times) {
        val remove = Memory.remove(1, randomDeadlineOption())

        import LevelZeroLogEntryWriter.Level0RemoveWriter
        val entry = LogEntry.Put[Slice[Byte], Memory.Remove](1, remove)

        val slice = Slice.allocate[Byte](entry.entryBytesSize)
        entry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        import LevelZeroLogEntryReader.Level0RemoveReader
        LogEntryReader.read[LogEntry.Put[Slice[Byte], Memory.Remove]](SliceReader(slice.drop(ByteSizeOf.byte))).runRandomIO.right.value shouldBe entry

        import LevelZeroLogEntryReader.Level0Reader
        val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.right.value
        readEntry shouldBe entry

        val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        readEntry applyBatch skipList
        val scalaSkipList = skipList.toIterable

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (1: Slice[Byte])
        headValue shouldBe remove
      }
    }

    "write Update key value" in {
      runThis(100.times) {
        val update = Memory.update(1, randomStringOption, randomDeadlineOption())

        import LevelZeroLogEntryWriter.Level0UpdateWriter
        val addEntry = LogEntry.Put[Slice[Byte], Memory.Update](1, update)

        val slice = Slice.allocate[Byte](addEntry.entryBytesSize)
        addEntry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        import LevelZeroLogEntryReader.Level0UpdateReader
        LogEntryReader.read[LogEntry.Put[Slice[Byte], Memory.Update]](SliceReader(slice.drop(ByteSizeOf.byte))).runRandomIO.right.value shouldBe addEntry

        import LevelZeroLogEntryReader.Level0Reader
        val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.right.value
        readEntry shouldBe addEntry

        val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        readEntry applyBatch skipList
        val scalaSkipList = skipList.toIterable

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (1: Slice[Byte])
        headValue shouldBe update
      }
    }

    "write Function key value" in {
      runThis(100.times) {
        val function = randomFunctionKeyValue(1)

        import LevelZeroLogEntryWriter.Level0FunctionWriter
        val addEntry = LogEntry.Put[Slice[Byte], Memory.Function](1, function)

        val slice = Slice.allocate[Byte](addEntry.entryBytesSize)
        addEntry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        import LevelZeroLogEntryReader.Level0FunctionReader
        LogEntryReader.read[LogEntry.Put[Slice[Byte], Memory.Function]](SliceReader(slice.drop(ByteSizeOf.byte))).runRandomIO.right.value shouldBe addEntry

        import LevelZeroLogEntryReader.Level0Reader
        val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.right.value
        readEntry shouldBe addEntry

        val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        readEntry applyBatch skipList
        val scalaSkipList = skipList.toIterable

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (1: Slice[Byte])
        headValue shouldBe function
      }
    }

    "write range key-value" in {
      import LevelZeroLogEntryWriter.Level0RangeWriter

      runThis(100.times) {
        val inputRange = randomRangeKeyValue(0, 1)

        val entry = LogEntry.Put[Slice[Byte], Memory.Range](0, inputRange)

        val slice = Slice.allocate[Byte](entry.entryBytesSize)
        entry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        import LevelZeroLogEntryReader.Level0RangeReader
        LogEntryReader.read[LogEntry.Put[Slice[Byte], Memory.Range]](SliceReader(slice.drop(ByteSizeOf.byte))).runRandomIO.right.value shouldBe entry

        import LevelZeroLogEntryReader.Level0Reader
        val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.right.value
        readEntry shouldBe entry

        val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        readEntry applyBatch skipList
        val scalaSkipList = skipList.toIterable

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (0: Slice[Byte])
        headValue shouldBe inputRange
      }
    }

    "write PendingApply key value" in {
      runThis(100.times) {
        val pendingApply = randomPendingApplyKeyValue(1)

        import LevelZeroLogEntryWriter.Level0PendingApplyWriter
        val addEntry = LogEntry.Put[Slice[Byte], Memory.PendingApply](1, pendingApply)

        val slice = Slice.allocate[Byte](addEntry.entryBytesSize)
        addEntry writeTo slice
        slice.isFull shouldBe true //this ensures that bytesRequiredFor is returning the correct size

        import LevelZeroLogEntryReader.Level0PendingApplyReader
        LogEntryReader.read[LogEntry.Put[Slice[Byte], Memory.PendingApply]](SliceReader(slice.drop(ByteSizeOf.byte))).runRandomIO.right.value shouldBe addEntry

        import LevelZeroLogEntryReader.Level0Reader
        val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.right.value
        readEntry shouldBe addEntry

        val skipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        readEntry applyBatch skipList
        val scalaSkipList = skipList.toIterable

        scalaSkipList should have size 1
        val (headKey, headValue) = scalaSkipList.head
        headKey shouldBe (1: Slice[Byte])
        headValue shouldBe pendingApply
      }
    }

    "write, remove & update key-value" in {
      runThis(100.times) {
        import LevelZeroLogEntryWriter.{Level0PutWriter, Level0RangeWriter, Level0RemoveWriter, Level0UpdateWriter}

        val put1 = Memory.put(1, randomStringOption, randomDeadlineOption())
        val put2 = Memory.put(2, randomStringOption, randomDeadlineOption())
        val put3 = Memory.put(3, randomStringOption, randomDeadlineOption())
        val put4 = Memory.put(4, randomStringOption, randomDeadlineOption())
        val put5 = Memory.put(5, randomStringOption, randomDeadlineOption())

        val remove1 = Memory.remove(1, randomDeadlineOption())
        val remove2 = Memory.remove(2, randomDeadlineOption())

        val update1 = Memory.update(3, randomStringOption, randomDeadlineOption())

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

        import LevelZeroLogEntryReader.Level0Reader
        val readEntry = LogEntryReader.read[LogEntry[Slice[Byte], Memory]](SliceReader(slice)).runRandomIO.right.value
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
        import LevelZeroLogEntryWriter.Level0LogEntryPutWriter
        val bytes = LogEntrySerialiser.write[Slice[Byte], Memory](skipList.iterator)
        val recoveryResult = LogEntrySerialiser.read[Slice[Byte], Memory](bytes, false).runRandomIO.right.value
        recoveryResult.result shouldBe IO.unit

        val readEntries = recoveryResult.item.value
        //clear and apply new skipList and the result should be the same as previous.
        skipList.clear()
        readEntries applyBatch skipList
        assertSkipList()
      }
    }
  }
}
