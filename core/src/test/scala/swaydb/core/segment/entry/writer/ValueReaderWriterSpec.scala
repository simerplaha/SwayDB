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

package swaydb.core.segment.entry.writer

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.segment.data.{Memory, Time}
import swaydb.core.segment.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class ValueReaderWriterSpec extends AnyWordSpec with Matchers {

  val timeIds: Seq[BaseEntryId.Time] =
    allBaseEntryIds collect {
      case entryId: BaseEntryId.Time =>
        entryId
    }

  //ignore key
  implicit val keyWriter: KeyWriter =
    new KeyWriter {
      override def write[T <: Memory](current: T,
                                      builder: EntryWriter.Builder,
                                      deadlineId: BaseEntryId.Deadline)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit =
        ()
    }

  implicit val valueWriter: ValueWriter =
    new ValueWriter {
      override def write[T <: Memory](current: T,
                                      entryId: BaseEntryId.Time,
                                      builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T], keyWriter: KeyWriter, deadlineWriter: DeadlineWriter): Unit =
        ()
    }

  implicit val deadlineWriter: DeadlineWriter =
    new DeadlineWriter {
      override private[segment] def write[T <: Memory](current: T,
                                                       builder: EntryWriter.Builder,
                                                       deadlineId: BaseEntryId.DeadlineId)(implicit binder: MemoryToKeyValueIdBinder[T], keyWriter: KeyWriter): Unit =
        ()
    }

  "apply no compression" when {

    timeIds should not be empty

    def assertNoCompression(keyValue: Memory,
                            builder: EntryWriter.Builder,
                            time: BaseEntryId.Time) = {

      implicit val binder: MemoryToKeyValueIdBinder[Memory] = MemoryToKeyValueIdBinder.getBinder(keyValue)

      ValueWriter.write(
        current = keyValue,
        builder = builder,
        entryId = time
      )

      builder.isCurrentPrefixCompressed shouldBe false
      builder.segmentHasPrefixCompression shouldBe false

      val reader = builder.bytes.createReader()

      keyValue.value match {
        case Slice.Null =>
          builder.bytes shouldBe empty

        case slice: Slice[_] =>
          reader.readUnsignedInt() shouldBe 0 //valueOffset
          reader.readUnsignedInt() shouldBe slice.size //valueLength
          reader.get() shouldBe 0 //tail empty bytes
      }
    }

    "previous key-value is None" in {
      runThis(100.times) {
        val keyValue = randomizedKeyValues(1).head

        timeIds foreach {
          deadlineId =>
            val builder = randomBuilder()

            assertNoCompression(
              keyValue = keyValue,
              builder = builder,
              time = deadlineId
            )
        }
      }
    }

    "previous key-value is defined with duplicate values" in {
      runThis(100.times) {
        val previous = Memory.Put(1, 1, None, Time.empty)
        val next = Memory.Put(2, 1, None, Time.empty)

        timeIds foreach {
          deadlineId =>
            val builder =
              eitherOne(
                randomBuilder(compressDuplicateValues = true, enablePrefixCompressionForCurrentWrite = false),
                randomBuilder(compressDuplicateValues = true, enablePrefixCompressionForCurrentWrite = true, prefixCompressKeysOnly = true)
              )

            ValueWriter.write(
              current = previous,
              builder = builder,
              entryId = deadlineId
            )

            builder.previous = previous

            ValueWriter.write(
              current = next,
              builder = builder,
              entryId = deadlineId
            )

            builder.isCurrentPrefixCompressed shouldBe false
            builder.segmentHasPrefixCompression shouldBe false

            val reader = builder.bytes.createReader()

            //entries for value1
            reader.readUnsignedInt() shouldBe 0 //valueOffset
            reader.readUnsignedInt() shouldBe 4 //valueLength
            //entries for value2
            reader.readUnsignedInt() shouldBe 0 //valueOffset
            reader.readUnsignedInt() shouldBe 4 //valueLength
            reader.get() shouldBe 0 //tail empty bytes
        }
      }
    }
  }

  "apply compression" when {
    "previous key-value is defined with duplicate values" in {
      runThis(100.times) {
        val previous = Memory.Put(1, 1, None, Time.empty)
        val next = Memory.Put(2, 2, None, Time.empty)

        timeIds foreach {
          deadlineId =>
            val builder = randomBuilder(compressDuplicateValues = true, enablePrefixCompressionForCurrentWrite = true, prefixCompressKeysOnly = false)

            ValueWriter.write(
              current = previous,
              builder = builder,
              entryId = deadlineId
            )

            builder.isCurrentPrefixCompressed shouldBe false
            builder.segmentHasPrefixCompression shouldBe false

            builder.previous = previous

            ValueWriter.write(
              current = next,
              builder = builder,
              entryId = deadlineId
            )

            builder.isCurrentPrefixCompressed shouldBe true
            builder.segmentHasPrefixCompression shouldBe true

            val reader = builder.bytes.createReader()

            //entries for value1
            reader.readUnsignedInt() shouldBe 0 //valueOffset
            reader.readUnsignedInt() shouldBe 4 //valueLength
            //entries for value2
            reader.readUnsignedInt() shouldBe 4 //valueOffset is 3 compressed
            //           //value length is fully compressed.
            reader.get() shouldBe 0 //tail empty bytes
        }
      }
    }
  }

}
