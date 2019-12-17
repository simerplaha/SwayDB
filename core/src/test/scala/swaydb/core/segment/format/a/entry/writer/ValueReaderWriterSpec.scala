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

package swaydb.core.segment.format.a.entry.writer

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class ValueReaderWriterSpec extends WordSpec with Matchers {

  val noCompressedDeadlineIds: Seq[BaseEntryId.Time] =
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
      override private[a] def write[T <: Memory](current: T,
                                                 builder: EntryWriter.Builder,
                                                 deadlineId: BaseEntryId.DeadlineId)(implicit binder: MemoryToKeyValueIdBinder[T], keyWriter: KeyWriter): Unit =
        ()
    }

  "apply no compression" when {

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
        case Some(value) =>
          reader.readUnsignedInt() shouldBe 0 //valueOffset
          reader.readUnsignedInt() shouldBe value.size //valueLength
          reader.get() shouldBe 0 //tail empty bytes
        case None =>
          builder.bytes shouldBe empty
      }
    }

    "previous key-value is None" in {
      runThis(100.times) {
        val keyValue = randomizedKeyValues(1).head

        noCompressedDeadlineIds foreach {
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

    "previous key-value is defined and can be compressed but compression itself is disabled" in {
      runThis(100.times) {
        //test on a key-value
        val value1: Slice[Byte] = Slice.writeInt(0)
        val value2: Slice[Byte] = Slice.writeInt(1)
        val previous = randomPutKeyValue(key = 1, value = Some(value1))
        val next = randomPutKeyValue(key = 2, value = Some(value2))

        noCompressedDeadlineIds foreach {
          deadlineId =>
            val builder =
              eitherOne(
                randomBuilder(enablePrefixCompression = false),
                randomBuilder(enablePrefixCompression = true, prefixCompressKeysOnly = true)
              )

            builder.previous = Some(previous)

            assertNoCompression(
              keyValue = next,
              builder = builder,
              time = deadlineId
            )
        }
      }
    }
  }
}
