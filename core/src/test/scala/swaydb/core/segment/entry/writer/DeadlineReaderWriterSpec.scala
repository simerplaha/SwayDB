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
import swaydb.core.TestData._
import swaydb.core.segment.data.Memory
import swaydb.core.segment.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.core.util.Times._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{FiniteDuration, _}

class DeadlineReaderWriterSpec extends AnyWordSpec with Matchers {

  val deadlineIds: Seq[BaseEntryId.DeadlineId] =
    allBaseEntryIds collect {
      case entryId: BaseEntryId.DeadlineId =>
        entryId
    }

  "apply no compression" when {

    def assertNoCompression(keyValue: Memory,
                            builder: EntryWriter.Builder,
                            deadlineId: BaseEntryId.DeadlineId) = {

      implicit val binder: MemoryToKeyValueIdBinder[Memory] = MemoryToKeyValueIdBinder.getBinder(keyValue)

      //ignore key
      implicit val keyWriter: KeyWriter =
        new KeyWriter {
          override def write[T <: Memory](current: T,
                                          builder: EntryWriter.Builder,
                                          deadlineId: BaseEntryId.Deadline)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit =
            ()
        }

      DeadlineWriter.write(
        current = keyValue,
        builder = builder,
        deadlineId = deadlineId
      )

      if (keyValue.deadline.isDefined)
        builder.bytes should not be empty
      else
        builder.bytes shouldBe empty

      builder.isCurrentPrefixCompressed shouldBe false
      builder.segmentHasPrefixCompression shouldBe false

      val reader = builder.bytes.createReader()

      if (keyValue.deadline.isDefined)
        keyValue.deadline.get shouldBe reader.readUnsignedLong().toDeadlineOption.get
      else
        reader.get() shouldBe 0 //empty tail bytes

      reader.get() shouldBe 0
    }

    "previous key-value is None" in {
      runThis(100.times) {
        val keyValue = randomizedKeyValues().head

        deadlineIds foreach {
          deadlineId =>
            val builder = randomBuilder()
            builder.enablePrefixCompressionForCurrentWrite = true

            assertNoCompression(
              keyValue = keyValue,
              builder = builder,
              deadlineId = deadlineId
            )
        }
      }
    }

    "previous key-value is defined by compression is disabled" in {
      runThis(100.times) {
        //test on a key-value
        val previous = randomPutKeyValue(key = 1, deadline = Some(randomDeadline()))
        val next = randomPutKeyValue(key = 2, deadline = Some(randomDeadline()))

        deadlineIds foreach {
          deadlineId =>
            val builder =
              eitherOne(
                randomBuilder(enablePrefixCompressionForCurrentWrite = false),
                randomBuilder(enablePrefixCompressionForCurrentWrite = true, prefixCompressKeysOnly = true)
              )
            builder.previous = previous

            assertNoCompression(
              keyValue = next,
              builder = builder,
              deadlineId = deadlineId
            )
        }
      }
    }

    "previous key-value is defined and compression is enabled but there are no common bytes" in {
      runThis(100.times) {
        //test on a key-value
        val deadline1 = new FiniteDuration(Int.MinValue, TimeUnit.MILLISECONDS).fromNow
        val deadline2 = new FiniteDuration(Int.MaxValue, TimeUnit.MILLISECONDS).fromNow

        val previous = randomFixedKeyValue(1, deadline = Some(deadline1))
        val next = randomFixedKeyValue(2, deadline = Some(deadline2))

        deadlineIds foreach {
          deadlineId =>
            val builder = randomBuilder(enablePrefixCompressionForCurrentWrite = true)
            builder.enablePrefixCompressionForCurrentWrite = true
            builder.previous = previous

            assertNoCompression(
              keyValue = next,
              builder = builder,
              deadlineId = deadlineId
            )
        }
      }
    }
  }

  "apply compression" when {

    def assertCompression(keyValue: Memory,
                          builder: EntryWriter.Builder,
                          deadlineId: BaseEntryId.DeadlineId) = {

      implicit val binder: MemoryToKeyValueIdBinder[Memory] = MemoryToKeyValueIdBinder.getBinder(keyValue)

      //ignore key
      implicit val keyWriter: KeyWriter =
        new KeyWriter {
          override def write[T <: Memory](current: T,
                                          builder: EntryWriter.Builder,
                                          deadlineId: BaseEntryId.Deadline)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit =
            ()
        }

      DeadlineWriter.write(
        current = keyValue,
        builder = builder,
        deadlineId = deadlineId
      )

      builder.bytes should not be empty
      builder.isCurrentPrefixCompressed shouldBe !builder.prefixCompressKeysOnly
      builder.segmentHasPrefixCompression shouldBe !builder.prefixCompressKeysOnly

      val deadlineBytes = keyValue.deadline.get.toUnsignedBytes
      //bytes written are smaller than deadline bytes
      if (builder.prefixCompressKeysOnly)
        builder.bytes shouldBe deadlineBytes
      else
        builder.bytes.size should be < deadlineBytes.size
    }

    "deadlines can be compressed" in {
      runThis(100.times) {
        //test on a key-value
        val deadline1 = 10.seconds.fromNow
        val deadline2 = 11.seconds.fromNow

        val previous = randomPutKeyValue(key = 1, deadline = Some(deadline1))
        val next = randomPutKeyValue(key = 2, deadline = Some(deadline2))

        deadlineIds foreach {
          deadlineId =>
            //also test when prefixCompressKeysOnly is set.
            val builder = randomBuilder(enablePrefixCompressionForCurrentWrite = true, prefixCompressKeysOnly = randomBoolean())
            builder.previous = previous

            assertCompression(
              keyValue = next,
              builder = builder,
              deadlineId = deadlineId
            )
        }
      }
    }
  }
}
