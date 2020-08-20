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

package swaydb.core.segment.format.a.entry.writer

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.core.segment.format.a.entry.reader.{EntryReaderFailure, KeyReader}
import swaydb.data.slice.Slice

class KeyWriterReaderSpec extends AnyWordSpec with Matchers {

  val noCompressedDeadlineIds: Seq[BaseEntryId.Deadline] =
    allBaseEntryIds collect {
      case entryId: BaseEntryId.Deadline.NoDeadline =>
        entryId

      case entryId: BaseEntryId.Deadline.Uncompressed =>
        entryId
    }

  "apply no compression" when {

    def assertNoCompression(keyValue: Memory,
                            builder: EntryWriter.Builder,
                            deadlineId: BaseEntryId.Deadline) = {

      implicit val binder: MemoryToKeyValueIdBinder[Memory] = MemoryToKeyValueIdBinder.getBinder(keyValue)

      KeyWriter.write(
        current = keyValue,
        builder = builder,
        deadlineId = deadlineId
      )

      builder.bytes should not be empty
      builder.isCurrentPrefixCompressed shouldBe false
      builder.segmentHasPrefixCompression shouldBe false

      val reader = builder.bytes.createReader()
      val keyBytes = reader.read(reader.readUnsignedInt())
      val keyValueId = reader.readUnsignedInt()

      binder.keyValueId.isKeyValueId_CompressedKey(keyValueId) shouldBe false

      val expectedId =
        binder.keyValueId.adjustBaseIdToKeyValueIdKey(
          baseId = deadlineId.baseId,
          isKeyCompressed = false
        )

      keyValueId shouldBe expectedId

      val key =
        KeyReader.read(
          keyValueIdInt = keyValueId,
          keyBytes = keyBytes,
          previousKey = Slice.Null,
          keyValueId = binder.keyValueId
        )

      key shouldBe keyValue.mergedKey

      if (builder.enableAccessPositionIndex)
        reader.readUnsignedInt() shouldBe 1
      else
        reader.get() shouldBe 0 //empty tail bytes
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
              deadlineId = deadlineId
            )
        }
      }
    }

    "previous key-value is defined by compression is disabled" in {
      runThis(100.times) {
        //test on a key-value
        val previous = randomizedKeyValues(1, startId = Some(1)).head
        val next = randomizedKeyValues(1, startId = Some(2)).head

        noCompressedDeadlineIds foreach {
          deadlineId =>
            val builder = randomBuilder(enablePrefixCompressionForCurrentWrite = false)
            builder.previous = previous

            assertNoCompression(
              keyValue = next,
              builder = builder,
              deadlineId = deadlineId
            )
        }
      }
    }

    "previous key-value is defined by compression is enabled but there are no common bytes" in {
      runThis(100.times) {
        //test on a key-value
        val previous = randomizedKeyValues(1, startId = Some(1)).head
        val next = randomizedKeyValues(1, startId = Some(Int.MaxValue - 100)).head

        noCompressedDeadlineIds foreach {
          deadlineId =>
            val builder = randomBuilder(enablePrefixCompressionForCurrentWrite = true)
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

    def assertCompression(previousKey: Slice[Byte],
                          next: Memory,
                          builder: EntryWriter.Builder,
                          deadlineId: BaseEntryId.Deadline) = {

      implicit val binder: MemoryToKeyValueIdBinder[Memory] = MemoryToKeyValueIdBinder.getBinder(next)

      KeyWriter.write(
        current = next,
        builder = builder,
        deadlineId = deadlineId
      )

      builder.bytes should not be empty
      builder.isCurrentPrefixCompressed shouldBe true
      builder.segmentHasPrefixCompression shouldBe true

      val reader = builder.bytes.createReader()
      val keyBytes = reader.read(reader.readUnsignedInt())
      val keyValueId = reader.readUnsignedInt()

      val expectedId =
        binder.keyValueId.adjustBaseIdToKeyValueIdKey(
          baseId = deadlineId.baseId,
          isKeyCompressed = true
        )

      binder.keyValueId.isKeyValueId_CompressedKey(keyValueId) shouldBe true

      keyValueId shouldBe expectedId

      //if previous is required and not supplied it will throw exception.
      assertThrows[EntryReaderFailure.NoPreviousKeyValue.type] {
        KeyReader.read(
          keyValueIdInt = keyValueId,
          keyBytes = keyBytes,
          previousKey = Slice.Null,
          keyValueId = binder.keyValueId
        )
      }

      val key =
        KeyReader.read(
          keyValueIdInt = keyValueId,
          keyBytes = keyBytes,
          previousKey = previousKey,
          keyValueId = binder.keyValueId
        )

      key shouldBe next.mergedKey

      if (builder.enableAccessPositionIndex)
        reader.readUnsignedInt() shouldBe Int.MaxValue
      else
        reader.get() shouldBe 0 //empty tail bytes
    }

    "previous key-value is defined and compression is enabled" in {
      runThis(100.times) {
        //test on a key-value
        val previous = randomizedKeyValues(1, startId = Some(1)).head
        val next = randomizedKeyValues(1, startId = Some(2)).head

        noCompressedDeadlineIds foreach {
          deadlineId =>
            val builder = randomBuilder(enablePrefixCompressionForCurrentWrite = true)
            builder.previous = previous
            //if accessPositionIndex is enabled then next key-value accessPosition will be same since it's prefix compressed.
            if (builder.enableAccessPositionIndex)
              builder.accessPositionIndex = Int.MaxValue

            assertCompression(
              previousKey = previous.key,
              next = next,
              builder = builder,
              deadlineId = deadlineId
            )
        }
      }
    }
  }
}
