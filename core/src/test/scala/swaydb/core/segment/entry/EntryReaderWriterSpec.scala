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

package swaydb.core.segment.entry

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.{Memory, Persistent}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.entry.id.MemoryToKeyValueIdBinder
import swaydb.core.segment.entry.reader.PersistentParser
import swaydb.core.segment.entry.writer._
import swaydb.data.RunThis._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class EntryReaderWriterSpec extends AnyWordSpec with Matchers {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeWriter: TimeWriter = TimeWriter
  implicit val valueWriter: ValueWriter = ValueWriter
  implicit val deadlineWriter: DeadlineWriter = DeadlineWriter
  implicit val keyWriter: KeyWriter = KeyWriter

  "write and read single Fixed entry" in {
    runThis(1000.times) {
      implicit val testTimer = TestTimer.random
      val keyValue = randomizedKeyValues(1).head

      def assertParse[T <: Memory](keyValue: T)(implicit binder: MemoryToKeyValueIdBinder[T]) = {
        val builder = randomBuilder()

        EntryWriter.write(
          current = keyValue,
          builder = builder
        )

        val bytes = Reader(builder.bytes.close())

        val parsedKeyValue =
          PersistentParser.parse(
            headerInteger = bytes.readUnsignedInt(),
            indexOffset = 0,
            tailBytes = bytes.readRemaining(),
            previous = Persistent.Null,
            mightBeCompressed = builder.isCurrentPrefixCompressed,
            keyCompressionOnly = builder.prefixCompressKeysOnly,
            sortedIndexEndOffset = bytes.slice.toOffset,
            normalisedByteSize = 0,
            hasAccessPositionIndex = builder.enableAccessPositionIndex,
            optimisedForReverseIteration = builder.optimiseForReverseIteration,
            valuesReaderOrNull = keyValue.value.mapC(buildSingleValueReader).orNull
          )

        parsedKeyValue shouldBe keyValue

        parsedKeyValue.nextIndexOffset shouldBe -1
        parsedKeyValue.nextKeySize shouldBe 0
      }

      keyValue match {
        case keyValue: Memory.Put =>
          assertParse(keyValue)

        case keyValue: Memory.Update =>
          assertParse(keyValue)

        case keyValue: Memory.Remove =>
          assertParse(keyValue)

        case keyValue: Memory.Function =>
          assertParse(keyValue)

        case keyValue: Memory.PendingApply =>
          assertParse(keyValue)

        case keyValue: Memory.Range =>
          assertParse(keyValue)
      }
    }
  }

  "write and read two fixed entries" in {
    runThis(1000.times) {
      implicit val testTimer = TestTimer.random

      val previous = randomizedKeyValues(count = 1).head
      val duplicateValues = if (Random.nextBoolean()) previous.value else randomStringSliceOptional
      val duplicateDeadline = if (Random.nextBoolean()) previous.deadline else randomDeadlineOption
      val next =
        eitherOne(
          randomFixedKeyValue(
            key = randomIntMax() + 1,
            value = duplicateValues,
            deadline = duplicateDeadline
          ),
          randomRangeKeyValue(
            from = previous.key.readInt() + 1,
            to = previous.key.readInt() + 100,
            fromValue = randomFromValueOption(deadline = duplicateDeadline),
            rangeValue = randomRangeValue(value = duplicateValues, deadline = duplicateDeadline)
          )
        )

      //      println
      //      println("write previous: " + previous)
      //      println("write next: " + next)

      def assertParse[P <: Memory, N <: Memory](previous: P, next: N)(implicit previousBinder: MemoryToKeyValueIdBinder[P],
                                                                      nextBinder: MemoryToKeyValueIdBinder[N]) = {
        val builder = randomBuilder()

        EntryWriter.write(
          current = previous,
          builder = builder
        )

        builder.previous = previous

        EntryWriter.write(
          current = next,
          builder = builder
        )

        val sortedIndexReader = Reader(builder.bytes.close())
        val valueBytes: Slice[Byte] = previous.value.toOptionC ++ next.value.toOptionC
        val valuesReaderOrNull = if (valueBytes.isEmpty) null else buildSingleValueReader(valueBytes)

        val previousParsedKeyValue =
          PersistentParser.parse(
            headerInteger = sortedIndexReader.readUnsignedInt(),
            indexOffset = 0,
            tailBytes = sortedIndexReader.readRemaining(),
            previous = Persistent.Null,
            mightBeCompressed = builder.isCurrentPrefixCompressed,
            keyCompressionOnly = builder.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndexReader.slice.toOffset,
            normalisedByteSize = 0,
            hasAccessPositionIndex = builder.enableAccessPositionIndex,
            optimisedForReverseIteration = builder.optimiseForReverseIteration,
            valuesReaderOrNull = valuesReaderOrNull
          )

        previousParsedKeyValue shouldBe previous

        previousParsedKeyValue.nextIndexOffset should be > previous.mergedKey.size

        sortedIndexReader moveTo previousParsedKeyValue.nextIndexOffset

        val nextParsedKeyValue =
          PersistentParser.parse(
            headerInteger = sortedIndexReader.readUnsignedInt(),
            indexOffset = previousParsedKeyValue.nextIndexOffset,
            tailBytes = sortedIndexReader.readRemaining(),
            previous = previousParsedKeyValue,
            mightBeCompressed = builder.isCurrentPrefixCompressed,
            keyCompressionOnly = builder.prefixCompressKeysOnly,
            sortedIndexEndOffset = sortedIndexReader.slice.toOffset,
            normalisedByteSize = 0,
            hasAccessPositionIndex = builder.enableAccessPositionIndex,
            optimisedForReverseIteration = builder.optimiseForReverseIteration,
            valuesReaderOrNull = valuesReaderOrNull
          )

        nextParsedKeyValue shouldBe next

        nextParsedKeyValue.nextIndexOffset shouldBe -1
        nextParsedKeyValue.nextKeySize shouldBe 0
      }

      previous match {
        case previous: Memory.Put =>
          next match {
            case next: Memory.Put => assertParse(previous, next)
            case next: Memory.Update => assertParse(previous, next)
            case next: Memory.Remove => assertParse(previous, next)
            case next: Memory.Function => assertParse(previous, next)
            case next: Memory.PendingApply => assertParse(previous, next)
            case next: Memory.Range => assertParse(previous, next)
          }

        case previous: Memory.Update =>
          next match {
            case next: Memory.Put => assertParse(previous, next)
            case next: Memory.Update => assertParse(previous, next)
            case next: Memory.Remove => assertParse(previous, next)
            case next: Memory.Function => assertParse(previous, next)
            case next: Memory.PendingApply => assertParse(previous, next)
            case next: Memory.Range => assertParse(previous, next)
          }

        case previous: Memory.Remove =>
          next match {
            case next: Memory.Put => assertParse(previous, next)
            case next: Memory.Update => assertParse(previous, next)
            case next: Memory.Remove => assertParse(previous, next)
            case next: Memory.Function => assertParse(previous, next)
            case next: Memory.PendingApply => assertParse(previous, next)
            case next: Memory.Range => assertParse(previous, next)
          }

        case previous: Memory.Function =>
          next match {
            case next: Memory.Put => assertParse(previous, next)
            case next: Memory.Update => assertParse(previous, next)
            case next: Memory.Remove => assertParse(previous, next)
            case next: Memory.Function => assertParse(previous, next)
            case next: Memory.PendingApply => assertParse(previous, next)
            case next: Memory.Range => assertParse(previous, next)
          }

        case previous: Memory.PendingApply =>
          next match {
            case next: Memory.Put => assertParse(previous, next)
            case next: Memory.Update => assertParse(previous, next)
            case next: Memory.Remove => assertParse(previous, next)
            case next: Memory.Function => assertParse(previous, next)
            case next: Memory.PendingApply => assertParse(previous, next)
            case next: Memory.Range => assertParse(previous, next)
          }

        case previous: Memory.Range =>
          next match {
            case next: Memory.Put => assertParse(previous, next)
            case next: Memory.Update => assertParse(previous, next)
            case next: Memory.Remove => assertParse(previous, next)
            case next: Memory.Function => assertParse(previous, next)
            case next: Memory.PendingApply => assertParse(previous, next)
            case next: Memory.Range => assertParse(previous, next)
          }
      }
    }
  }
}
