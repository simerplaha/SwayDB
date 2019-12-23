///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.format.a.entry
//
//import org.scalatest.{Matchers, WordSpec}
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.core.data.Memory
//import swaydb.core.io.reader.Reader
//import swaydb.core.segment.format.a.entry.id.MemoryToKeyValueIdBinder
//import swaydb.core.segment.format.a.entry.reader.EntryReader
//import swaydb.core.segment.format.a.entry.writer._
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.util.Random
//
//class EntryReaderWriterSpec extends WordSpec with Matchers {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val timeWriter: TimeWriter = TimeWriter
//  implicit val valueWriter: ValueWriter = ValueWriter
//  implicit val deadlineWriter: DeadlineWriter = DeadlineWriter
//  implicit val keyWriter: KeyWriter = KeyWriter
//
//  "write and read single Fixed entry" in {
//    runThis(1000.times) {
//      implicit val testTimer = TestTimer.random
//      val keyValue = randomizedKeyValues(1).head
//
//      def assertParse[T <: Memory](keyValue: T)(implicit binder: MemoryToKeyValueIdBinder[T]) = {
//        val builder = randomBuilder()
//
//        EntryWriter.write(
//          current = keyValue,
//          builder = builder
//        )
//
//        val bytes = Reader(builder.bytes.close())
//
//        val parsedKeyValue =
//          EntryReader.parse(
//            headerInteger = bytes.readUnsignedInt(),
//            indexEntry = bytes.readRemaining(),
//            mightBeCompressed = builder.isCurrentPrefixCompressed,
//            keyCompressionOnly = builder.prefixCompressKeysOnly,
//            sortedIndexEndOffset = bytes.slice.toOffset,
//            valuesReader = keyValue.value.map(buildSingleValueReader),
//            indexOffset = 0,
//            hasAccessPositionIndex = builder.enableAccessPositionIndex,
//            normalisedByteSize = 0,
//            previous = None
//          )
//
//        parsedKeyValue shouldBe keyValue
//
//        parsedKeyValue.nextIndexOffset shouldBe -1
//        parsedKeyValue.nextKeySize shouldBe 0
//      }
//
//      keyValue match {
//        case keyValue: Memory.Put =>
//          assertParse(keyValue)
//
//        case keyValue: Memory.Update =>
//          assertParse(keyValue)
//
//        case keyValue: Memory.Remove =>
//          assertParse(keyValue)
//
//        case keyValue: Memory.Function =>
//          assertParse(keyValue)
//
//        case keyValue: Memory.PendingApply =>
//          assertParse(keyValue)
//
//        case keyValue: Memory.Range =>
//          assertParse(keyValue)
//      }
//    }
//  }
//
//  "write and read two fixed entries" in {
//    runThis(1000.times) {
//      implicit val testTimer = TestTimer.random
//
//      val previous = randomizedKeyValues(count = 1).head
//      val duplicateValues = if (Random.nextBoolean()) previous.value else randomStringOption
//      val duplicateDeadline = if (Random.nextBoolean()) previous.deadline else randomDeadlineOption
//      val next =
//        eitherOne(
//          randomFixedKeyValue(
//            key = randomIntMax() + 1,
//            value = duplicateValues,
//            deadline = duplicateDeadline
//          ),
//          randomRangeKeyValue(
//            from = previous.key.readInt() + 1,
//            to = previous.key.readInt() + 100,
//            fromValue = randomFromValueOption(deadline = duplicateDeadline),
//            rangeValue = randomRangeValue(value = duplicateValues, deadline = duplicateDeadline)
//          )
//        )
//
//      //      println
//      //      println("write previous: " + previous)
//      //      println("write next: " + next)
//
//      def assertParse[P <: Memory, N <: Memory](previous: P, next: N)(implicit previousBinder: MemoryToKeyValueIdBinder[P],
//                                                                      nextBinder: MemoryToKeyValueIdBinder[N]) = {
//        val builder = randomBuilder()
//
//        EntryWriter.write(
//          current = previous,
//          builder = builder
//        )
//
//        builder.previous = Some(previous)
//
//        EntryWriter.write(
//          current = next,
//          builder = builder
//        )
//
//        val sortedIndexReader = Reader(builder.bytes.close())
//        val valueBytes: Slice[Byte] = previous.value ++ next.value
//        val valuesReader = if (valueBytes.isEmpty) None else Some(buildSingleValueReader(valueBytes))
//
//        val previousParsedKeyValue =
//          EntryReader.parse(
//            headerInteger = sortedIndexReader.readUnsignedInt(),
//            indexEntry = sortedIndexReader.readRemaining(),
//            mightBeCompressed = false,
//            keyCompressionOnly = builder.prefixCompressKeysOnly,
//            sortedIndexEndOffset = sortedIndexReader.slice.toOffset,
//            valuesReaderNullable = valuesReaderNullable,
//            indexOffset = 0,
//            hasAccessPositionIndex = builder.enableAccessPositionIndex,
//            normalisedByteSize = 0,
//            previous = None
//          )
//
//        previousParsedKeyValue shouldBe previous
//
//        previousParsedKeyValue.nextIndexOffset should be > previous.mergedKey.size
//
//        sortedIndexReader moveTo previousParsedKeyValue.nextIndexOffset
//
//        val nextParsedKeyValue =
//          EntryReader.parse(
//            headerInteger = sortedIndexReader.readUnsignedInt(),
//            indexEntry = sortedIndexReader.readRemaining(),
//            mightBeCompressed = builder.isCurrentPrefixCompressed,
//            keyCompressionOnly = builder.prefixCompressKeysOnly,
//            sortedIndexEndOffset = sortedIndexReader.slice.toOffset,
//            valuesReaderNullable = valuesReaderNullable,
//            indexOffset = previousParsedKeyValue.nextIndexOffset,
//            hasAccessPositionIndex = builder.enableAccessPositionIndex,
//            normalisedByteSize = 0,
//            previous = Some(previousParsedKeyValue)
//          )
//
//        nextParsedKeyValue shouldBe next
//
//        nextParsedKeyValue.nextIndexOffset shouldBe -1
//        nextParsedKeyValue.nextKeySize shouldBe 0
//      }
//
//      previous match {
//        case previous: Memory.Put =>
//          next match {
//            case next: Memory.Put => assertParse(previous, next)
//            case next: Memory.Update => assertParse(previous, next)
//            case next: Memory.Remove => assertParse(previous, next)
//            case next: Memory.Function => assertParse(previous, next)
//            case next: Memory.PendingApply => assertParse(previous, next)
//            case next: Memory.Range => assertParse(previous, next)
//          }
//
//        case previous: Memory.Update =>
//          next match {
//            case next: Memory.Put => assertParse(previous, next)
//            case next: Memory.Update => assertParse(previous, next)
//            case next: Memory.Remove => assertParse(previous, next)
//            case next: Memory.Function => assertParse(previous, next)
//            case next: Memory.PendingApply => assertParse(previous, next)
//            case next: Memory.Range => assertParse(previous, next)
//          }
//
//        case previous: Memory.Remove =>
//          next match {
//            case next: Memory.Put => assertParse(previous, next)
//            case next: Memory.Update => assertParse(previous, next)
//            case next: Memory.Remove => assertParse(previous, next)
//            case next: Memory.Function => assertParse(previous, next)
//            case next: Memory.PendingApply => assertParse(previous, next)
//            case next: Memory.Range => assertParse(previous, next)
//          }
//
//        case previous: Memory.Function =>
//          next match {
//            case next: Memory.Put => assertParse(previous, next)
//            case next: Memory.Update => assertParse(previous, next)
//            case next: Memory.Remove => assertParse(previous, next)
//            case next: Memory.Function => assertParse(previous, next)
//            case next: Memory.PendingApply => assertParse(previous, next)
//            case next: Memory.Range => assertParse(previous, next)
//          }
//
//        case previous: Memory.PendingApply =>
//          next match {
//            case next: Memory.Put => assertParse(previous, next)
//            case next: Memory.Update => assertParse(previous, next)
//            case next: Memory.Remove => assertParse(previous, next)
//            case next: Memory.Function => assertParse(previous, next)
//            case next: Memory.PendingApply => assertParse(previous, next)
//            case next: Memory.Range => assertParse(previous, next)
//          }
//
//        case previous: Memory.Range =>
//          next match {
//            case next: Memory.Put => assertParse(previous, next)
//            case next: Memory.Update => assertParse(previous, next)
//            case next: Memory.Remove => assertParse(previous, next)
//            case next: Memory.Function => assertParse(previous, next)
//            case next: Memory.PendingApply => assertParse(previous, next)
//            case next: Memory.Range => assertParse(previous, next)
//          }
//      }
//    }
//  }
//}
