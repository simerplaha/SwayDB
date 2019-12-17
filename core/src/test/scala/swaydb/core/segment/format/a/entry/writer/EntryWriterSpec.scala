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
//package swaydb.core.segment.format.a.entry.writer
//
//import org.scalatest.{Matchers, WordSpec}
//import swaydb.core.data.Memory
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.TestTimer
//import swaydb.serializers._
//import swaydb.serializers.Default._
//import swaydb.core.RunThis._
//import swaydb.core.io.reader.Reader
//import swaydb.core.segment.format.a.entry.id.MemoryToKeyValueIdBinder
//import swaydb.core.segment.format.a.entry.reader.EntryReader
//import swaydb.data.slice.Slice
//
//class EntryWriterSpec extends WordSpec with Matchers {
//
//  implicit def timer = TestTimer.Empty
//
//  "write fixed" when {
//
//    "no value, deadline or time"
//
//
//    "no value & no-deadline" in {
//      runThis(100.times) {
//        val keyValues: Seq[Memory.Fixed] =
//          (1 to 100) map {
//            _ =>
//              randomFixedKeyValue(
//                key = randomIntMax(),
//                value = None,
//                deadline = None
//              )
//          } collect {
//            case keyValue: Memory.Put => keyValue
//            case keyValue: Memory.Remove => keyValue
//            case keyValue: Memory.Update => keyValue
//          }
//
//        keyValues.foreach(_.value shouldBe empty)
//
//        def createBuilder() =
//          EntryWriter.Builder(
//            enablePrefixCompression = false,
//            prefixCompressKeysOnly = false,
//            compressDuplicateValues = false,
//            enableAccessPositionIndex = false,
//            bytes = Slice.create[Byte](100)
//          )
//
//        def assertParse[T <: Memory](keyValue: T)(implicit binder: MemoryToKeyValueIdBinder[T]) = {
//          val builder = createBuilder()
//          EntryWriter.write(
//            current = keyValue,
//            builder = builder
//          )
//
//          val bytes = Reader(builder.bytes.close())
//          val parsedKeyValue =
//            EntryReader.parse(
//              headerInteger = bytes.readUnsignedInt(),
//              indexEntry = bytes.readRemaining(),
//              mightBeCompressed = false,
//              keyCompressionOnly = false,
//              sortedIndexEndOffset = bytes.size.toInt - 1,
//              valuesReader = None,
//              indexOffset = 0,
//              hasAccessPositionIndex = builder.enableAccessPositionIndex,
//              normalisedByteSize = 0,
//              previous = None
//            )
//
//          parsedKeyValue.nextIndexOffset shouldBe -1
//          parsedKeyValue.nextKeySize shouldBe 0
//
//          parsedKeyValue shouldBe keyValue
//        }
//
//        keyValues foreach {
//          case keyValue: Memory.Put =>
//            assertParse(keyValue)
//
//          case keyValue: Memory.Update =>
//            assertParse(keyValue)
//
//          case keyValue: Memory.Remove =>
//            assertParse(keyValue)
//
//          case _: Memory.Function | _: Memory.Function =>
//            fail("Should've been filterd out")
//        }
//
//      }
//
//    }
//
//  }
//
//}
