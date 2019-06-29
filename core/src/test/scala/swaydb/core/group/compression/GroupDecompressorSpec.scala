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
//package swaydb.core.group.compression
//
//import scala.concurrent.duration._
//import scala.util.Random
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.IOAssert._
//import swaydb.core.data._
//import swaydb.core.io.reader.Reader
//import swaydb.core.queue.KeyValueLimiter
//import swaydb.core.segment.format.a.{SegmentReader, SegmentWriter}
//import swaydb.data.IO._
//import swaydb.core.{TestBase, TestData}
//import swaydb.data.IO
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//
///**
//  * [[swaydb.core.group.compression.GroupCompressor]] is always invoked directly from [[Transient.Group]] there these test cases initialise the Group
//  * to get full code coverage.
//  *
//  */
//class GroupDecompressorSpec extends TestBase {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//
//  val keyValueCount = 10000
//
//  "GroupDecompressor" should {
//    "Concurrently read multiple key-values" in {
//      //only 100.bytes (very small) for key-values bytes so that all key-values get dropped from the cache eventually.
//      implicit val keyValueLimiter = KeyValueLimiter(1.byte, 1.second)
//      runThis(10.times) {
//        //randomly generate key-values
//        val keyValues =
//          eitherOne(
//            left = randomKeyValues(keyValueCount),
//            right = randomizedKeyValues(keyValueCount)
//          )
//
//        //create a group for the key-values
//        val group =
//          Transient.Group(
//            keyValues = keyValues,
//            indexCompressions = Seq(randomCompression()),
//            valueCompressions = Seq(randomCompression()),
//            falsePositiveRate = TestData.falsePositiveRate,
//            enableBinarySearchIndex = TestData.enableBinarySearchIndex,
//            buildFullBinarySearchIndex = TestData.buildFullBinarySearchIndex,
//            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
//            minimumNumberOfKeysForHashIndex = TestData.minimumNumberOfKeysForHashIndex,
//            allocateSpace = TestData.allocateSpace,
//            previous = None,
//            maxProbe = TestData.maxProbe
//          ).assertGet
//
//        //write the group to a Segment
//        //        val (bytes, _) =
//        //          SegmentWriter.write(
//        //            keyValues = Seq(group),
//        //            createdInLevel = 0,
//        //            maxProbe = TestData.maxProbe,
//        //            falsePositiveRate = TestData.falsePositiveRate
//        //          ).assertGet
//        //
//        //        //read footer
//        //        val readKeyValues = SortedIndex.readAll(SegmentFooter.read(Reader(bytes)).assertGet, Reader(bytes)).assertGet
//        //        readKeyValues should have size 1
//        //        val persistentGroup = readKeyValues.head.asInstanceOf[Persistent.Group]
//        //
//        //        //concurrently with 100 threads read randomly all key-values from the Group. Hammer away!
//        //        runThisParallel(100.times) {
//        //          eitherOne(
//        //            left = Random.shuffle(unzipGroups(keyValues).toList),
//        //            right = unzipGroups(keyValues)
//        //          ) mapIO {
//        //            keyValue =>
//        //              IO.Async.runSafe(persistentGroup.binarySegment.get(keyValue.key).get).safeGetBlocking match {
//        //                case IO.Failure(exception) =>
//        //                  IO.Failure(exception)
//        //
//        //                case IO.Success(value) =>
//        //                  try {
//        //                    IO.Async.runSafe(value.get.toMemory().get).safeGetBlocking.assertGet shouldBe keyValue
//        //                    IO.unit
//        //                  } catch {
//        //                    case ex: Exception =>
//        //                      IO.Failure(ex.getCause)
//        //                  }
//        //              }
//        //          } assertGet
//        //        }
//        //
//        //        println("Done reading.")
//        //        //cache should eventually be empty.
//        //        eventual(20.seconds) {
//        //          persistentGroup.binarySegment.isCacheEmpty shouldBe true
//        //        }
//        //        println("Cache is empty")
//        ???
//      }
//
//      keyValueLimiter.terminate()
//    }
//  }
//}
