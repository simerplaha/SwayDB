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
//package swaydb.core.segment.format.a
//
//import java.nio.file._
//import scala.concurrent.duration._
//import swaydb.core.CommonAssertions._
//import swaydb.core.IOAssert._
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.data.{Memory, _}
//import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
//import swaydb.core.segment.Segment
//import swaydb.core.util._
//import swaydb.core.{TestBase, TestData, TestLimitQueues}
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//
///**
//  * These class has tests to assert the behavior of [[KeyValueLimiter]] on [[swaydb.core.segment.Segment]]s.
//  */
//class SegmentKeyValueLimiterSpec extends TestBase with Benchmark {
//
//  val keyValuesCount = 100
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//
//  //  override def deleteFiles = false
//
//  "MemorySegment" should {
//    /**
//      * Test that [[Memory.Group]] key-values are never dropped from the [[swaydb.core.segment.MemorySegment]]'s cache.
//      * They should only be uncompressed.
//      */
//    "not drop head Group on memory-overflow" in {
//      //create a group
//      val groupKeyValues = randomizedKeyValues(1000, addRandomGroups = false)
//      val group =
//        Transient.Group(
//          keyValues = groupKeyValues,
//          segmentCompression = randomSegmentCompression(),
//          falsePositiveRate = TestData.falsePositiveRate,
//          enableBinarySearchIndex = TestData.enableBinarySearchIndex,
//          buildFullBinarySearchIndex = TestData.buildFullBinarySearchIndex,
//          resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
//          minimumNumberOfKeysForHashIndex = TestData.minimumNumberOfKeysForHashIndex,
//          allocateSpace = TestData.allocateSpace,
//          previous = None,
//          maxProbe = TestData.maxProbe
//        ).assertGet
//
//      //add more key-values to the right of the Group
//      val nonGroupKeyValues = randomKeyValues(count = 1000, startId = Some(groupKeyValues.last.key.readInt() + 1))
//
//      //key-values to write to the Segment
//      val mergedKeyValues = (Seq(group) ++ nonGroupKeyValues).updateStats
//
//      //set the limiter to drop key-values fast
//      implicit val keyValueLimiter = KeyValueLimiter(1.byte, 100.millis)
//      try {
//        val segment =
//          Segment.memory(
//            path = Paths.get("/test"),
//            createdInLevel = 0,
//            keyValues = mergedKeyValues
//          )(KeyOrder.default, timeOrder, functionStore, TestLimitQueues.fileOpenLimiter, None, keyValueLimiter).assertGet
//
//        //perform reads multiple times and assert that while the key-values are getting drop, the group key-value does
//        //not value dropped
//        runThis(10.times) {
//          eventual(5.seconds) {
//            //cache should only contain the uncompressed Group and other non group key-values
//            segment.cache.size() shouldBe (nonGroupKeyValues.size + 1)
//
//            //assert that group always exists and that it does not value dropped from the cache.
//            val headGroup = segment.cache.firstEntry().getValue.asInstanceOf[Memory.Group]
//            headGroup.isHeaderDecompressed shouldBe false
//            headGroup.isValueDecompressed shouldBe false
//            headGroup.isIndexDecompressed shouldBe false
//            //since no key-values are read the group's key-values should be empty
//            headGroup.segment(KeyOrder.default, keyValueLimiter).isCacheEmpty shouldBe true
//
//            //read all key-values and this should trigger dropping of key-values
//            assertGet(nonGroupKeyValues, segment)
//            assertGet(groupKeyValues, segment)
//
//            //after the key-values are read, Group is decompressed and the groups cache is eventually emptied
//            eventual(5.seconds) {
//              headGroup.isHeaderDecompressed shouldBe true
//              headGroup.isValueDecompressed shouldBe true
//              headGroup.isIndexDecompressed shouldBe true
//              headGroup.segment(KeyOrder.default, keyValueLimiter).isCacheEmpty shouldBe true
//            }
//
//            //but Segment's cache is never empties
//            segment.cache.size() shouldBe (nonGroupKeyValues.size + 1)
//          }
//        }
//      } finally {
//        keyValueLimiter.terminate()
//      }
//    }
//  }
//
//  "PersistentSegment" should {
//    "drop Group key-value only after it's been decompressed" in {
//      //create a group
//      val groupKeyValues = randomKeyValues(10000)
//      val group =
//        Transient.Group(
//          keyValues = groupKeyValues,
//          segmentCompression = randomSegmentCompression(),
//          falsePositiveRate = TestData.falsePositiveRate,
//          enableBinarySearchIndex = TestData.enableBinarySearchIndex,
//          buildFullBinarySearchIndex = TestData.buildFullBinarySearchIndex,
//          resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
//          minimumNumberOfKeysForHashIndex = TestData.minimumNumberOfKeysForHashIndex,
//          allocateSpace = TestData.allocateSpace,
//          previous = None,
//          maxProbe = TestData.maxProbe
//        ).assertGet
//
//      //add key-values to the right of the group
//      val nonGroupKeyValues = randomKeyValues(count = 1000, startId = Some(groupKeyValues.last.key.readInt() + 1))
//
//      //Segment's key-values
//      val mergedKeyValues = (Seq(group) ++ nonGroupKeyValues).updateStats
//
//      //set the limiter to drop key-values fast
//      implicit val keyValueLimiter = KeyValueLimiter(4000.byte, 2.second)
//      try {
//
//        //create persistent Segment
//        val segment = TestSegment(mergedKeyValues)(KeyOrder.default, keyValueLimiter, FileLimiter.empty, timeOrder, None).assertGet
//
//        //initially Segment's cache is empty
//        segment.isCacheEmpty shouldBe true
//
//        //read all key-values and this should trigger dropping of key-values
//        assertGet(nonGroupKeyValues, segment)
//        assertGet(groupKeyValues, segment)
//
//        //Group is cached into the Segment
//        val headGroup = segment.cache.firstEntry().getValue.asInstanceOf[Persistent.Group]
//
//        //since all key-values are read, the Group should be decompressed.
//        headGroup.isHeaderDecompressed shouldBe true
//        headGroup.isValueDecompressed shouldBe true
//        headGroup.isIndexDecompressed shouldBe true
//        //the Groups's cache is not empty as all it's key-values are read.
//        headGroup.segment(KeyOrder.default, keyValueLimiter).isCacheEmpty shouldBe false
//
//        //eventually the cache has dropped all key-values other then Group key-values. Group key-value is only decompressed
//        eventual(2.seconds)(segment.cacheSize shouldBe 1)
//
//        //fetch the head Group key-value from the Segment's cache and assert that it actually is decompressed and it's cache is empty.
//        val headGroupAgain = segment.cache.firstEntry().getValue.asInstanceOf[Persistent.Group]
//        //header is always decompressed because it's added back into the queue and header is read to fetch the decompressed size.
//        //      headGroupAgain.isHeaderDecompressed shouldBe false
//        headGroupAgain.isValueDecompressed shouldBe false
//        headGroupAgain.isIndexDecompressed shouldBe false
//        headGroupAgain.segment(KeyOrder.default, keyValueLimiter).isCacheEmpty shouldBe true
//
//        segment.close.assertGet
//      } finally {
//        keyValueLimiter.terminate()
//      }
//    }
//  }
//}
