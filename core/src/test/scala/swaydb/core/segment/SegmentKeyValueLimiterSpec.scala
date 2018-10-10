/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import java.nio.file._

import swaydb.core.data.{Memory, _}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.DBFile
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.util._
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder

import scala.concurrent.duration._

class SegmentKeyValueLimiterSpec extends TestBase with Benchmark {

  val keyValuesCount = 100

  implicit override val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None

  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default

  //  override def deleteFiles = false

  implicit val fileOpenLimiterImplicit: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValueLimiterImplicit: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  "MemorySegment" should {
    /**
      * Test that [[Memory.Group]] key-values are never dropped from the [[MemorySegment]]'s cache.
      * They should only be uncompressed.
      */
    "not drop head Group key-values on memory-overflow" in {
      //create a group
      val groupKeyValues = randomIntKeyValues(1000)
      val group =
        Transient.Group(
          keyValues = groupKeyValues,
          indexCompression = randomCompression(),
          valueCompression = randomCompression(),
          falsePositiveRate = 0.1,
          previous = None
        ).assertGet

      //add more key-values to the right of the Group
      val nonGroupKeyValues = randomIntKeyValues(count = 1000, startId = Some(groupKeyValues.last.key.readInt() + 1))

      //key-values to write to the Segment
      val mergedKeyValues = (Seq(group) ++ nonGroupKeyValues).updateStats

      //set the limiter to drop key-values fast
      implicit val keyValueLimiter = KeyValueLimiter(1.byte, 1.millis)

      val segment =
        Segment.memory(
          path = Paths.get("/test"),
          keyValues = mergedKeyValues,
          bloomFilterFalsePositiveRate = 0.1,
          removeDeletes = false
        )(ordering, None, keyValueLimiter).assertGet

      //perform reads multiple times and assert that while the key-values are getting drop, the group key-value does
      //not get dropped
      runThis(10.times) {
        //cache should only contain the uncompressed Group and other non group key-values
        segment.cache.size() shouldBe (nonGroupKeyValues.size + 1)

        //assert that group always exists and that it does not get dropped from the cache.
        val headGroup = segment.cache.firstEntry().getValue.asInstanceOf[Memory.Group]
        headGroup.isHeaderDecompressed shouldBe false
        headGroup.isValueDecompressed shouldBe false
        headGroup.isIndexDecompressed shouldBe false
        //since no key-values are read the group's key-values should be empty
        headGroup.segmentCache(ordering, keyValueLimiter).isCacheEmpty shouldBe true

        //read all key-values and this should trigger dropping of key-values
        assertGet(nonGroupKeyValues, segment)
        assertGet(groupKeyValues, segment)

        //after the key-values are read, Group is decompressed and the groups cache is not empty
        headGroup.isHeaderDecompressed shouldBe true
        headGroup.isValueDecompressed shouldBe true
        headGroup.isIndexDecompressed shouldBe true
        headGroup.segmentCache(ordering, keyValueLimiter).isCacheEmpty shouldBe false
        //wait to allow for limit to do it's clean up
        sleep(1.second)
      }
    }
  }

  /**
    * This test is hard to get accurate and is commented out because
    * it's difficult to determine the behavior of [[KeyValueLimiter]] during runtime but this test can
    * used to debug the behavior of [[KeyValueLimiter]] on [[Persistent.Group]].
    *
    * It tests that [[Persistent.Group]] is always uncompressed before it get dropped from the Segment's cache.
    */
  //  "PersistentSegment" should {
  //    "drop Group key-value only after it's been decompressed" in {
  //      //create a group
  //      val groupKeyValues = randomIntKeyValues(1000)
  //      val group =
  //        Transient.Group(
  //          keyValues = groupKeyValues,
  //          indexCompression = randomCompression(),
  //          valueCompression = randomCompression(),
  //          falsePositiveRate = 0.1,
  //          previous = None
  //        ).assertGet
  //
  //      //add key-values to the right of the group
  //      val nonGroupKeyValues = randomIntKeyValues(count = 1000, startId = Some(groupKeyValues.last.key.readInt() + 1))
  //
  //      //Segment's key-values
  //      val mergedKeyValues = (Seq(group) ++ nonGroupKeyValues).updateStats
  //
  //      //set the limiter to drop key-values fast
  //      implicit val keyValueLimiter = KeyValueLimiter(4000.byte, 2.second)
  //
  //      //create persistent Segment
  //      val segment = TestSegment(mergedKeyValues)(ordering, keyValueLimiter, fileOpenLimiterImplicit, None).assertGet
  //
  //      //initially Segment's cache is empty
  //      segment.isCacheEmpty shouldBe true
  //
  //      //read all key-values and this should trigger dropping of key-values
  //      assertGet(nonGroupKeyValues, segment)
  //      assertGet(groupKeyValues, segment)
  //
  //      //Group is cached into the Segment
  //      val headGroup = segment.cache.firstEntry().getValue.asInstanceOf[Persistent.Group]
  //
  //      //since all key-values are read, the Group should be decompressed.
  //      headGroup.isHeaderDecompressed shouldBe true
  //      headGroup.isValueDecompressed shouldBe true
  //      headGroup.isIndexDecompressed shouldBe true
  //      //the Groups's cache is not empty as all it's key-values are read.
  //      headGroup.segmentCache(ordering, keyValueLimiter).isCacheEmpty shouldBe false
  //
  //      //eventually the cache has dropped all key-values other then Group key-values. Group key-value is only decompressed
  //      eventual(2.seconds)(segment.cacheSize shouldBe 1)
  //
  //      //fetch the head Group key-value from the Segment's cache and assert that it actually is decompressed and it's cache is empty.
  //      val headGroupAgain = segment.cache.firstEntry().getValue.asInstanceOf[Persistent.Group]
  //      //header is always decompressed because it's added back into the queue and header is read to fetch the decompressed size.
  //      //      headGroupAgain.isHeaderDecompressed shouldBe false
  //      headGroupAgain.isValueDecompressed shouldBe false
  //      headGroupAgain.isIndexDecompressed shouldBe false
  //      headGroupAgain.segmentCache(ordering, keyValueLimiter).isCacheEmpty shouldBe true
  //    }
  //  }

}