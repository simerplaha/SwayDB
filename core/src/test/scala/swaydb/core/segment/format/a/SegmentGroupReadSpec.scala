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

package swaydb.core.segment.format.a

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import swaydb.core.{TestBase, TestData}
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.IOAssert._
import swaydb.core.data.{Memory, Persistent, Transient}
import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, KeyValueGroupingStrategyInternal}
import swaydb.core.queue.{Command, KeyValueLimiter}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class SegmentGroupReadSpec0 extends SegmentGroupReadSpec {
  val keyValuesCount = 100000
}

class SegmentGroupReadSpec1 extends SegmentGroupReadSpec {
  val keyValuesCount = 100000
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentGroupReadSpec2 extends SegmentGroupReadSpec {
  val keyValuesCount = 100000
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentGroupReadSpec3 extends SegmentGroupReadSpec {
  val keyValuesCount = 100000
  override def inMemoryStorage = true
}

sealed trait SegmentGroupReadSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val keyOrder = KeyOrder.default

  def keyValuesCount: Int

  //  override def deleteFiles = false

  "read for randomly compressed key-values" in {
    implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] =
      Some(
        KeyValueGroupingStrategyInternal.Count(
          count = (keyValuesCount / 50) max 10,
          groupCompression =
            Some(
              GroupGroupingStrategyInternal.Count(
                count = 20,
                indexCompression = randomCompression(),
                valueCompression = randomCompression()
              )
            ),
          indexCompression = randomCompression(),
          valueCompression = randomCompression()
        )
      )

    //    println(compressionType)

    val keyValues = randomKeyValues(keyValuesCount, startId = Some(0))
    val segment = TestSegment(keyValues).assertGet

    val segments =
      segment.put(
        newKeyValues = keyValues.toMemory,
        minSegmentSize = 100.mb,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
        resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
        minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
        enableRangeFilter = TestData.enableRangeFilter,
        compressDuplicateValues = true,
        removeDeletes = false,
        createdInLevel = 0,
        maxProbe = TestData.maxProbe
      ).assertGet
    //    printGroupHierarchy(segments)

    segments should have size 1
    val newSegment = segments.head
    assertReads(keyValues, newSegment)
  }

  "Nested groups" should {
    "read their group's key-values only" in {
      val group1KeyValues = randomizedKeyValues(keyValuesCount, addRandomGroups = false, startId = Some(0))
      val group1 = randomGroup(group1KeyValues)

      val group2KeyValues = randomizedKeyValues(keyValuesCount, startId = Some(group1.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
      val group2 = randomGroup((Slice(group1) ++ group2KeyValues).updateStats)

      val group3KeyValues = randomizedKeyValues(keyValuesCount, startId = Some(group2.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
      val group3 = randomGroup((Slice(group2) ++ group3KeyValues).updateStats)

      val group4KeyValues = randomizedKeyValues(keyValuesCount, startId = Some(group3.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
      val group4 = randomGroup((Slice(group3) ++ group4KeyValues).updateStats)

      val group5KeyValues = randomizedKeyValues(keyValuesCount, startId = Some(group4.maxKey.maxKey.readInt() + 1), addRandomGroups = false)
      val group5 = randomGroup((Slice(group4) ++ group5KeyValues).updateStats)

      //group5 is the root Group.
      val allGroupKeyValues = group1KeyValues ++ group2KeyValues ++ group3KeyValues ++ group4KeyValues ++ group5KeyValues

      //write the root Group
      val (bytes, deadline) =
        SegmentWriter.write(
          keyValues = Slice(group5),
          createdInLevel = 0,
          maxProbe = TestData.maxProbe,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = TestData.enableRangeFilter
        ).assertGet

      readAll(bytes).assertGet shouldBe allGroupKeyValues
    }
  }

  //  "Decompressed group" should {
  //    "eventually get uncompressed and update cache" in {
  //      val keyValues = randomizedKeyValues(100, addRandomGroups = false)
  //
  //      implicit val keyValueLimiter = KeyValueLimiter(100.bytes, 5.second)
  //
  //      val group: Transient.Group = randomGroup(keyValues)
  //      val segment = TestSegment(Slice(group)).assertGet
  //
  //      if (persistent) {
  //        segment.isCacheEmpty shouldBe true
  //
  //        println(segment.segmentSize)
  //
  //        println("*** Reading single key-value ***")
  //        segment.get(keyValues.head.key).assertGet shouldBe keyValues.head
  //        assertPostReader()
  //
  //        println("*** Reading all key-values ***")
  //        assertGet(keyValues, segment)
  //        assertPostReader()
  //
  //        def assertPostReader() = {
  //          println("Checking that group is decompressed")
  //          eventual(10.seconds) {
  //            val group = segment.cache.firstEntry().getValue.asInstanceOf[Persistent.Group]
  //            group.isHeaderDecompressed shouldBe true
  //            group.isValueDecompressed shouldBe true
  //            group.isIndexDecompressed shouldBe true
  //            segment.isCacheEmpty shouldBe false
  //          }
  //          println("Checking group should eventually get removed")
  //          eventual(10.seconds) {
  //            segment.isCacheEmpty shouldBe true
  //          }
  //        }
  //      } else {
  //        segment.isCacheEmpty shouldBe false
  //
  //        println(segment.segmentSize)
  //
  //        println("*** Reading single key-value ***")
  //        segment.get(keyValues.head.key).assertGet shouldBe keyValues.head
  //        assertPostReader()
  //
  //        println("*** Reading all key-values ***")
  //        assertGet(keyValues, segment)
  //        assertPostReader()
  //
  //        def assertPostReader() = {
  //          println("Checking group should eventually decompressed")
  //          eventual(10.seconds) {
  //            val group = segment.cache.firstEntry().getValue.asInstanceOf[Memory.Group]
  //            group.isHeaderDecompressed shouldBe false
  //            group.isValueDecompressed shouldBe false
  //            group.isIndexDecompressed shouldBe false
  //            segment.isCacheEmpty shouldBe false
  //          }
  //          //Memory Groups are NEVER removed.
  //          println("Checking Memory Group is not removed.")
  //          eventual(10.seconds) {
  //            segment.isCacheEmpty shouldBe false
  //            segment.cache.firstEntry().getValue.isInstanceOf[Memory.Group] shouldBe true
  //          }
  //        }
  //      }
  //
  //      keyValueLimiter.terminate()
  //    }
  //  }
}
