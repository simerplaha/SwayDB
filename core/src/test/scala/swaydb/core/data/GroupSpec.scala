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

package swaydb.core.data

import java.nio.file.Paths
import swaydb.core.{TestBase, TestData, TestLimitQueues, TestTimeGenerator}
import swaydb.core.segment.Segment
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.TryAssert._
import swaydb.data.order.{KeyOrder, TimeOrder}

class GroupSpec extends TestBase {

  val keyValueCount = 100

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def timeGenerator: TestTimeGenerator = TestTimeGenerator.random
  implicit val keyValueLimiter = TestLimitQueues.keyValueLimiter


  "lastGroup on a list of WriteOnly key-values" should {
    "return empty if there are no groups" in {
      Seq.empty[KeyValue.WriteOnly].lastGroup() shouldBe empty
    }

    "return last group if there is only one group" in {
      val group =
        Transient.Group(
          keyValues = randomizedKeyValues(keyValueCount),
          indexCompression = randomCompression(),
          valueCompression = randomCompression(),
          falsePositiveRate = TestData.falsePositiveRate,
          previous = None
        ).assertGet

      Seq(group).lastGroup().assertGet shouldBe group
    }

    "return last group if there are multiple groups" in {
      val groups =
        (1 to 10) map {
          _ =>
            Transient.Group(
              keyValues = randomizedKeyValues(keyValueCount),
              indexCompression = randomCompression(),
              valueCompression = randomCompression(),
              falsePositiveRate = TestData.falsePositiveRate,
              previous = None
            ).assertGet
        }

      groups.lastGroup().assertGet shouldBe groups.last
    }

    "return last group if there are multiple groups after a non Group key-value" in {
      val groups =
        (1 to 10) map {
          i =>
            if (i == 5)
              Transient.put(1) //on the 5th iteration add a Put key-values. The 4th group should be returned.
            else
              Transient.Group(
                keyValues = randomizedKeyValues(keyValueCount),
                indexCompression = randomCompression(),
                valueCompression = randomCompression(),
                falsePositiveRate = TestData.falsePositiveRate,
                previous = None
              ).assertGet
        }

      groups.lastGroup().assertGet shouldBe groups.drop(3).head.asInstanceOf[Transient.Group]
    }
  }

  "uncompressing a Group" should {
    "return a new instance of uncompressed Persistent.Group" in {
      //create group key-values
      val keyValues = randomPutKeyValues(keyValueCount)
      val group =
        Transient.Group(
          keyValues = keyValues.toTransient,
          indexCompression = randomCompression(),
          valueCompression = randomCompression(),
          falsePositiveRate = TestData.falsePositiveRate,
          previous = None
        ).assertGet

      //create Segment
      val segment = TestSegment(Slice(group)).assertGet

      //read all Group's key-values
      val readKeyValues = segment.getAll().assertGet
      readKeyValues should have size 1
      val readGroup = readKeyValues.head.asInstanceOf[Persistent.Group]

      //assert that group is decompressed
      readGroup.isHeaderDecompressed shouldBe false
      readGroup.isIndexDecompressed shouldBe false
      readGroup.isValueDecompressed shouldBe false

      val segmentCache = readGroup.segmentCache
      segmentCache.isCacheEmpty shouldBe true
      segmentCache.get(keyValues.head.key).assertGet shouldBe keyValues.head

      readGroup.isHeaderDecompressed shouldBe true
      readGroup.isIndexDecompressed shouldBe true
      readGroup.isValueDecompressed shouldBe true

      //uncompressed the Group
      val uncompressedGroup = readGroup.uncompress()

      //assert the result of uncompression
      uncompressedGroup.isHeaderDecompressed shouldBe false
      uncompressedGroup.isIndexDecompressed shouldBe false
      uncompressedGroup.isValueDecompressed shouldBe false
      uncompressedGroup.segmentCache.isCacheEmpty shouldBe true
      uncompressedGroup.segmentCache.get(keyValues.head.key).assertGet shouldBe keyValues.head
      uncompressedGroup.segmentCache.isCacheEmpty shouldBe false
    }

    "return a new instance of uncompressed Memory.Group" in {
      val keyValues = randomKeyValues(keyValueCount)
      val group =
        Transient.Group(
          keyValues = keyValues,
          indexCompression = randomCompression(),
          valueCompression = randomCompression(),
          falsePositiveRate = TestData.falsePositiveRate,
          previous = None
        ).assertGet

      implicit val groupingStrategy = None

      val segment =
        Segment.memory(
          path = Paths.get("/test"),
          keyValues = Seq(group),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          removeDeletes = false
        ).assertGet

      val readKeyValues = segment.getAll().assertGet
      readKeyValues should have size 1
      val readGroup = readKeyValues.head.asInstanceOf[Memory.Group]

      readGroup.isHeaderDecompressed shouldBe false
      readGroup.isIndexDecompressed shouldBe false
      readGroup.isValueDecompressed shouldBe false

      val segmentCache = readGroup.segmentCache
      segmentCache.isCacheEmpty shouldBe true
      segmentCache.get(keyValues.head.key).assertGet shouldBe keyValues.head

      readGroup.isHeaderDecompressed shouldBe true
      readGroup.isIndexDecompressed shouldBe true
      readGroup.isValueDecompressed shouldBe true

      val uncompressedGroup = readGroup.uncompress()

      uncompressedGroup.isHeaderDecompressed shouldBe false
      uncompressedGroup.isIndexDecompressed shouldBe false
      uncompressedGroup.isValueDecompressed shouldBe false
      uncompressedGroup.segmentCache.isCacheEmpty shouldBe true
      uncompressedGroup.segmentCache.get(keyValues.head.key).assertGet shouldBe keyValues.head
      uncompressedGroup.segmentCache.isCacheEmpty shouldBe false
    }
  }

}
