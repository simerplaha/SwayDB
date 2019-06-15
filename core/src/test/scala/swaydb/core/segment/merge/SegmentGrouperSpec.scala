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

package swaydb.core.segment.merge

import swaydb.core.{TestBase, TestData, TestTimer}
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.{SegmentReader, SegmentWriter}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import swaydb.core.IOAssert._

class SegmentGrouperSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit def groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None
  val keyValueCount = 100

  import keyOrder._

  "addKeyValue" should {
    "add KeyValue to next split and close the split if the new key-value does not fit" in {

      val initialSegment = ListBuffer[KeyValue.WriteOnly]()
      initialSegment += Transient.put(key = 1, value = 1, previous = None, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true)
      initialSegment += Transient.put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true) //total segmentSize is 60 bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](initialSegment)
      //this KeyValue's segment size without footer is 13 bytes
      val keyValue = Memory.put(3, 3)

      //minSegmentSize is 69.bytes. Adding the above keyValues should create a segment of total size
      // 60 + 13 - 3 (common bytes between 2 and 3) which is over the limit = 70.bytes.
      // this should result is closing the existing segment and starting a new segment
      SegmentGrouper.addKeyValue(
        keyValueToAdd = keyValue,
        splits = segments,
        minSegmentSize = 70.bytes,
        maxProbe = TestData.maxProbe,
        forInMemory = false,
        isLastLevel = false,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
        resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
        compressDuplicateValues = true
      )

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 2

      val firstSegment = segments.head
      firstSegment(0).key equiv initialSegment.head.key
      firstSegment(1).key equiv initialSegment.last.key
      firstSegment.last.stats.segmentSize shouldBe 72.bytes

      val secondSegment = segments.last
      secondSegment.head.key equiv keyValue.key
    }

    "add KeyValue all key-values until Group count is reached" in {

      implicit val groupingStrategy =
        Some(
          KeyValueGroupingStrategyInternal.Count(
            count = 1000,
            groupCompression = None,
            indexCompression = randomCompression(),
            valueCompression = randomCompression()
          )
        )

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](ListBuffer.empty)
      val keyValues = randomPutKeyValues(1000)

      keyValues foreach {
        keyValue =>
          SegmentGrouper.addKeyValue(
            keyValueToAdd = keyValue,
            splits = segments,
            minSegmentSize = 100.mb,
            forInMemory = Random.nextBoolean(),
            maxProbe = TestData.maxProbe,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
            isLastLevel = false,
            compressDuplicateValues = true
          )
      }

      segments should have size 1
      segments.head should have size 1
      val group = segments.head.head.asInstanceOf[Transient.Group]
      group.keyValues shouldBe keyValues

      val (segmentBytes, deadline) =
        SegmentWriter.write(
          keyValues = Slice(group),
          createdInLevel = 0,
          maxProbe = TestData.maxProbe,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = TestData.enableRangeFilter
        ).assertGet

      val reader = Reader(segmentBytes)
      val footer = SegmentReader.readFooter(reader.copy()).assertGet
      val readKeyValues = SegmentReader.readAll(footer, reader).assertGet
      readKeyValues shouldBe keyValues
    }

    "add KeyValue all key-values until Group size is reached" in {

      val keyValues = randomKeyValues(1000)

      implicit val groupingStrategy =
        Some(
          KeyValueGroupingStrategyInternal.Size(
            size = keyValues.last.stats.segmentSizeWithoutFooter,
            groupCompression = None,
            indexCompression = randomCompression(),
            valueCompression = randomCompression()
          )
        )

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](ListBuffer.empty)

      keyValues foreach {
        keyValue =>
          SegmentGrouper.addKeyValue(
            keyValueToAdd = keyValue.toMemoryResponse,
            splits = segments,
            minSegmentSize = 100.mb,
            forInMemory = Random.nextBoolean(),
            maxProbe = TestData.maxProbe,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
            isLastLevel = false,
            compressDuplicateValues = true
          )
      }

      segments should have size 1
      segments.head should have size 1
      val group = segments.head.head.asInstanceOf[Transient.Group]
      group.keyValues shouldBe keyValues

      val (segmentBytes, deadline) =
        SegmentWriter.write(
          keyValues = Slice(group),
          createdInLevel = 0,
          maxProbe = TestData.maxProbe,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = TestData.enableRangeFilter
        ).assertGet

      val reader = Reader(segmentBytes)
      val footer = SegmentReader.readFooter(reader.copy()).assertGet
      val readKeyValues = SegmentReader.readAll(footer, reader).assertGet
      readKeyValues shouldBe keyValues
    }

    "add large number of key-values for Grouping" in {

      val keyValues = randomizedKeyValues(100000, addRandomGroups = false)

      val groupSize = keyValues.last.stats.segmentSizeWithoutFooter / 100

      implicit val groupingStrategy =
        Some(
          KeyValueGroupingStrategyInternal.Size(
            size = groupSize,
            groupCompression = None,
            indexCompression = randomCompression(),
            valueCompression = randomCompression()
          )
        )

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](ListBuffer.empty)

      keyValues foreach {
        keyValue =>
          SegmentGrouper.addKeyValue(
            keyValueToAdd = keyValue.toMemoryResponse,
            splits = segments,
            minSegmentSize = 100.mb,
            forInMemory = Random.nextBoolean(),
            maxProbe = TestData.maxProbe,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
            isLastLevel = false,
            compressDuplicateValues = true
          )
      }

      segments should have size 1
      segments.head.size should be > 10
    }
  }
}
