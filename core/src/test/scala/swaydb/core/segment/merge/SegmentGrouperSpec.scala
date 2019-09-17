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

import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.order.KeyOrder
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer

class SegmentGrouperSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit def groupBy: Option[GroupByInternal.KeyValues] = None
  val keyValueCount = 100

  import keyOrder._

  "addKeyValue" should {
    "add KeyValue to next split and close the split if the new key-value does not fit" in {
      runThis(100.times) {
        val initialSegment = SegmentBuffer(groupBy)
        initialSegment add Transient.put(key = 1, value = Some(1), previous = None)
        initialSegment add Transient.put(key = 2, value = Some(2), previous = initialSegment.lastOption) //total segmentSize is 133.bytes

        val segments = ListBuffer[SegmentBuffer](initialSegment)
        //this KeyValue's segment size without footer is 17.bytes
        val keyValue = Memory.put(3, 3)

        val minSegmentSize =
          eitherOne(
            initialSegment.last.stats.segmentSize.bytes,
            initialSegment.last.stats.segmentSize.bytes - randomIntMax(initialSegment.last.stats.segmentSize.bytes),
            initialSegment.last.stats.segmentSize.bytes - randomIntMax(initialSegment.last.stats.segmentSize.bytes / 2),
            randomIntMax(initialSegment.last.stats.segmentSize.bytes)
          ) //<= segmentSize of the first 2 key-values which should always result in a new segment being created

        val segmentMergeConfigs =
          SegmentMergeConfigs(
            segmentValuesConfig = ValuesBlock.Config.random,
            segmentSortedIndexConfig = SortedIndexBlock.Config.random,
            segmentBinarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            segmentHashIndexConfig = HashIndexBlock.Config.random,
            segmentBloomFilterConfig = BloomFilterBlock.Config.random,
            groupBy = groupBy
          )

        //total Segment bytes with the next key-value is 150.bytes which also the minimum SegmentSize which should start a new segment on add.
        SegmentGrouper.addKeyValue(
          keyValueToAdd = keyValue,
          splits = segments,
          minSegmentSize = minSegmentSize,
          forInMemory = false,
          isLastLevel = false,
          createdInLevel = 0,
          segmentMergeConfigs = segmentMergeConfigs,
          segmentIO = SegmentIO.random
        )

        //the initialSegment should be closed and a new segment should value started
        segments.size shouldBe 2

        val firstSegment = segments.head
        firstSegment.head.key equiv initialSegment.head.key
        firstSegment.toList(1).key equiv initialSegment.last.key
        firstSegment.last.stats.segmentSize should be >= minSegmentSize

        val secondSegment = segments.last
        secondSegment.head.key equiv keyValue.key
      }
    }

    "add KeyValue all key-values until Group count is reached" in {
      runThis(20.times) {
        val forInMemory = randomBoolean()

        val keyValueCount = 10000

        val groupBy =
          GroupByInternal.KeyValues(
            count = keyValueCount,
            size = None,
            applyGroupingOnCopy = randomBoolean(),
            groupByGroups = None,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            valuesConfig = ValuesBlock.Config.random,
            groupConfig = SegmentBlock.Config.random
          )

        val segmentMergeConfigs =
          SegmentMergeConfigs(
            segmentValuesConfig = ValuesBlock.Config.random,
            segmentSortedIndexConfig = SortedIndexBlock.Config.random,
            segmentBinarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            segmentHashIndexConfig = HashIndexBlock.Config.random,
            segmentBloomFilterConfig = BloomFilterBlock.Config.random,
            groupBy = Some(groupBy)
          )

        val buffer: SegmentBuffer.Grouped = SegmentBuffer(groupBy)
        val segments = ListBuffer[SegmentBuffer](buffer)
        val keyValues = randomizedKeyValues(keyValueCount, addGroups = false).toMemory

        Benchmark(s"Grouping ${keyValues.size} key-values.") {
          keyValues foreach {
            keyValue =>
              SegmentGrouper.addKeyValue(
                keyValueToAdd = keyValue,
                splits = segments,
                minSegmentSize = 100.mb,
                forInMemory = forInMemory,
                createdInLevel = 0,
                isLastLevel = false,
                segmentMergeConfigs = segmentMergeConfigs,
                segmentIO = SegmentIO.random
              ).get
          }
        }

        segments.size shouldBe 1
        buffer.currentGroups.size shouldBe 1
        buffer.unGrouped.size shouldBe 0
        val group = buffer.groupedKeyValues.head
        group.keyValues shouldBe keyValues

        val persistentGroupKeyValues = readAll(group).right.value
        persistentGroupKeyValues shouldBe group.keyValues
      }
    }

    "group key-values equally" in {
      runThis(20.times) {
        val forInMemory = randomBoolean()

        val keyValueCount = 10000

        val groupBy =
          GroupByInternal.KeyValues(
            count = keyValueCount / 100,
            size = None,
            applyGroupingOnCopy = randomBoolean(),
            groupByGroups = None,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            valuesConfig = ValuesBlock.Config.random,
            groupConfig = SegmentBlock.Config.random
          )

        val buffer: SegmentBuffer.Grouped = SegmentBuffer(groupBy)
        val segments = ListBuffer[SegmentBuffer](buffer)
        val keyValues = randomizedKeyValues(keyValueCount, addGroups = false).toMemory

        val segmentMergeConfigs =
          SegmentMergeConfigs(
            segmentValuesConfig = ValuesBlock.Config.random,
            segmentSortedIndexConfig = SortedIndexBlock.Config.random,
            segmentBinarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            segmentHashIndexConfig = HashIndexBlock.Config.random,
            segmentBloomFilterConfig = BloomFilterBlock.Config.random,
            groupBy = Some(groupBy)
          )

        Benchmark(s"Grouping ${keyValues.size} key-values.") {
          keyValues foreach {
            keyValue =>
              SegmentGrouper.addKeyValue(
                keyValueToAdd = keyValue,
                splits = segments,
                minSegmentSize = 100.mb,
                forInMemory = forInMemory,
                createdInLevel = 0,
                isLastLevel = false,
                segmentMergeConfigs = segmentMergeConfigs,
                segmentIO = SegmentIO.random
              ).get
          }
        }

        segments.size shouldBe 1
        buffer.currentGroups.size shouldBe 100
        buffer.unGrouped.size shouldBe 0

        Benchmark("write a read all") {
          val persistentGroupKeyValues = writeAndRead(buffer).right.value
          persistentGroupKeyValues shouldBe keyValues
        }
      }
    }

    "group key-values when size is specified" in {
      runThis(20.times) {
        val forInMemory = randomBoolean()

        val keyValueCount = 10000

        val groupBy =
          GroupByInternal.KeyValues(
            count = keyValueCount,
            size = Some(1.kb),
            applyGroupingOnCopy = randomBoolean(),
            groupByGroups = None,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            valuesConfig = ValuesBlock.Config.random,
            groupConfig = SegmentBlock.Config.random
          )

        val buffer: SegmentBuffer.Grouped = SegmentBuffer(groupBy)
        val segments = ListBuffer[SegmentBuffer](buffer)
        val keyValues = randomizedKeyValues(keyValueCount, addGroups = false).toMemory

        val segmentMergeConfigs =
          SegmentMergeConfigs(
            segmentValuesConfig = ValuesBlock.Config.random,
            segmentSortedIndexConfig = SortedIndexBlock.Config.random,
            segmentBinarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            segmentHashIndexConfig = HashIndexBlock.Config.random,
            segmentBloomFilterConfig = BloomFilterBlock.Config.random,
            groupBy = Some(groupBy)
          )

        Benchmark(s"Grouping ${keyValues.size} key-values.") {
          keyValues foreach {
            keyValue =>
              SegmentGrouper.addKeyValue(
                keyValueToAdd = keyValue,
                splits = segments,
                minSegmentSize = 100.mb,
                forInMemory = forInMemory,
                createdInLevel = 0,
                isLastLevel = false,
                segmentMergeConfigs = segmentMergeConfigs,
                segmentIO = SegmentIO.random
              ).get
          }
        }

        segments.size shouldBe 1
        buffer.currentGroups.size should be > 100

        //group any ungrouped key-values
        SegmentGrouper.group(
          buffer = buffer,
          createdInLevel = 0,
          segmentValuesConfig = segmentMergeConfigs.segmentValuesConfig,
          segmentSortedIndexConfig = segmentMergeConfigs.segmentSortedIndexConfig,
          segmentBinarySearchIndexConfig = segmentMergeConfigs.segmentBinarySearchIndexConfig,
          segmentHashIndexConfig = segmentMergeConfigs.segmentHashIndexConfig,
          segmentBloomFilterConfig = segmentMergeConfigs.segmentBloomFilterConfig,
          force = true,
          skipQuotaCheck = false
        ).get

        buffer.unGrouped.size shouldBe 0

        Benchmark("write a read all") {
          val persistentGroupKeyValues = writeAndRead(buffer).right.value
          printGroupHierarchy(persistentGroupKeyValues, 5)
          persistentGroupKeyValues shouldBe keyValues
        }
      }
    }

    "group by groups" in {
      runThis(20.times) {
        val forInMemory = randomBoolean()

        val keyValueCount = 1000

        val groupBy =
          GroupByInternal.KeyValues(
            count = keyValueCount / 10,
            size = None,
            applyGroupingOnCopy = randomBoolean(),
            groupByGroups =
              Some(
                GroupByInternal.Groups(
                  count = 2,
                  size = None,
                  valuesConfig = ValuesBlock.Config.random,
                  sortedIndexConfig = SortedIndexBlock.Config.random,
                  binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
                  hashIndexConfig = HashIndexBlock.Config.random,
                  bloomFilterConfig = BloomFilterBlock.Config.random,
                  groupConfig = SegmentBlock.Config.random,
                  applyGroupingOnCopy = randomBoolean()
                )
              ),
            bloomFilterConfig = BloomFilterBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            valuesConfig = ValuesBlock.Config.random,
            groupConfig = SegmentBlock.Config.random
          )

        val buffer: SegmentBuffer.Grouped = SegmentBuffer(groupBy)
        val segments = ListBuffer[SegmentBuffer](buffer)
        val keyValues = randomizedKeyValues(keyValueCount, addGroups = false).toMemory

        val segmentMergeConfigs =
          SegmentMergeConfigs(
            segmentValuesConfig = ValuesBlock.Config.random,
            segmentSortedIndexConfig = SortedIndexBlock.Config.random,
            segmentBinarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            segmentHashIndexConfig = HashIndexBlock.Config.random,
            segmentBloomFilterConfig = BloomFilterBlock.Config.random,
            groupBy = Some(groupBy)
          )

        Benchmark(s"Grouping ${keyValues.size} key-values.") {
          keyValues foreach {
            keyValue =>
              SegmentGrouper.addKeyValue(
                keyValueToAdd = keyValue,
                splits = segments,
                minSegmentSize = 100.mb,
                forInMemory = forInMemory,
                createdInLevel = 0,
                isLastLevel = false,
                segmentMergeConfigs = segmentMergeConfigs,
                segmentIO = SegmentIO.random
              ).get
          }
        }

        segments.size shouldBe 1
        buffer.currentGroups.size shouldBe 1

        //group any ungrouped key-values
        SegmentGrouper.group(
          buffer = buffer,
          createdInLevel = 0,
          segmentValuesConfig = segmentMergeConfigs.segmentValuesConfig,
          segmentSortedIndexConfig = segmentMergeConfigs.segmentSortedIndexConfig,
          segmentBinarySearchIndexConfig = segmentMergeConfigs.segmentBinarySearchIndexConfig,
          segmentHashIndexConfig = segmentMergeConfigs.segmentHashIndexConfig,
          segmentBloomFilterConfig = segmentMergeConfigs.segmentBloomFilterConfig,
          force = true,
          skipQuotaCheck = false
        ).get

        buffer.unGrouped.size shouldBe 0

        Benchmark("write a read all") {
          val persistentGroupKeyValues = writeAndRead(buffer).right.value
          printGroupHierarchy(persistentGroupKeyValues, 5)
          persistentGroupKeyValues shouldBe keyValues
        }
      }
    }
  }
}
