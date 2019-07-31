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

import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.IOValues._
import swaydb.core.data._
import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, KeyValueGroupingStrategyInternal}
import swaydb.core.segment.format.a.block._
import swaydb.core.{TestBase, TestData, TestTimer}
import swaydb.data.order.KeyOrder
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.RunThis._
import swaydb.core.io.reader.Reader
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

class SegmentGrouperSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit def groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None
  val keyValueCount = 100

  import keyOrder._

  "addKeyValue" should {
    "add KeyValue to next split and close the split if the new key-value does not fit" in {
      runThis(100.times) {
        val initialSegment = ListBuffer[Transient]()
        initialSegment += Transient.put(key = 1, value = Some(1), previous = None)
        initialSegment += Transient.put(key = 2, value = Some(2), previous = initialSegment.lastOption) //total segmentSize is 133.bytes

        val segments = ListBuffer[ListBuffer[Transient]](initialSegment)
        //this KeyValue's segment size without footer is 17.bytes
        val keyValue = Memory.put(3, 3)

        val minSegmentSize =
          eitherOne(
            initialSegment.last.stats.segmentSize.bytes,
            initialSegment.last.stats.segmentSize.bytes - randomIntMax(initialSegment.last.stats.segmentSize.bytes),
            initialSegment.last.stats.segmentSize.bytes - randomIntMax(initialSegment.last.stats.segmentSize.bytes / 2),
            randomIntMax(initialSegment.last.stats.segmentSize.bytes)
          ) //<= segmentSize of the first 2 key-values which should always result in a new segment being created

        //total Segment bytes with the next key-value is 150.bytes which also the minimum SegmentSize which should start a new segment on add.
        SegmentGrouper.addKeyValue(
          keyValueToAdd = keyValue,
          splits = segments,
          minSegmentSize = minSegmentSize,
          forInMemory = false,
          isLastLevel = false,
          createdInLevel = 0,
          valuesConfig = ValuesBlock.Config.random,
          sortedIndexConfig = SortedIndexBlock.Config.random,
          binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
          hashIndexConfig = HashIndexBlock.Config.random,
          bloomFilterConfig = BloomFilterBlock.Config.random,
          segmentIO = SegmentIO.random
        )

        //the initialSegment should be closed and a new segment should value started
        segments.size shouldBe 2

        val firstSegment = segments.head
        firstSegment(0).key equiv initialSegment.head.key
        firstSegment(1).key equiv initialSegment.last.key
        firstSegment.last.stats.segmentSize should be >= minSegmentSize

        val secondSegment = segments.last
        secondSegment.head.key equiv keyValue.key
      }
    }

    "add KeyValue all key-values until Group count is reached" in {

      runThis(1.times) {
        val forInMemory = randomBoolean()

        implicit val groupingStrategy =
          Some(
            KeyValueGroupingStrategyInternal.Count(
              count = 100000,
              applyGroupingOnCopy = randomBoolean(),
              groupCompression = None,
              bloomFilterConfig = BloomFilterBlock.Config.random,
              hashIndexConfig = HashIndexBlock.Config.random,
              binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
              sortedIndexConfig = SortedIndexBlock.Config.random,
              valuesConfig = ValuesBlock.Config.random,
              groupConfig = SegmentBlock.Config.random
            )
          )

        val segments = ListBuffer[ListBuffer[Transient]](ListBuffer.empty)
        val keyValues = randomPutKeyValues(100000)

        Benchmark("Grouping") {
          keyValues foreach {
            keyValue =>
              SegmentGrouper.addKeyValue(
                keyValueToAdd = keyValue,
                splits = segments,
                minSegmentSize = 100.mb,
                forInMemory = forInMemory,
                createdInLevel = 0,
                isLastLevel = false,
                valuesConfig = ValuesBlock.Config.random,
                sortedIndexConfig = SortedIndexBlock.Config.random,
                binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
                hashIndexConfig = HashIndexBlock.Config.random,
                bloomFilterConfig = BloomFilterBlock.Config.random,
                segmentIO = SegmentIO.random
              )
          }
        }

        Benchmark("Size check") {
          segments.size shouldBe 1
//          segments.head.size shouldBe 1
//          val group = segments.head.head.asInstanceOf[Transient.Group]
        }
//        group.keyValues shouldBe keyValues
//
//        val persistentGroupKeyValues = readAll(group).value
//        persistentGroupKeyValues shouldBe group.keyValues
      }
    }

    "add KeyValue all key-values until Group size is reached" in {

      val keyValues = randomKeyValues(1000)

      val forInMemory = randomBoolean()

      implicit val groupingStrategy =
        Some(
          KeyValueGroupingStrategyInternal.Size(
            size = keyValues.last.stats.segmentSizeWithoutFooter,
            applyGroupingOnCopy = randomBoolean(),
            groupCompression = None,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            valuesConfig = ValuesBlock.Config.random,
            groupConfig = SegmentBlock.Config.random
          )
        )

      val segments = ListBuffer[ListBuffer[Transient]](ListBuffer.empty)

      Benchmark("") {
        keyValues foreach {
          keyValue =>
            SegmentGrouper.addKeyValue(
              keyValueToAdd = keyValue.toMemory,
              splits = segments,
              minSegmentSize = 100.mb,
              forInMemory = forInMemory,
              createdInLevel = 0,
              isLastLevel = false,
              valuesConfig = ValuesBlock.Config.random,
              sortedIndexConfig = SortedIndexBlock.Config.random,
              binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
              hashIndexConfig = HashIndexBlock.Config.random,
              bloomFilterConfig = BloomFilterBlock.Config.random,
              segmentIO = SegmentIO.random
            )
        }
      }

      segments should have size 1
      segments.head should have size 1
      val group = segments.head.head.asInstanceOf[Transient.Group]
//      group.keyValues shouldBe keyValues

//      val (segmentBytes, deadline) =
//        SegmentWriter.write(
//          keyValues = Slice(group),
//          createdInLevel = 0,
//          maxProbe = TestData.maxProbe
//        ).runIO.flatten
//
//      val reader = Reader(segmentBytes)
//      val footer = SegmentFooter.read(reader.copy()).runIO
//      val readKeyValues = SortedIndex.readAll(footer.sortedIndexOffset, footer.keyValueCount, reader).runIO
//      readKeyValues shouldBe keyValues
    }

    //    "add large number of key-values for Grouping" in {
    //
    //      val keyValues = randomizedKeyValues(100000, addGroups = false)
    //
    //      val groupSize = keyValues.last.stats.segmentSizeWithoutFooter / 100
    //
    //      implicit val groupingStrategy =
    //        Some(
    //          KeyValueGroupingStrategyInternal.Size(
    //            size = groupSize,
    //            groupCompression = None,
    //            indexCompression = randomCompression(),
    //            valueCompression = randomCompression()
    //          )
    //        )
    //
    //      val segments = ListBuffer[ListBuffer[Transient]](ListBuffer.empty)
    //
    //      keyValues foreach {
    //        keyValue =>
    //          SegmentGrouper.addKeyValue(
    //            keyValueToAdd = keyValue.toMemoryResponse,
    //            splits = segments,
    //            minSegmentSize = 100.mb,
    //            maxProbe = TestData.maxProbe,
    //            enableBinarySearchIndex = TestData.enableBinarySearchIndex,
    //            buildFullBinarySearchIndex = TestData.buildFullBinarySearchIndex,
    //            forInMemory = Random.nextBoolean(),
    //            isLastLevel = false,
    //            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
    //            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
    //            minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeysForHashIndex,
    //            allocateSpace = TestData.allocateSpace,
    //            compressDuplicateValues = true
    //          )
    //      }
    //
    //      segments should have size 1
    //      segments.head.size should be > 10
    //    }
  }
}
