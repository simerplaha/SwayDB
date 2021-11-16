/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package swaydb.core.segment.block
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.core.CommonAssertions._
//import swaydb.core.{TestBase, TestSweeper}
//import swaydb.core.TestData._
//import swaydb.core.io.file.BlockCache
//import swaydb.Benchmark
//import swaydb.data.config.IOStrategy
//import swaydb.slice.order.KeyOrder
//import swaydb.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.util.Random
//
//class SegmentSearcherPerformanceSpec extends TestBase with MockFactory {
//  implicit val order = KeyOrder.default
//  implicit def blockCache: Option[BlockCacheState] = TestSweeper.randomBlockCache
//
//  "search performance" in {
//    val compressions = Slice.fill(5)(randomCompressionsOrEmpty())
//
//    val access = IOStrategy.ConcurrentIO(true)
//
//    val keyValues =
//      randomizedKeyValues(
//        count = 100000,
//        startId = Some(1),
//        addPut = true,
//        addGroups = false
//      )(
//        valuesConfig =
//          ValuesBlockConfig(
//            compressDuplicateValues = randomBoolean(),
//            compressDuplicateRangeValues = randomBoolean(),
//            blockIO = _ => access,
//            compressions = _ => compressions.head
//          ),
//        sortedIndexConfig =
//          SegmentIndexBlockConfig.create(
//            blockIO = _ => access,
//            prefixCompressionResetCount = 0,
//            enableAccessPositionIndex = true,
//            normaliseForBinarySearch = randomBoolean(),
//            compressions = _ => compressions(1)
//          ),
//        binarySearchIndexConfig =
//          BinarySearchIndexConfig(
//            enabled = true,
//            minimumNumberOfKeys = 1,
//            fullIndex = true,
//            blockIO = _ => access,
//            compressions = _ => compressions(2)
//          ),
//        hashIndexConfig =
//          HashIndexConfig(
//            maxProbe = 5,
//            minimumNumberOfKeys = 2,
//            minimumNumberOfHits = 2,
//            allocateSpace = _.requiredSpace * 10,
//            blockIO = _ => access,
//            compressions = _ => compressions(3)
//          ),
//        bloomFilterConfig =
//          BloomFilterConfig(
//            falsePositiveRate = 0.001,
//            minimumNumberOfKeys = 2,
//            blockIO = _ => access,
//            compressions = _ => compressions(4)
//          )
//      )
//
//    val closedSegment =
//      SegmentBlock.writeClosed(
//        keyValues = keyValues,
//        segmentConfig =
//          new SegmentBlockConfig(
//            blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
//            compressions = _ => Seq.empty
//          ),
//        createdInLevel = 0
//      ).get
//
//    val fileReader = createRandomFileReader(closedSegment.flattenSegmentBytes)
//    val blocks = readBlocksFromReader(fileReader, SegmentIO.defaultSynchronisedStored).get
//
//    val randomKeyValues = Random.shuffle(keyValues)
//
//    Benchmark("search performance") {
//      randomKeyValues foreach {
//        keyValue =>
//          SegmentSearcher.search(
//            key = keyValue.key,
//            start = None,
//            end = None,
//            hashIndexReader = blocks.hashIndexReader,
//            binarySearchIndexReader = blocks.binarySearchIndexReader,
//            sortedIndexReader = blocks.sortedIndexReader,
//            valuesReader = blocks.valuesReader,
//            hasRange = keyValues.last.stats.segmentHasRange
//          ).get shouldBe keyValue
//      }
//    }
//  }
//}
