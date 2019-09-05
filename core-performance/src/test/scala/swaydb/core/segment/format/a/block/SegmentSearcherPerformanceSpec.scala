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
//package swaydb.core.segment.format.a.block
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.core.CommonAssertions._
//import swaydb.core.{TestBase, TestLimitQueues}
//import swaydb.core.TestData._
//import swaydb.core.io.file.BlockCache
//import swaydb.core.util.Benchmark
//import swaydb.data.config.IOStrategy
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.util.Random
//
//class SegmentSearcherPerformanceSpec extends TestBase with MockFactory {
//  implicit val order = KeyOrder.default
//  implicit def blockCache: Option[BlockCache.State] = TestLimitQueues.randomBlockCache
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
//      ).updateStats(
//        valuesConfig =
//          ValuesBlock.Config(
//            compressDuplicateValues = randomBoolean(),
//            compressDuplicateRangeValues = randomBoolean(),
//            blockIO = _ => access,
//            compressions = _ => compressions.head
//          ),
//        sortedIndexConfig =
//          SortedIndexBlock.Config.create(
//            blockIO = _ => access,
//            prefixCompressionResetCount = 0,
//            enableAccessPositionIndex = true,
//            normaliseForBinarySearch = randomBoolean(),
//            compressions = _ => compressions(1)
//          ),
//        binarySearchIndexConfig =
//          BinarySearchIndexBlock.Config(
//            enabled = true,
//            minimumNumberOfKeys = 1,
//            fullIndex = true,
//            blockIO = _ => access,
//            compressions = _ => compressions(2)
//          ),
//        hashIndexConfig =
//          HashIndexBlock.Config(
//            maxProbe = 5,
//            minimumNumberOfKeys = 2,
//            minimumNumberOfHits = 2,
//            allocateSpace = _.requiredSpace * 10,
//            blockIO = _ => access,
//            compressions = _ => compressions(3)
//          ),
//        bloomFilterConfig =
//          BloomFilterBlock.Config(
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
//          new SegmentBlock.Config(
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
