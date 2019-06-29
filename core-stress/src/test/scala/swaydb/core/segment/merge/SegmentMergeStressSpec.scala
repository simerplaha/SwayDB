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
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.merge
//
//import scala.util.Random
//import swaydb.core.{TestBase, TestData}
//import swaydb.core.data._
//import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, KeyValueGroupingStrategyInternal}
//import swaydb.core.util.Benchmark
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//import swaydb.core.TestData._
//import swaydb.core.CommonAssertions._
//import swaydb.core.RunThis._
//import swaydb.core.IOAssert._
//
//class SegmentMergeStressSpec extends TestBase {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//
//  val keyValueCount = 100
//  val maxIteration = 5
//
//  /**
//    * Result: Merging of grouped key-values is expensive and should be deferred to lower levels.
//    *
//    * Deeply nested groups should be avoided. So [[GroupGroupingStrategyInternal]] should be avoided.
//    */
//  "Randomly merging overlapping key-values" should {
//    "be performant" in {
//      (1 to maxIteration).foldLeft(Slice.empty[KeyValue.ReadOnly]) {
//        case (oldKeyValues, index) =>
//          println(s"Iteration: $index/$maxIteration")
//
//          val newKeyValues = randomizedKeyValues(count = keyValueCount, startId = Some(index * 200))
//
//          val groupGroupingStrategy =
//            if (Random.nextBoolean())
//              Some(
//                GroupGroupingStrategyInternal.Count(
//                  count = randomInt(5) + 1,
//                  sortedIndexCompression = randomCompression(),
//                  valuesCompression = randomCompression()
//                )
//              )
//            else if (Random.nextBoolean())
//              Some(
//                GroupGroupingStrategyInternal.Size(
//                  size = randomInt(500) + 200,
//                  sortedIndexCompression = randomCompression(),
//                  valuesCompression = randomCompression()
//                )
//              )
//            else
//              None
//
//          implicit val compression =
//            if (Random.nextBoolean())
//              Some(
//                KeyValueGroupingStrategyInternal.Count(
//                  count = newKeyValues.size / randomInt(10) + 1,
//                  groupCompression = groupGroupingStrategy,
//                  sortedIndexCompression = randomCompression(),
//                  valuesCompression = randomCompression()
//                )
//              )
//            else if (Random.nextBoolean())
//              Some(
//                KeyValueGroupingStrategyInternal.Size(
//                  size = newKeyValues.last.stats.segmentSizeWithoutFooter / randomInt(10) + 1,
//                  groupCompression = groupGroupingStrategy,
//                  sortedIndexCompression = randomCompression(),
//                  valuesCompression = randomCompression()
//                )
//              )
//            else
//              None
//
//          val mergedKeyValues =
//            Benchmark("Merge performance") {
//              SegmentMerger.merge(
//                newKeyValues = newKeyValues,
//                oldKeyValues = oldKeyValues,
//                minSegmentSize = 100.mb,
//                maxProbe = TestData.maxProbe,
//                isLastLevel = false,
//                forInMemory = false,
//                bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
//                resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
//                minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeysForHashIndex,
//                allocateSpace = TestData.allocateSpace,
//                enableBinarySearchIndex = TestData.enableBinarySearchIndex,
//                buildFullBinarySearchIndex = TestData.buildFullBinarySearchIndex,
//                compressDuplicateValues = true
//              ).assertGet
//            }
//          mergedKeyValues should have size 1
//          val head = mergedKeyValues.head.toSlice
//          println(head.size)
//          head
//      }
//    }
//  }
//}
