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
//
//package swaydb.core.merge
//
//import swaydb.core.TestBase
//import swaydb.core.TestData._
//import swaydb.core.data._
//import swaydb.core.segment.block._
//import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
//import swaydb.core.segment.block.hashindex.HashIndexBlock
//import swaydb.core.util.Benchmark
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//
//class SegmentMergeStressSpec extends TestBase {
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//
//  val keyValueCount = 1000
//  val maxIteration = 10
//
//  /**
//   * Result: Merging of grouped key-values is expensive and should be deferred to lower levels.
//   *
//   * Deeply nested groups should be avoided. So [[GroupByInternal.Groups]] should be avoided.
//   */
//  "Randomly merging overlapping key-values" should {
//    "be performant" in {
//      (1 to maxIteration).foldLeft(Slice.empty[KeyValue]) {
//        case (oldKeyValues, index) =>
//          println(s"Iteration: $index/$maxIteration")
//
//          val newKeyValues = randomizedKeyValues(count = keyValueCount, startId = Some(index * 200))
//
//          val mergedKeyValues =
//            Benchmark("Merge performance") {
//              SegmentMerger.merge(
//                newKeyValues = newKeyValues,
//                oldKeyValues = oldKeyValues,
//                minSegmentSize = 100.mb,
//                isLastLevel = false,
//                forInMemory = false,
//                createdInLevel = randomIntMax(),
//                valuesConfig = ValuesBlock.Config.random,
//                sortedIndexConfig = SortedIndexBlock.Config.random,
//                binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
//                hashIndexConfig = HashIndexBlock.Config.random,
//                bloomFilterConfig = BloomFilterBlock.Config.random
//              )
//            }
//
//          mergedKeyValues should have size 1
//          val head = mergedKeyValues.head.toSlice
//          println("Merged key-values: " + head.size)
//          head
//      }
//    }
//  }
//}
