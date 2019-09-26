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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.merge

import swaydb.core.CommonAssertions._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.util.Benchmark
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class SegmentMergeStressSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  val keyValueCount = 1000
  val maxIteration = 10

  /**
   * Result: Merging of grouped key-values is expensive and should be deferred to lower levels.
   *
   * Deeply nested groups should be avoided. So [[GroupByInternal.Groups]] should be avoided.
   */
  "Randomly merging overlapping key-values" should {
    "be performant" in {
      (1 to maxIteration).foldLeft(Slice.empty[KeyValue.ReadOnly]) {
        case (oldKeyValues, index) =>
          println(s"Iteration: $index/$maxIteration")

          val newKeyValues = randomizedKeyValues(count = keyValueCount, startId = Some(index * 200))

          val mergedKeyValues =
            Benchmark("Merge performance") {
              SegmentMerger.merge(
                newKeyValues = newKeyValues,
                oldKeyValues = oldKeyValues,
                minSegmentSize = 100.mb,
                isLastLevel = false,
                forInMemory = false,
                createdInLevel = randomIntMax(),
                valuesConfig = ValuesBlock.Config.random,
                sortedIndexConfig = SortedIndexBlock.Config.random,
                binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
                hashIndexConfig = HashIndexBlock.Config.random,
                bloomFilterConfig = BloomFilterBlock.Config.random
              )
            }

          mergedKeyValues should have size 1
          val head = mergedKeyValues.head.toSlice
          println("Merged key-values: " + head.size)
          head
      }
    }
  }
}
