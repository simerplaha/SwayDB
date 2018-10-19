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

package swaydb.core.segment.merge

import swaydb.core.TestBase
import swaydb.core.data._
import swaydb.core.group.compression.data.{GroupGroupingStrategyInternal, KeyValueGroupingStrategyInternal}
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder

import scala.concurrent.duration._
import scala.util.Random

class SegmentMergeStressSpec extends TestBase {

  override implicit val ordering = KeyOrder.default

  val keyValueCount = 100
  val maxIteration = 5

  /**
    * Result: Merging of grouped key-values is expensive and should be deferred to lower levels.
    *
    * Deeply nested groups should be avoided. So [[GroupGroupingStrategyInternal]] should be avoided.
    */
  "Randomly merging overlapping key-values" should {
    "be performant" in {
      (1 to maxIteration).foldLeft(Slice.empty[KeyValue.ReadOnly]) {
        case (oldKeyValues, index) =>
          println(s"Iteration: $index/$maxIteration")

          val newKeyValues = randomizedIntKeyValues(count = keyValueCount, startId = Some(index * 200), addRandomGroups = Random.nextBoolean())

          val groupGroupingStrategy =
            if (Random.nextBoolean())
              Some(
                GroupGroupingStrategyInternal.Count(
                  count = randomInt(5) + 1,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              )
            else if (Random.nextBoolean())
              Some(
                GroupGroupingStrategyInternal.Size(
                  size = randomInt(500) + 200,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              )
            else
              None

          implicit val compression =
            if (Random.nextBoolean())
              Some(
                KeyValueGroupingStrategyInternal.Count(
                  count = newKeyValues.size / randomInt(10) + 1,
                  groupCompression = groupGroupingStrategy,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              )
            else if (Random.nextBoolean())
              Some(
                KeyValueGroupingStrategyInternal.Size(
                  size = newKeyValues.last.stats.segmentSizeWithoutFooter / randomInt(10) + 1,
                  groupCompression = groupGroupingStrategy,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              )
            else
              None

          val mergedKeyValues =
            Benchmark("Merge performance") {
              SegmentMerger.merge(newKeyValues, oldKeyValues, 100.mb, false, false, 0.1, 0.seconds, compressDuplicateValues = true).assertGet
            }
          mergedKeyValues should have size 1
          val head = mergedKeyValues.head.toSlice
          println(head.size)
          head
      }
    }
  }
}