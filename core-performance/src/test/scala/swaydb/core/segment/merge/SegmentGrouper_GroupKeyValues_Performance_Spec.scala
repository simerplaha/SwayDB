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

import swaydb.core.{TestBase, TestData}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.util.Benchmark
import scala.collection.mutable.ListBuffer
import swaydb.core.data.KeyValue
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._

class SegmentGrouper_GroupKeyValues_Performance_Count_Spec extends SegmentGrouper_GroupKeyValues_Performance_Spec {
  val useCount = true
  val force = false
}

class SegmentGrouper_GroupKeyValues_Performance_Size_Spec extends SegmentGrouper_GroupKeyValues_Performance_Spec {
  val useCount = false
  val force = false
}

class SegmentGrouper_GroupKeyValues_Performance_Count_Force_Spec extends SegmentGrouper_GroupKeyValues_Performance_Spec {
  val useCount = true
  val force = true
}

class SegmentGrouper_GroupKeyValues_Performance_Size_Force_Spec extends SegmentGrouper_GroupKeyValues_Performance_Spec {
  val useCount = false
  val force = true
}

sealed trait SegmentGrouper_GroupKeyValues_Performance_Spec extends TestBase {

  def force: Boolean

  def useCount: Boolean

  "groupKeyValues" should {
    "benchmark" when {
      "there are not enough key-values" in {
        val keyValues = randomizedKeyValues(1000000, addRandomGroups = false)
        val mutableKeyValues = ListBuffer.empty[KeyValue.WriteOnly]
        keyValues foreach (mutableKeyValues += _)

        Benchmark("there are not enough key-values") {
          SegmentGrouper.groupKeyValues(
            segmentKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
            minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeysForHashIndex,
            hashIndexCompensation = TestData.hashIndexCompensation,
            enableBinarySearchIndex = TestData.enableBinarySearchIndex,
            buildFullBinarySearchIndex = TestData.buildFullBinarySearchIndex,
            maxProbe = TestData.maxProbe,
            force = force,
            groupingStrategy =
              if (useCount)
                KeyValueGroupingStrategyInternal.Count(
                  count = keyValues.size + 10,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              else
                KeyValueGroupingStrategyInternal.Size(
                  size = keyValues.last.stats.segmentSizeWithoutFooter + 1,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
          )
        }
      }
    }

    "benchmark" when {
      "there are enough key-values" in {
        val keyValues = randomizedKeyValues(1000000, addRandomGroups = false)
        val mutableKeyValues = ListBuffer.empty[KeyValue.WriteOnly]
        keyValues foreach (mutableKeyValues += _)

        Benchmark("there are enough key-values") {
          SegmentGrouper.groupKeyValues(
            segmentKeyValues = mutableKeyValues,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
            resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
            minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeysForHashIndex,
            hashIndexCompensation = TestData.hashIndexCompensation,
            enableBinarySearchIndex = TestData.enableBinarySearchIndex,
            buildFullBinarySearchIndex = TestData.buildFullBinarySearchIndex,
            maxProbe = TestData.maxProbe,
            force = force,
            groupingStrategy =
              if (useCount)
                KeyValueGroupingStrategyInternal.Count(
                  count = keyValues.size / 10,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
              else
                KeyValueGroupingStrategyInternal.Size(
                  size = keyValues.last.stats.segmentSizeWithoutFooter - 100,
                  groupCompression = None,
                  indexCompression = randomCompression(),
                  valueCompression = randomCompression()
                )
          )
        }
      }
    }
  }
}
