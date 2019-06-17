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
import swaydb.core.IOAssert._
import swaydb.core.TestData._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class SegmentMerge_Performance_Spec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit def groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(10)

  val keyValueCount = 100

  "performance" in {
    val keyValues = randomKeyValues(100000)
    Benchmark("SegmentMerger performance") {
      SegmentMerger.merge(
        newKeyValues = keyValues,
        oldKeyValues = keyValues,
        minSegmentSize = 100.mb,
        maxProbe = TestData.maxProbe,
        isLastLevel = false,
        forInMemory = false,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
        resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
        minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
        enableRangeFilter = TestData.enableRangeFilter,
        hashIndexCompensation = TestData.hashIndexCompensation,
        compressDuplicateValues = true
      ).assertGet
    }
  }
}
