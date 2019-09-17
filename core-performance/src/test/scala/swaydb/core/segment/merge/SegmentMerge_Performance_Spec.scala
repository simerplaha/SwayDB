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
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class SegmentMerge_Performance_Spec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val testTimer: TestTimer = TestTimer.Empty
  val keyValueCount = 100

  "performance" in {
    //        implicit val groupBy: Option[GroupByInternal.KeyValues] = Some(randomGroupBy(100, keyValueSize = Some(1.mb), groupByGroups = None))
    //        implicit val groupBy: Option[GroupByInternal.KeyValues] = Some(randomGroupBy(2, groupByGroups = Some(randomGroupByGroups(Int.MaxValue, size = Some(1.mb)))))
    //    implicit val groupBy: Option[GroupByInternal.KeyValues] = randomGroupByOption(10, keyValueSize = None, groupByGroups = None)
    implicit val groupBy: Option[GroupByInternal.KeyValues] = None

    val keyValues = randomKeyValues(100000)
    Benchmark(s"SegmentMerger performance. groupBy: ${groupBy.map(_.count)}:${groupBy.flatMap(_.size)}.bytes - groupByGroups: ${groupBy.flatMap(_.groupByGroups.map(_.count))}:${groupBy.flatMap(_.groupByGroups.flatMap(_.size))}.bytes") {
      SegmentMerger.merge(
        newKeyValues = keyValues,
        oldKeyValues = keyValues,
        minSegmentSize = 100.mb,
        isLastLevel = false,
        forInMemory = false,
        createdInLevel = randomIntMax(),
        valuesConfig = ValuesBlock.Config.disabled,
        sortedIndexConfig = SortedIndexBlock.Config.disabled,
        binarySearchIndexConfig = BinarySearchIndexBlock.Config.disabled,
        hashIndexConfig = HashIndexBlock.Config.disabled,
        bloomFilterConfig = BloomFilterBlock.Config.disabled,
        segmentIO = SegmentIO.defaultConcurrentStoredIfCompressed
      ).get
    }
  }
}
