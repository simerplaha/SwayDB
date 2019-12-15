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

import swaydb.core.TestData._
import swaydb.core.data.{Time, Transient}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.{BinarySearchEntryFormat, BinarySearchIndexBlock}
import swaydb.core.segment.format.a.block.hashindex.{HashIndexBlock, HashIndexEntryFormat}
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.config.IOStrategy
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class SegmentMerge_Performance_Spec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val testTimer: TestTimer = TestTimer.Empty
  val keyValueCount = 100

  "performance" in {
    val sortedIndexConfig =
      SortedIndexBlock.Config.disabled
    //      SortedIndexBlock.Config(
    //        ioStrategy = _ => IOStrategy.ConcurrentIO(cacheOnAccess = false),
    //        prefixCompressionResetCount = 0,
    //        enableAccessPositionIndex = true,
    //        normaliseIndex = false,
    //        compressions = _ => Seq.empty
    //      )

    val binarySearchIndexConfig =
      BinarySearchIndexBlock.Config.disabled
    //      BinarySearchIndexBlock.Config(
    //        enabled = true,
    //        format = BinarySearchEntryFormat.CopyKey,
    //        minimumNumberOfKeys = 1,
    //        searchSortedIndexDirectlyIfPossible = false,
    //        fullIndex = true,
    //        ioStrategy = _ => IOStrategy.ConcurrentIO(cacheOnAccess = false),
    //        compressions = _ => Seq.empty
    //      )

    val valuesConfig =
          ValuesBlock.Config.disabled
//      ValuesBlock.Config(
//        compressDuplicateValues = true,
//        compressDuplicateRangeValues = true,
//        ioStrategy = _ => IOStrategy.ConcurrentIO(cacheOnAccess = false),
//        compressions = _ => Seq.empty
//      )

    val hashIndexConfig =
      HashIndexBlock.Config.disabled
    //      HashIndexBlock.Config(
    //        maxProbe = 1,
    //        format = HashIndexEntryFormat.Reference,
    //        minimumNumberOfKeys = 5,
    //        minimumNumberOfHits = 5,
    //        allocateSpace = _.requiredSpace,
    //        ioStrategy = _ => IOStrategy.ConcurrentIO(cacheOnAccess = false),
    //        compressions = _ => Seq.empty
    //      )

    val bloomFilterConfig =
      BloomFilterBlock.Config.disabled
    //      BloomFilterBlock.Config(
    //        falsePositiveRate = 0.001,
    //        minimumNumberOfKeys = 2,
    //        optimalMaxProbe = _ => 1,
    //        ioStrategy = _ => IOStrategy.SynchronisedIO(cacheOnAccess = true),
    //        compressions = _ => Seq.empty
    //      )

    val keys = (1 to 1000000).map(Slice.writeInt)
    val keyValues = Slice.create[Transient.Put](keys.size)

    Benchmark("Creating key-values") {
      keys.foldLeft(Option.empty[Transient.Put]) {
        case (previous, key) =>
          val put =
            Transient.Put(
              key = key,
              normaliseToSize = None,
              value = Some(key),
              deadline = None,
              time = Time.empty,
              previous = previous,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              valuesConfig = valuesConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig
            )

          keyValues add put

          Some(put)
      }
    }
//    Benchmark(s"SegmentMerger performance") {
//      SegmentMerger.merge(
//        newKeyValues = keyValues,
//        oldKeyValues = keyValues,
//        minSegmentSize = 100.mb,
//        isLastLevel = false,
//        forInMemory = false,
//        createdInLevel = randomIntMax(),
//        sortedIndexConfig = sortedIndexConfig,
//        binarySearchIndexConfig = binarySearchIndexConfig,
//        valuesConfig = valuesConfig,
//        hashIndexConfig = hashIndexConfig,
//        bloomFilterConfig = bloomFilterConfig
//      )
//    }
  }
}
