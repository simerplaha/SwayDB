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

package swaydb.core.segment.format.a.block

import swaydb.core.TestData._
import swaydb.core.actor.MemorySweeper
import swaydb.core.io.file.BlockCache
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.config.IOStrategy
import swaydb.data.slice.Slice

class GroupPerformanceSpec extends TestBase {

  "test" in {
    implicit val blockCache: Option[BlockCache.State] = None
    implicit val memorySweeper: Option[MemorySweeper.KeyValue] = None

    val keyValues =
      randomKeyValues(
        count = 1000000,
        startId = Some(1),
        sortedIndexConfig =
          SortedIndexBlock.Config(
            ioStrategy = _ => IOStrategy.SynchronisedIO(cacheOnAccess = false),
            prefixCompressionResetCount = 0,
            enableAccessPositionIndex = true,
            enablePartialRead = true,
            disableKeyPrefixCompression = false,
            normaliseIndex = false,
            compressions = _ => Seq.empty
          ),
        binarySearchIndexConfig =
          BinarySearchIndexBlock.Config(
            enabled = true,
            minimumNumberOfKeys = 1,
            searchSortedIndexDirectlyIfPossible = true,
            fullIndex = true,
            blockIO = _ => IOStrategy.ConcurrentIO(false),
            compressions = _ => Seq.empty
          ),
        valuesConfig =
          ValuesBlock.Config(
            compressDuplicateValues = true,
            compressDuplicateRangeValues = true,
            blockIO = _ => IOStrategy.ConcurrentIO(false),
            compressions = _ => Seq.empty
          ),
        hashIndexConfig =
          HashIndexBlock.Config(
            maxProbe = 2,
            copyIndex = true,
            minimumNumberOfKeys = 5,
            minimumNumberOfHits = 5,
            allocateSpace = _.requiredSpace * 10,
            blockIO = _ => IOStrategy.SynchronisedIO(cacheOnAccess = false),
            compressions = _ => Seq.empty
          ),
        //      hashIndexConfig = HashIndexBlock.Config.disabled,
        bloomFilterConfig =
          BloomFilterBlock.Config.disabled
        //        BloomFilterBlock.Config(
        //          falsePositiveRate = 0.001,
        //          minimumNumberOfKeys = 2,
        //          optimalMaxProbe = _ => 1,
        //          blockIO = _ => IOStrategy.SynchronisedIO(cacheOnAccess = true),
        //          compressions = _ => Seq.empty
        //        )
      )
    val group =
      randomGroup(
        keyValues,
        sortedIndexConfig =
          SortedIndexBlock.Config(
            ioStrategy = _ => IOStrategy.SynchronisedIO(cacheOnAccess = false),
            prefixCompressionResetCount = 0,
            enableAccessPositionIndex = true,
            enablePartialRead = true,
            disableKeyPrefixCompression = true,
            normaliseIndex = true,
            compressions = _ => Seq.empty
          ),
        binarySearchIndexConfig =
          BinarySearchIndexBlock.Config(
            enabled = true,
            minimumNumberOfKeys = 1,
            searchSortedIndexDirectlyIfPossible = true,
            fullIndex = true,
            blockIO = _ => IOStrategy.ConcurrentIO(false),
            compressions = _ => Seq.empty
          ),
        valuesConfig =
          ValuesBlock.Config(
            compressDuplicateValues = true,
            compressDuplicateRangeValues = true,
            blockIO = _ => IOStrategy.ConcurrentIO(false),
            compressions = _ => Seq.empty
          ),
        hashIndexConfig =
          HashIndexBlock.Config(
            maxProbe = 2,
            copyIndex = true,
            minimumNumberOfKeys = 5,
            minimumNumberOfHits = 5,
            allocateSpace = _.requiredSpace * 10,
            blockIO = _ => IOStrategy.SynchronisedIO(cacheOnAccess = false),
            compressions = _ => Seq.empty
          ),
        //      hashIndexConfig = HashIndexBlock.Config.disabled,
        bloomFilterConfig =
          BloomFilterBlock.Config.disabled
        //        BloomFilterBlock.Config(
        //          falsePositiveRate = 0.001,
        //          minimumNumberOfKeys = 2,
        //          optimalMaxProbe = _ => 1,
        //          blockIO = _ => IOStrategy.SynchronisedIO(cacheOnAccess = true),
        //          compressions = _ => Seq.empty
        //        )
      )

    val segment =
      Benchmark("creating segment") {
                TestSegment(Slice(group)).get
//        TestSegment(keyValues).get
      }

    Benchmark("Read performance") {
      keyValues foreach {
        keyValue =>
          segment.get(keyValue.key).get
      }
    }
  }
}
