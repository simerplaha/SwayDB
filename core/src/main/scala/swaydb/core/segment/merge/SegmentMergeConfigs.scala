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

import swaydb.core.group.compression.GroupByInternal
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.{BloomFilterBlock, HashIndexBlock, SortedIndexBlock, ValuesBlock}

object SegmentMergeConfigs {
  def apply( //segment related configs
             segmentValuesConfig: ValuesBlock.Config,
             segmentSortedIndexConfig: SortedIndexBlock.Config,
             segmentBinarySearchIndexConfig: BinarySearchIndexBlock.Config,
             segmentHashIndexConfig: HashIndexBlock.Config,
             segmentBloomFilterConfig: BloomFilterBlock.Config,
             //group related configs
             groupBy: Option[GroupByInternal.KeyValues]): SegmentMergeConfigs =
    new SegmentMergeConfigs(
      //segment related configs
      segmentValuesConfig = segmentValuesConfig,
      segmentSortedIndexConfig = segmentSortedIndexConfig,
      segmentBinarySearchIndexConfig = segmentBinarySearchIndexConfig,
      segmentHashIndexConfig = segmentHashIndexConfig,
      segmentBloomFilterConfig = segmentBloomFilterConfig,
      //group related configs. Group grouping is not specified use segment's config.
      groupValuesConfig = groupBy.map(_.valuesConfig) getOrElse segmentValuesConfig,
      groupSortedIndexConfig = groupBy.map(_.sortedIndexConfig) getOrElse segmentSortedIndexConfig,
      groupBinarySearchIndexConfig = groupBy.map(_.binarySearchIndexConfig) getOrElse segmentBinarySearchIndexConfig,
      groupHashIndexConfig = groupBy.map(_.hashIndexConfig) getOrElse segmentHashIndexConfig,
      groupBloomFilterConfig = groupBy.map(_.bloomFilterConfig) getOrElse segmentBloomFilterConfig
    )
}

case class SegmentMergeConfigs( //segment related configs
                                segmentValuesConfig: ValuesBlock.Config,
                                segmentSortedIndexConfig: SortedIndexBlock.Config,
                                segmentBinarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                segmentHashIndexConfig: HashIndexBlock.Config,
                                segmentBloomFilterConfig: BloomFilterBlock.Config,
                                //group related configs
                                groupValuesConfig: ValuesBlock.Config,
                                groupSortedIndexConfig: SortedIndexBlock.Config,
                                groupBinarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                groupHashIndexConfig: HashIndexBlock.Config,
                                groupBloomFilterConfig: BloomFilterBlock.Config)