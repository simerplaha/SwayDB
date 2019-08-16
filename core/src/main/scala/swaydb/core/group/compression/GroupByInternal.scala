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

package swaydb.core.group.compression

import swaydb.core.segment.format.a.block._

private[swaydb] sealed trait GroupByInternal {
  def count: Int
  def size: Option[Int]
  def applyGroupingOnCopy: Boolean
  def bloomFilterConfig: BloomFilterBlock.Config
  def hashIndexConfig: HashIndexBlock.Config
  def binarySearchIndexConfig: BinarySearchIndexBlock.Config
  def sortedIndexConfig: SortedIndexBlock.Config
  def valuesConfig: ValuesBlock.Config
  def groupConfig: SegmentBlock.Config
  def groupIO: SegmentIO =
    SegmentIO(
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = groupConfig
    )
}

private[swaydb] object GroupByInternal {

  val none = Option.empty[GroupByInternal]

  /**
    * Converts public type KeyValueGroupBy to internal KeyValueGroupBy which the core understands.
    */
  def apply(groupBy: swaydb.data.api.grouping.GroupBy.KeyValues): GroupByInternal.KeyValues =
    groupBy match {
      case grouping: swaydb.data.api.grouping.GroupBy.KeyValues =>
        GroupByInternal.KeyValues(
          count = grouping.count,
          size = grouping.size,
          applyGroupingOnCopy = grouping.applyGroupingOnCopy,
          groupByGroups = grouping.groupGroupBy map (GroupByInternal.apply(grouping.applyGroupingOnCopy, _)),
          bloomFilterConfig = BloomFilterBlock.Config(grouping.bloomFilter),
          hashIndexConfig = HashIndexBlock.Config(grouping.hashIndex),
          binarySearchIndexConfig = BinarySearchIndexBlock.Config(grouping.binarySearchIndex),
          sortedIndexConfig = SortedIndexBlock.Config(grouping.sortedIndex),
          valuesConfig = ValuesBlock.Config(grouping.values),
          groupConfig = SegmentBlock.Config(grouping.groupIO, None, grouping.groupCompressions)
        )
    }

  def apply(applyGroupingOnCopy: Boolean, grouping: swaydb.data.api.grouping.GroupBy.Groups): GroupByInternal.Groups =
    GroupByInternal.Groups(
      count = grouping.count,
      size = grouping.size,
      bloomFilterConfig = BloomFilterBlock.Config(grouping.bloomFilter),
      applyGroupingOnCopy = applyGroupingOnCopy,
      hashIndexConfig = HashIndexBlock.Config(grouping.hashIndex),
      binarySearchIndexConfig = BinarySearchIndexBlock.Config(grouping.binarySearchIndex),
      sortedIndexConfig = SortedIndexBlock.Config(grouping.sortedIndex),
      valuesConfig = ValuesBlock.Config(grouping.values),
      groupConfig = SegmentBlock.Config(grouping.groupIO, None, grouping.groupCompressions)
    )

  case class KeyValues(count: Int,
                       size: Option[Int],
                       applyGroupingOnCopy: Boolean,
                       groupByGroups: Option[GroupByInternal.Groups],
                       bloomFilterConfig: BloomFilterBlock.Config,
                       hashIndexConfig: HashIndexBlock.Config,
                       binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                       sortedIndexConfig: SortedIndexBlock.Config,
                       valuesConfig: ValuesBlock.Config,
                       groupConfig: SegmentBlock.Config) extends GroupByInternal

  case class Groups(count: Int,
                    size: Option[Int],
                    applyGroupingOnCopy: Boolean,
                    bloomFilterConfig: BloomFilterBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    valuesConfig: ValuesBlock.Config,
                    groupConfig: SegmentBlock.Config) extends GroupByInternal
}