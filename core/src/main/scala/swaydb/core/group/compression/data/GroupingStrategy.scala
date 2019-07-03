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

package swaydb.core.group.compression.data

import swaydb.core.segment.format.a.block.{BlocksCompression, _}

private[swaydb] sealed trait GroupingStrategy {
  def bloomFilterConfig: BloomFilter.Config
  def hashIndexConfig: HashIndex.Config
  def binarySearchIndexConfig: BinarySearchIndex.Config
  def sortedIndexConfig: SortedIndex.Config
  def valuesConfig: Values.Config
  def blockCompressions: BlocksCompression
}

private[swaydb] sealed trait KeyValueGroupingStrategyInternal extends GroupingStrategy {
  def groupCompression: Option[GroupGroupingStrategyInternal]
  def applyGroupingOnCopy: Boolean
  def bloomFilterConfig: BloomFilter.Config
  def hashIndexConfig: HashIndex.Config
  def binarySearchIndexConfig: BinarySearchIndex.Config
  def sortedIndexConfig: SortedIndex.Config
  def valuesConfig: Values.Config
  def blockCompressions: BlocksCompression
}

private[swaydb] object KeyValueGroupingStrategyInternal {

  val none = Option.empty[KeyValueGroupingStrategyInternal]

  /**
    * Converts public type KeyValueGroupingStrategy to internal KeyValueGroupingStrategy which the core understands.
    */
  def apply(groupingStrategy: swaydb.data.api.grouping.KeyValueGroupingStrategy): KeyValueGroupingStrategyInternal =
    groupingStrategy match {
      case grouping: swaydb.data.api.grouping.KeyValueGroupingStrategy.Count =>
        KeyValueGroupingStrategyInternal.Count(
          count = grouping.count,
          applyGroupingOnCopy = grouping.applyGroupingOnCopy,
          groupCompression = grouping.groupGroupingStrategy map GroupGroupingStrategyInternal.apply,
          bloomFilterConfig = BloomFilter.Config(grouping.bloomFilter),
          hashIndexConfig = HashIndex.Config(grouping.hashIndex),
          binarySearchIndexConfig = BinarySearchIndex.Config(grouping.binarySearchIndex),
          sortedIndexConfig = SortedIndex.Config(grouping.sortedIndex),
          valuesConfig = Values.Config(grouping.values),
          blockCompressions =
            BlocksCompression(
              bloomFilter = grouping.bloomFilter,
              hashIndex = grouping.hashIndex,
              binarySearchIndex = grouping.binarySearchIndex,
              sortedIndex = grouping.sortedIndex,
              values = grouping.values,
              segment = grouping.groupCompression
            )
        )

      case grouping: swaydb.data.api.grouping.KeyValueGroupingStrategy.Size =>
        KeyValueGroupingStrategyInternal.Size(
          size = grouping.size,
          applyGroupingOnCopy = grouping.applyGroupingOnCopy,
          groupCompression = grouping.groupGroupingStrategy map GroupGroupingStrategyInternal.apply,
          bloomFilterConfig = BloomFilter.Config(grouping.bloomFilter),
          hashIndexConfig = HashIndex.Config(grouping.hashIndex),
          binarySearchIndexConfig = BinarySearchIndex.Config(grouping.binarySearchIndex),
          sortedIndexConfig = SortedIndex.Config(grouping.sortedIndex),
          valuesConfig = Values.Config(grouping.values),
          blockCompressions =
            BlocksCompression(
              bloomFilter = grouping.bloomFilter,
              hashIndex = grouping.hashIndex,
              binarySearchIndex = grouping.binarySearchIndex,
              sortedIndex = grouping.sortedIndex,
              values = grouping.values,
              segment = grouping.groupCompression
            )
        )
    }

  case class Count(count: Int,
                   applyGroupingOnCopy: Boolean,
                   groupCompression: Option[GroupGroupingStrategyInternal],
                   bloomFilterConfig: BloomFilter.Config,
                   hashIndexConfig: HashIndex.Config,
                   binarySearchIndexConfig: BinarySearchIndex.Config,
                   sortedIndexConfig: SortedIndex.Config,
                   valuesConfig: Values.Config,
                   blockCompressions: BlocksCompression) extends KeyValueGroupingStrategyInternal

  case class Size(size: Int,
                  applyGroupingOnCopy: Boolean,
                  groupCompression: Option[GroupGroupingStrategyInternal],
                  bloomFilterConfig: BloomFilter.Config,
                  hashIndexConfig: HashIndex.Config,
                  binarySearchIndexConfig: BinarySearchIndex.Config,
                  sortedIndexConfig: SortedIndex.Config,
                  valuesConfig: Values.Config,
                  blockCompressions: BlocksCompression) extends KeyValueGroupingStrategyInternal
}

private[swaydb] sealed trait GroupGroupingStrategyInternal extends GroupingStrategy

private[swaydb] object GroupGroupingStrategyInternal {

  /**
    * Converts public type GroupGroupingStrategy to internal GroupGroupingStrategy which the core understands.
    */
  def apply(groupingStrategy: swaydb.data.api.grouping.GroupGroupingStrategy): GroupGroupingStrategyInternal =
    groupingStrategy match {
      case grouping: swaydb.data.api.grouping.GroupGroupingStrategy.Count =>
        GroupGroupingStrategyInternal.Count(
          count = grouping.count,
          bloomFilterConfig = BloomFilter.Config(grouping.bloomFilter),
          hashIndexConfig = HashIndex.Config(grouping.hashIndex),
          binarySearchIndexConfig = BinarySearchIndex.Config(grouping.binarySearchIndex),
          sortedIndexConfig = SortedIndex.Config(grouping.sortedIndex),
          valuesConfig = Values.Config(grouping.values),
          blockCompressions =
            BlocksCompression(
              bloomFilter = grouping.bloomFilter,
              hashIndex = grouping.hashIndex,
              binarySearchIndex = grouping.binarySearchIndex,
              sortedIndex = grouping.sortedIndex,
              values = grouping.values,
              segment = grouping.groupsCompression
            )
        )
      case grouping: swaydb.data.api.grouping.GroupGroupingStrategy.Size =>
        GroupGroupingStrategyInternal.Size(
          size = grouping.size,
          bloomFilterConfig = BloomFilter.Config(grouping.bloomFilter),
          hashIndexConfig = HashIndex.Config(grouping.hashIndex),
          binarySearchIndexConfig = BinarySearchIndex.Config(grouping.binarySearchIndex),
          sortedIndexConfig = SortedIndex.Config(grouping.sortedIndex),
          valuesConfig = Values.Config(grouping.values),
          blockCompressions =
            BlocksCompression(
              bloomFilter = grouping.bloomFilter,
              hashIndex = grouping.hashIndex,
              binarySearchIndex = grouping.binarySearchIndex,
              sortedIndex = grouping.sortedIndex,
              values = grouping.values,
              segment = grouping.groupsCompression
            )
        )
    }

  case class Count(count: Int,
                   bloomFilterConfig: BloomFilter.Config,
                   hashIndexConfig: HashIndex.Config,
                   binarySearchIndexConfig: BinarySearchIndex.Config,
                   sortedIndexConfig: SortedIndex.Config,
                   valuesConfig: Values.Config,
                   blockCompressions: BlocksCompression) extends GroupGroupingStrategyInternal

  case class Size(size: Int,
                  bloomFilterConfig: BloomFilter.Config,
                  hashIndexConfig: HashIndex.Config,
                  binarySearchIndexConfig: BinarySearchIndex.Config,
                  sortedIndexConfig: SortedIndex.Config,
                  valuesConfig: Values.Config,
                  blockCompressions: BlocksCompression) extends GroupGroupingStrategyInternal
}