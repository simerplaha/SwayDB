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

import swaydb.compression.CompressionInternal

private[swaydb] sealed trait GroupingStrategy {
  val indexCompressions: Seq[CompressionInternal]
  val valueCompressions: Seq[CompressionInternal]
}

private[swaydb] sealed trait KeyValueGroupingStrategyInternal extends GroupingStrategy {
  val groupCompression: Option[GroupGroupingStrategyInternal]
}

private[swaydb] object KeyValueGroupingStrategyInternal {

  val none = Option.empty[KeyValueGroupingStrategyInternal]

  /**
    * Converts public type KeyValueGroupingStrategy to internal KeyValueGroupingStrategy which the core understands.
    */
  def apply(groupingStrategy: swaydb.data.api.grouping.KeyValueGroupingStrategy): KeyValueGroupingStrategyInternal =
    groupingStrategy match {
      case swaydb.data.api.grouping.KeyValueGroupingStrategy.Count(count, indexCompressions, valueCompressions, groupGroupingStrategy) =>
        KeyValueGroupingStrategyInternal.Count(
          count = count,
          groupCompression = groupGroupingStrategy map GroupGroupingStrategyInternal.apply,
          indexCompressions = indexCompressions map CompressionInternal.apply,
          valueCompressions = valueCompressions map CompressionInternal.apply
        )
      case swaydb.data.api.grouping.KeyValueGroupingStrategy.Size(size, indexCompressions, valueCompressions, groupGroupingStrategy) =>
        KeyValueGroupingStrategyInternal.Size(
          size = size,
          groupCompression = groupGroupingStrategy map GroupGroupingStrategyInternal.apply,
          indexCompressions = indexCompressions map CompressionInternal.apply,
          valueCompressions = valueCompressions map CompressionInternal.apply
        )
    }

  object Count {
    def apply(count: Int,
              groupCompression: Option[GroupGroupingStrategyInternal],
              indexCompression: CompressionInternal,
              valueCompression: CompressionInternal): Count =
      new Count(
        count = count,
        groupCompression = groupCompression,
        indexCompressions = Seq(indexCompression),
        valueCompressions = Seq(valueCompression)
      )
  }

  case class Count(count: Int,
                   groupCompression: Option[GroupGroupingStrategyInternal],
                   indexCompressions: Seq[CompressionInternal],
                   valueCompressions: Seq[CompressionInternal]) extends KeyValueGroupingStrategyInternal

  object Size {
    def apply(size: Int,
              groupCompression: Option[GroupGroupingStrategyInternal],
              indexCompression: CompressionInternal,
              valueCompression: CompressionInternal): Size =
      new Size(
        size = size,
        groupCompression = groupCompression,
        indexCompressions = Seq(indexCompression),
        valueCompressions = Seq(valueCompression)
      )
  }

  case class Size(size: Int,
                  groupCompression: Option[GroupGroupingStrategyInternal],
                  indexCompressions: Seq[CompressionInternal],
                  valueCompressions: Seq[CompressionInternal]) extends KeyValueGroupingStrategyInternal
}

private[swaydb] sealed trait GroupGroupingStrategyInternal extends GroupingStrategy

private[swaydb] object GroupGroupingStrategyInternal {

  /**
    * Converts public type GroupGroupingStrategy to internal GroupGroupingStrategy which the core understands.
    */
  def apply(groupingStrategy: swaydb.data.api.grouping.GroupGroupingStrategy): GroupGroupingStrategyInternal =
    groupingStrategy match {
      case swaydb.data.api.grouping.GroupGroupingStrategy.Count(count, indexCompressions, valueCompressions) =>
        GroupGroupingStrategyInternal.Count(
          count = count,
          indexCompressions = indexCompressions map CompressionInternal.apply,
          valueCompressions = valueCompressions map CompressionInternal.apply
        )
      case swaydb.data.api.grouping.GroupGroupingStrategy.Size(size, indexCompressions, valueCompressions) =>
        GroupGroupingStrategyInternal.Size(
          size = size,
          indexCompressions = indexCompressions map CompressionInternal.apply,
          valueCompressions = valueCompressions map CompressionInternal.apply
        )
    }

  object Count {
    def apply(count: Int,
              indexCompression: CompressionInternal,
              valueCompression: CompressionInternal): Count =
      new Count(
        count = count,
        indexCompressions = Seq(indexCompression),
        valueCompressions = Seq(valueCompression)
      )
  }

  case class Count(count: Int,
                   indexCompressions: Seq[CompressionInternal],
                   valueCompressions: Seq[CompressionInternal]) extends GroupGroupingStrategyInternal

  object Size {
    def apply(size: Int,
              indexCompression: CompressionInternal,
              valueCompression: CompressionInternal): Size =
      new Size(
        size = size,
        indexCompressions = Seq(indexCompression),
        valueCompressions = Seq(valueCompression)
      )
  }

  case class Size(size: Int,
                  indexCompressions: Seq[CompressionInternal],
                  valueCompressions: Seq[CompressionInternal]) extends GroupGroupingStrategyInternal
}