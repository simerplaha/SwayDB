/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.merge

import swaydb.core.data.Memory
import swaydb.core.merge.MergeStats.Persistent
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock

import scala.collection.mutable.ListBuffer

sealed trait MergeStatsSizeCalculator[S >: Null] {
  def isStatsOrNullSmall(statsOrNull: S)(implicit segmentConfig: SegmentBlock.Config): Boolean
}

object MergeStatsSizeCalculator {

  implicit def persistentSizeCalculator(implicit sortedIndexConfig: SortedIndexBlock.Config): PersistentSizeCalculator =
    new PersistentSizeCalculator()

  class PersistentSizeCalculator()(implicit sortedIndexConfig: SortedIndexBlock.Config) extends MergeStatsSizeCalculator[MergeStats.Persistent.Builder[Memory, ListBuffer]] {

    override def isStatsOrNullSmall(statsOrNull: Persistent.Builder[Memory, ListBuffer])(implicit segmentConfig: SegmentBlock.Config): Boolean = {
      val mergeStats =
        statsOrNull.close(
          hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
          optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
        )

      mergeStats.keyValuesCount < segmentConfig.maxCount && mergeStats.maxSortedIndexSize + statsOrNull.totalValuesSize < segmentConfig.minSize / 2
    }
  }

  /**
   * Create [[MergeStats]] instance for memory [[swaydb.core.level.Level]]
   */
  implicit case object MemoryCreator extends MergeStatsSizeCalculator[MergeStats.Memory.Builder[Memory, ListBuffer]] {

    override def isStatsOrNullSmall(statsOrNull: MergeStats.Memory.Builder[Memory, ListBuffer])(implicit segmentConfig: SegmentBlock.Config): Boolean =
      statsOrNull.keyValueCount < segmentConfig.maxCount && statsOrNull.segmentSize < segmentConfig.minSize

  }
}
