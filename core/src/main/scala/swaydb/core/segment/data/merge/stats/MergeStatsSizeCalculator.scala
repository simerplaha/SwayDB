/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.data.merge.stats

import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.data.Memory

import scala.collection.mutable.ListBuffer

/**
 * Calculates if the [[MergeStats.Segment]] is small. If yes the Segment is expanded and merged
 * with other Segments.
 */
sealed trait MergeStatsSizeCalculator[S >: Null] {
  def isStatsOrNullSmall(statsOrNull: S)(implicit segmentConfig: SegmentBlockConfig): Boolean
}

object MergeStatsSizeCalculator {

  implicit def persistentSizeCalculator(implicit sortedIndexConfig: SortedIndexBlockConfig): PersistentSizeCalculator =
    new PersistentSizeCalculator()

  /**
   * Calculates the size of [[MergeStats.Persistent]] Segments. The size of persistent
   * Segment is an approximation because the size of hashIndex and binarySearch cannot
   * be pre-computed if their configurations are custom.
   *
   * Logic is - MergeStats is considered small if it's sortedIndex size is less than half
   * the size of [[swaydb.config.SegmentConfig.minSegmentSize]].
   */
  class PersistentSizeCalculator(implicit sortedIndexConfig: SortedIndexBlockConfig) extends MergeStatsSizeCalculator[MergeStats.Persistent.Builder[Memory, ListBuffer]] {

    override def isStatsOrNullSmall(statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer])(implicit segmentConfig: SegmentBlockConfig): Boolean =
      if (statsOrNull == null || statsOrNull.isEmpty) {
        false
      } else {
        val mergeStats =
          statsOrNull.close(
            hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
            optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
          )

        mergeStats.keyValuesCount < segmentConfig.maxCount && mergeStats.maxSortedIndexSize + statsOrNull.totalValuesSize < segmentConfig.minSize / 2
      }
  }

  /**
   * Calculates the size of MemorySegment.
   */
  implicit object MemorySizeCalculator extends MergeStatsSizeCalculator[MergeStats.Memory.Builder[Memory, ListBuffer]] {

    override def isStatsOrNullSmall(statsOrNull: MergeStats.Memory.Builder[Memory, ListBuffer])(implicit segmentConfig: SegmentBlockConfig): Boolean =
      if (statsOrNull == null || statsOrNull.isEmpty)
        false
      else
        statsOrNull.keyValueCount < segmentConfig.maxCount && statsOrNull.segmentSize < segmentConfig.minSize

  }
}
