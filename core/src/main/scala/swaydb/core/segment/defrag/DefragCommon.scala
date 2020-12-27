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

package swaydb.core.segment.defrag

import swaydb.Aggregator
import swaydb.core.data.Memory
import swaydb.core.merge.{KeyValueGrouper, MergeStats}
import swaydb.core.segment.Segment
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.ref.SegmentRef

import scala.collection.mutable.ListBuffer

private[core] object DefragCommon {

  def isStatsOrNullSmall(statsOrNull: MergeStats.Persistent.Builder[Memory, ListBuffer])(implicit sortedIndexConfig: SortedIndexBlock.Config,
                                                                                         segmentConfig: SegmentBlock.Config): Boolean =
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


  @inline def isSegmentSmall(segment: Segment)(implicit segmentConfig: SegmentBlock.Config): Boolean =
    segment.segmentSize < segmentConfig.minSize

  @inline def isSegmentRefSmall(ref: SegmentRef)(implicit segmentConfig: SegmentBlock.Config): Boolean =
    ref.keyValueCount < segmentConfig.maxCount

  @inline def createMergeStats(removeDeletes: Boolean): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    if (removeDeletes)
      MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.addLastLevel)
    else
      MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)

  @inline def lastStatsOrNull(fragments: ListBuffer[TransientSegment.Fragment]): MergeStats.Persistent.Builder[Memory, ListBuffer] =
    fragments.lastOption match {
      case Some(stats: TransientSegment.Stats) =>
        stats.stats

      case Some(_) | None =>
        null
    }

}
