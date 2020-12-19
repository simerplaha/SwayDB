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

import swaydb.core.data.{Memory, MergeResult}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.MergeStats
import swaydb.core.segment.SegmentSource
import swaydb.core.segment.SegmentSource._
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

object Defrag {

  def run[SEG, NULL_SEG >: SEG](segment: Option[SEG],
                                nullSegment: NULL_SEG,
                                fragments: ListBuffer[TransientSegment.Fragment],
                                headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                mergeableCount: Int,
                                mergeable: Iterator[Assignable],
                                removeDeletes: Boolean,
                                createdInLevel: Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                     functionStore: FunctionStore,
                                                     segmentSource: SegmentSource[SEG],
                                                     valuesConfig: ValuesBlock.Config,
                                                     sortedIndexConfig: SortedIndexBlock.Config,
                                                     binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                     hashIndexConfig: HashIndexBlock.Config,
                                                     bloomFilterConfig: BloomFilterBlock.Config,
                                                     segmentConfig: SegmentBlock.Config): MergeResult[NULL_SEG, ListBuffer[TransientSegment.Fragment]] = {

    val mergeResult =
      segment match {
        case Some(segment) =>
          //forceExpand if there are cleanable key-values or if segment size is too small.
          val forceExpand =
            (removeDeletes && segment.hasUpdateOrRange) || ((headGap.nonEmpty || tailGap.nonEmpty) && segment.segmentSize < segmentConfig.minSize && segment.keyValueCount < segmentConfig.maxCount)

          val newFragments =
            if (headGap.isEmpty)
              fragments
            else
              DefragGap.run(
                gap = headGap,
                fragments = fragments,
                removeDeletes = removeDeletes,
                createdInLevel = createdInLevel,
                hasNext = mergeableCount > 0 || forceExpand
              )

          val source =
            DefragMerge.run(
              segment = segment,
              nullSegment = nullSegment,
              mergeableCount = mergeableCount,
              mergeable = mergeable,
              removeDeletes = removeDeletes,
              forceExpand = forceExpand,
              fragments = newFragments
            )

          //if there was no segment replacement then create a fence so head and tail gaps do not get collapsed.
          if (source == nullSegment)
            newFragments += TransientSegment.Fence

          MergeResult(
            source = source,
            result = newFragments
          )

        case None =>
          val newFragments =
            if (headGap.isEmpty)
              fragments
            else
              DefragGap.run(
                gap = headGap,
                fragments = fragments,
                removeDeletes = removeDeletes,
                createdInLevel = createdInLevel,
                hasNext = false
              )

          //create a fence so tail does not get collapsed into head.
          newFragments += TransientSegment.Fence

          MergeResult(
            source = nullSegment,
            result = newFragments
          )
      }

    if (tailGap.nonEmpty)
      DefragGap.run(
        gap = tailGap,
        fragments = mergeResult.result,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        hasNext = false
      )

    mergeResult
  }
}
