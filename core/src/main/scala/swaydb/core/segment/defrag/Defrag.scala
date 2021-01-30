/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.core.data.{DefIO, Memory}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.stats.{MergeStats, MergeStatsCreator, MergeStatsSizeCalculator}
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.defrag.DefragSource._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object Defrag {

  def runOnGaps[SEG, S >: Null <: MergeStats.Segment[Memory, ListBuffer]](fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                                          headGap: Iterable[Assignable.Gap[S]],
                                                                          tailGap: Iterable[Assignable.Gap[S]],
                                                                          removeDeletes: Boolean,
                                                                          createdInLevel: Int,
                                                                          fence: TransientSegment.Fragment[S])(implicit segmentConfig: SegmentBlock.Config,
                                                                                                               mergeStatsCreator: MergeStatsCreator[S],
                                                                                                               mergeStatsSizeCalculator: MergeStatsSizeCalculator[S],
                                                                                                               executionContext: ExecutionContext): Future[ListBuffer[TransientSegment.Fragment[S]]] = {
    @inline def run(gap: Iterable[Assignable.Gap[S]], fragments: ListBuffer[TransientSegment.Fragment[S]]) =
      if (gap.isEmpty)
        Future.successful(fragments)
      else
        Future {
          DefragGap.run(
            gap = gap,
            fragments = fragments,
            removeDeletes = removeDeletes,
            createdInLevel = createdInLevel,
            hasNext = false
          )
        }

    val headFragments = run(headGap, fragments)
    val tailFragments = run(tailGap, ListBuffer.empty)

    for {
      head <- headFragments
      tail <- tailFragments
    } yield (head += fence) ++= tail
  }

  def runOnSegment[SEG, NULL_SEG >: SEG, S >: Null <: MergeStats.Segment[Memory, ListBuffer]](segment: SEG,
                                                                                              nullSegment: NULL_SEG,
                                                                                              fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                                                              headGap: Iterable[Assignable.Gap[S]],
                                                                                              tailGap: Iterable[Assignable.Gap[S]],
                                                                                              newKeyValues: Iterator[Assignable],
                                                                                              removeDeletes: Boolean,
                                                                                              createdInLevel: Int,
                                                                                              createFence: SEG => TransientSegment.Fragment[S])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                                functionStore: FunctionStore,
                                                                                                                                                defragSource: DefragSource[SEG],
                                                                                                                                                segmentConfig: SegmentBlock.Config,
                                                                                                                                                mergeStatsCreator: MergeStatsCreator[S],
                                                                                                                                                mergeStatsSizeCalculator: MergeStatsSizeCalculator[S]): DefIO[NULL_SEG, ListBuffer[TransientSegment.Fragment[S]]] = {
    //forceExpand if there are cleanable (updates, removes etc) key-values or if segment size is too small.
    val forceExpand =
      (removeDeletes && segment.hasUpdateOrRangeOrExpired) || ((headGap.nonEmpty || tailGap.nonEmpty) && segment.segmentSize < segmentConfig.minSize && segment.keyValueCount < segmentConfig.maxCount)

    val newFragments =
      if (headGap.isEmpty)
        fragments
      else
        DefragGap.run(
          gap = headGap,
          fragments = fragments,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          hasNext = newKeyValues.hasNext || forceExpand
        )

    val source =
      DefragMerge.run(
        segment = segment,
        nullSegment = nullSegment,
        newKeyValues = newKeyValues,
        removeDeletes = removeDeletes,
        forceExpand = forceExpand,
        fragments = newFragments
      )

    //if there was no segment replacement then create a fence so head and tail gaps do not get collapsed.
    if (source == nullSegment)
      newFragments += createFence(segment)

    val mergeResult =
      DefIO(
        input = source,
        output = newFragments
      )

    if (tailGap.nonEmpty)
      DefragGap.run(
        gap = tailGap,
        fragments = mergeResult.output,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        hasNext = false
      )

    mergeResult
  }
}
