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

package swaydb.core.segment.defrag

import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.merge.stats.{MergeStats, MergeStatsCreator, MergeStatsSizeCalculator}
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.defrag.DefragSource._
import swaydb.core.util.DefIO
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object Defrag {

  def runOnGaps[SEG, S >: Null <: MergeStats.Segment[Memory, ListBuffer]](fragments: ListBuffer[TransientSegment.Fragment[S]],
                                                                          headGap: Iterable[Assignable.Gap[S]],
                                                                          tailGap: Iterable[Assignable.Gap[S]],
                                                                          removeDeletes: Boolean,
                                                                          createdInLevel: Int,
                                                                          fence: TransientSegment.Fragment[S])(implicit segmentConfig: SegmentBlockConfig,
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
                                                                                                                                                segmentConfig: SegmentBlockConfig,
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
        initialiseIteratorsInOneSeek = segmentConfig.initialiseIteratorsInOneSeek,
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
