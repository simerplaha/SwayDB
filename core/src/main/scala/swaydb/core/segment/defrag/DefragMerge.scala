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

import swaydb.core.segment.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.segment.data.merge.KeyValueMerger
import swaydb.core.segment.data.merge.stats.{MergeStats, MergeStatsCreator}
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.defrag.DefragSource._
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice

import scala.collection.mutable.ListBuffer

/**
 * Defrag gap key-values or [[Assignable.Collection]] by avoiding expanding collections as much as possible
 * so that we can defer transfer bytes to OS skipping JVM heap allocation.
 *
 * But always expand if
 *  - the collection has removable/cleanable key-values.
 *  - the collection is small or head key-values are too small.
 */

private[segment] object DefragMerge {

  /**
   * @return the input [[SEG]] if the merge occurred else returns [[NULL_SEG]] indicating
   *         no merge occurred.
   */
  def run[SEG, NULL_SEG >: SEG, S <: MergeStats.Segment[Memory, ListBuffer]](segment: SEG,
                                                                             nullSegment: NULL_SEG,
                                                                             newKeyValues: Iterator[Assignable],
                                                                             removeDeletes: Boolean,
                                                                             forceExpand: Boolean,
                                                                             initialiseIteratorsInOneSeek: Boolean,
                                                                             fragments: ListBuffer[TransientSegment.Fragment[S]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                  functionStore: FunctionStore,
                                                                                                                                  defragSource: DefragSource[SEG],
                                                                                                                                  mergeStatsCreator: MergeStatsCreator[S]): NULL_SEG =
    if (newKeyValues.hasNext)
      fragments.lastOption match {
        case Some(TransientSegment.Stats(stats)) =>
          KeyValueMerger.merge(
            headGap = Assignable.emptyIterable,
            tailGap = Assignable.emptyIterable,
            newKeyValues = newKeyValues,
            oldKeyValues = segment.iterator(initialiseIteratorsInOneSeek),
            stats = stats,
            isLastLevel = removeDeletes,
            initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek
          )

          segment

        case Some(_) | None =>
          val newStats = mergeStatsCreator.create(removeDeletes = removeDeletes)

          KeyValueMerger.merge(
            headGap = Assignable.emptyIterable,
            tailGap = Assignable.emptyIterable,
            newKeyValues = newKeyValues,
            oldKeyValues = segment.iterator(initialiseIteratorsInOneSeek),
            stats = newStats,
            isLastLevel = removeDeletes,
            initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek
          )

          if (!newStats.isEmpty)
            fragments += TransientSegment.Stats(newStats)

          segment
      }
    else if (forceExpand)
      fragments.lastOption match {
        case Some(TransientSegment.Stats(lastStats)) =>
          segment.iterator(initialiseIteratorsInOneSeek) foreach (keyValue => lastStats.addOne(keyValue.toMemory()))
          segment

        case Some(_) | None =>
          val newStats = mergeStatsCreator.create(removeDeletes = removeDeletes)

          KeyValueMerger.merge(
            headGap = Assignable.emptyIterable,
            tailGap = Assignable.emptyIterable,
            newKeyValues = newKeyValues,
            oldKeyValues = segment.iterator(initialiseIteratorsInOneSeek),
            stats = newStats,
            isLastLevel = removeDeletes,
            initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek
          )

          if (!newStats.isEmpty)
            fragments += TransientSegment.Stats(newStats)

          segment
      }
    else
      nullSegment
}
