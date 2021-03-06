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

import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.merge.KeyValueMerger
import swaydb.core.merge.stats.{MergeStats, MergeStatsCreator}
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.defrag.DefragSource._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

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
          segment.iterator(initialiseIteratorsInOneSeek) foreach (keyValue => lastStats.add(keyValue.toMemory()))
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
