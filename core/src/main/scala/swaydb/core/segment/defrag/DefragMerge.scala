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

import swaydb.core.function.FunctionStore
import swaydb.core.merge.KeyValueMerger
import swaydb.core.segment.SegmentSource
import swaydb.core.segment.SegmentSource._
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.data.TransientSegment
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

  def run[SEG, NULL_SEG >: SEG](segment: SEG,
                                nullSegment: NULL_SEG,
                                mergeableCount: Int,
                                mergeable: Iterator[Assignable],
                                removeDeletes: Boolean,
                                forceExpand: Boolean,
                                fragments: ListBuffer[TransientSegment.Fragment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                                                  functionStore: FunctionStore,
                                                                                  segmentSource: SegmentSource[SEG]): NULL_SEG =
    if (mergeableCount > 0)
      fragments.lastOption match {
        case Some(TransientSegment.Stats(stats)) =>
          KeyValueMerger.merge(
            headGap = Assignable.emptyIterable,
            tailGap = Assignable.emptyIterable,
            mergeableCount = mergeableCount,
            mergeable = mergeable,
            oldKeyValuesCount = segment.keyValueCount,
            oldKeyValues = segment.iterator(),
            stats = stats,
            isLastLevel = removeDeletes
          )

          segment

        case Some(_) | None =>
          val newStats = DefragCommon.createMergeStats(removeDeletes = removeDeletes)

          KeyValueMerger.merge(
            headGap = Assignable.emptyIterable,
            tailGap = Assignable.emptyIterable,
            mergeableCount = mergeableCount,
            mergeable = mergeable,
            oldKeyValuesCount = segment.keyValueCount,
            oldKeyValues = segment.iterator(),
            stats = newStats,
            isLastLevel = removeDeletes
          )

          if (!newStats.isEmpty)
            fragments += TransientSegment.Stats(newStats)

          segment
      }
    else if (forceExpand)
      fragments.lastOption match {
        case Some(TransientSegment.Stats(lastStats)) =>
          segment.iterator() foreach (keyValue => lastStats.add(keyValue.toMemory()))
          segment

        case Some(_) | None =>
          val newStats = DefragCommon.createMergeStats(removeDeletes = removeDeletes)

          KeyValueMerger.merge(
            headGap = Assignable.emptyIterable,
            tailGap = Assignable.emptyIterable,
            mergeableCount = mergeableCount,
            mergeable = mergeable,
            oldKeyValuesCount = segment.keyValueCount,
            oldKeyValues = segment.iterator(),
            stats = newStats,
            isLastLevel = removeDeletes
          )

          if (!newStats.isEmpty)
            fragments += TransientSegment.Stats(newStats)

          segment
      }
    else
      nullSegment
}
