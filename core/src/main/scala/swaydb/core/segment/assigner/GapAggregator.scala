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

package swaydb.core.segment.assigner

import swaydb.Aggregator
import swaydb.core.data.Memory
import swaydb.core.merge.{MergeStats, MergeStatsCreator}

import scala.collection.mutable.ListBuffer

/**
 * An [[Aggregator]] that also builds [[MergeStats]] for gap key-values to help Segments pre-compute stats
 * which [[swaydb.core.segment.assigner.SegmentAssignment]] are happening.
 */
object GapAggregator {

  def create[S >: Null <: MergeStats[Memory, ListBuffer]](removeDeletes: Boolean)(implicit mergeStatsCreator: MergeStatsCreator[S]): Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[S]]] =
    creator[S](mergeStatsCreator.createMergeStats(removeDeletes))

  @inline private def creator[B >: Null <: MergeStats[Memory, ListBuffer]](createNewGap: => B): Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[B]]] =
    () =>
      GapAggregator.aggregator(createNewGap)

  private def aggregator[B >: Null <: MergeStats[Memory, ListBuffer]](createNewGap: => B): Aggregator[Assignable, ListBuffer[Assignable.Gap[B]]] =
    new Aggregator[Assignable, ListBuffer[Assignable.Gap[B]]] {
      val buffer = ListBuffer.empty[Assignable.Gap[B]]

      var lastStatsOrNull: B = _

      override def add(item: Assignable): Unit =
        item match {
          case collection: Assignable.Collection =>
            lastStatsOrNull = null
            buffer += collection

          case point: Assignable.Point =>
            if (lastStatsOrNull == null) {
              lastStatsOrNull = createNewGap
              buffer += Assignable.Stats(lastStatsOrNull)
            }

            lastStatsOrNull add point.toMemory()
        }

      override def result: ListBuffer[Assignable.Gap[B]] =
        buffer
    }
}
