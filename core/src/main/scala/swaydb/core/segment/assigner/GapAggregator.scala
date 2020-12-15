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
import swaydb.core.merge.{KeyValueGrouper, MergeStats}

import scala.collection.mutable.ListBuffer

/**
 * An [[Aggregator]] that also builds [[MergeStats]] for gap key-values to help Segments pre-compute stats
 * which [[swaydb.core.segment.assigner.Assignment]] are happening.
 */
object GapAggregator {

  def persistentCreator(removeDeletes: Boolean): Aggregator.Creator[Assignable, ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]]] =
    if (removeDeletes)
      creator(MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.addLastLevel))
    else
      creator(MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)(_.toMemory()))

  def memoryCreator(removeDeletes: Boolean): Aggregator.Creator[Assignable, ListBuffer[Either[MergeStats.Memory.Builder[Memory, ListBuffer], Assignable.Collection]]] =
    if (removeDeletes)
      creator(MergeStats.memory[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.addLastLevel))
    else
      creator(MergeStats.memory[Memory, ListBuffer](Aggregator.listBuffer)(_.toMemory()))

  @inline private def creator[B >: Null <: MergeStats[Memory, ListBuffer]](createNewGap: => B): Aggregator.Creator[Assignable, ListBuffer[Either[B, Assignable.Collection]]] =
    () =>
      GapAggregator.aggregator(createNewGap)

  private def aggregator[B >: Null <: MergeStats[Memory, ListBuffer]](createNewGap: => B): Aggregator[Assignable, ListBuffer[Either[B, Assignable.Collection]]] =
    new Aggregator[Assignable, ListBuffer[Either[B, Assignable.Collection]]] {
      val buffer = ListBuffer.empty[Either[B, Assignable.Collection]]

      var lastStatsOrNull: B = _

      override def add(item: Assignable): Unit =
        item match {
          case collection: Assignable.Collection =>
            lastStatsOrNull = null
            buffer += Right(collection)

          case point: Assignable.Point =>
            if (lastStatsOrNull == null) {
              lastStatsOrNull = createNewGap
              buffer += Left(lastStatsOrNull)
            }

            lastStatsOrNull add point.toMemory()
        }

      override def result: ListBuffer[Either[B, Assignable.Collection]] =
        buffer
    }
}
