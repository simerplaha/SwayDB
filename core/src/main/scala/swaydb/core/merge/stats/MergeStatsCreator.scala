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

package swaydb.core.merge.stats

import swaydb.Aggregator
import swaydb.core.data.Memory
import swaydb.core.merge.KeyValueGrouper

import scala.collection.mutable.ListBuffer

/**
 * Allows creating [[MergeStats]] instance depending if the current defragment process is for
 * [[swaydb.core.segment.MemorySegment]] or [[swaydb.core.segment.PersistentSegment]].
 */
sealed trait MergeStatsCreator[S] {
  def create(removeDeletes: Boolean): S
}

object MergeStatsCreator {

  /**
   * Create [[MergeStats]] instance for persistent [[swaydb.core.level.Level]]
   */
  implicit case object PersistentCreator extends MergeStatsCreator[MergeStats.Persistent.Builder[Memory, ListBuffer]] {

    override def create(removeDeletes: Boolean): MergeStats.Persistent.Builder[Memory, ListBuffer] =
      if (removeDeletes)
        MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.addLastLevel)
      else
        MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)
  }

  /**
   * Create [[MergeStats]] instance for memory [[swaydb.core.level.Level]]
   */
  implicit case object MemoryCreator extends MergeStatsCreator[MergeStats.Memory.Builder[Memory, ListBuffer]] {

    override def create(removeDeletes: Boolean): MergeStats.Memory.Builder[Memory, ListBuffer] =
      if (removeDeletes)
        MergeStats.memory[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.addLastLevel)
      else
        MergeStats.memory[Memory, ListBuffer](Aggregator.listBuffer)
  }
}
