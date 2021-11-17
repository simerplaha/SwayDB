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

package swaydb.core.segment.data.merge.stats

import swaydb.utils.Aggregator
import swaydb.core.segment.data.Memory
import swaydb.core.segment.data.merge.KeyValueGrouper

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
  implicit object PersistentCreator extends MergeStatsCreator[MergeStats.Persistent.Builder[Memory, ListBuffer]] {

    override def create(removeDeletes: Boolean): MergeStats.Persistent.Builder[Memory, ListBuffer] =
      if (removeDeletes)
        MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.toLastLevelOrNull)
      else
        MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)
  }

  /**
   * Create [[MergeStats]] instance for memory [[swaydb.core.level.Level]]
   */
  implicit object MemoryCreator extends MergeStatsCreator[MergeStats.Memory.Builder[Memory, ListBuffer]] {

    override def create(removeDeletes: Boolean): MergeStats.Memory.Builder[Memory, ListBuffer] =
      if (removeDeletes)
        MergeStats.memory[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.toLastLevelOrNull)
      else
        MergeStats.memory[Memory, ListBuffer](Aggregator.listBuffer)
  }
}
