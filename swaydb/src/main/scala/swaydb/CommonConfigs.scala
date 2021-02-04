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

package swaydb

import swaydb.configs.level.DefaultExecutionContext
import swaydb.data.compaction.{CompactionConfig, PushStrategy}
import swaydb.data.{Atomic, OptimiseWrites}

import scala.concurrent.duration._

case object CommonConfigs {

  def segmentDeleteDelay: FiniteDuration = 10.seconds

  def compactionConfig(maxThreads: Int = Runtime.getRuntime.availableProcessors() max 2): CompactionConfig = {
    //create compaction config
    val actorExecutionContext = DefaultExecutionContext.compactionEC(maxThreads = 1)

    val availableCompactionThreads = maxThreads - 1
    val compactionExecutionContext = DefaultExecutionContext.compactionEC(maxThreads = availableCompactionThreads)

    CompactionConfig(
      resetCompactionPriorityAtInterval = 3,
      actorExecutionContext = actorExecutionContext,
      compactionExecutionContext = compactionExecutionContext,
      levelZeroFlattenParallelism = availableCompactionThreads,
      levelZeroMergeParallelism = availableCompactionThreads,
      multiLevelTaskParallelism = 1,
      levelSegmentAssignmentParallelism = 2,
      groupedSegmentDefragParallelism = 2,
      defragmentedSegmentParallelism = 2,
      pushStrategy = PushStrategy.OnOverflow
    )
  }

  def optimiseWrites(): OptimiseWrites =
    OptimiseWrites.RandomOrder

  def atomic(): Atomic =
    Atomic.Off
}
