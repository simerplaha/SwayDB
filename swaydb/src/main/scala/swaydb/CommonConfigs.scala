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

package swaydb

import swaydb.configs.level.DefaultExecutionContext
import swaydb.data.compaction.{CompactionExecutionContext, ParallelMerge}
import swaydb.data.{Atomic, OptimiseWrites}

import scala.concurrent.duration._

case object CommonConfigs {

  def segmentDeleteDelay = 10.seconds

  def parallelMerge(): ParallelMerge =
    ParallelMerge.On(
      levelParallelism = 0,
      levelParallelismTimeout = 1.hour,
      //disable Segment parallelism because default tuning is for small machines
      //this can be turned on similar to above if you have more cores.
      segmentParallelism = 0,
      segmentParallelismTimeout = 5.seconds
    )

  def compactionExecutionContext(): CompactionExecutionContext.Create = {
    //init default parallel merge config
    val parallelMerge = CommonConfigs.parallelMerge()
    //create default compaction execution context passing it parallelMerge for parallelMerge's threads to be created
    val executionContext = DefaultExecutionContext.compactionEC(parallelMerge = parallelMerge)
    //create compaction config
    CompactionExecutionContext.Create(
      compactionExecutionContext = executionContext,
      compactionIOExecutionContext = DefaultExecutionContext.compactionIOEC,
      parallelMerge = parallelMerge,
      resetCompactionPriorityAtInterval = 3
    )
  }

  def optimiseWrites(): OptimiseWrites =
    OptimiseWrites.RandomOrder

  def atomic(): Atomic =
    Atomic.Off

}
