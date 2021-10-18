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

package swaydb

import swaydb.configs.level.DefaultExecutionContext
import swaydb.data.compaction.{CompactionConfig, PushStrategy}
import swaydb.data.{Atomic, OptimiseWrites}

import scala.concurrent.duration._

case object CommonConfigs {

  def segmentDeleteDelay: FiniteDuration = 10.seconds

  def compactionConfig(maxThreads: Int = Runtime.getRuntime.availableProcessors() max 2): CompactionConfig = {
    //assign a single thread to compaction Actor so that when writes alter compaction of
    //a new map available for compaction it does not starve and submits the messages and
    //continues accepting more writes
    val actorExecutionContext = DefaultExecutionContext.compactionEC(maxThreads = 1)

    //Remaining threads are the maximum threads minus the one reserved above.
    val availableCompactionThreads = maxThreads - 1
    val compactionExecutionContext = DefaultExecutionContext.compactionEC(maxThreads = availableCompactionThreads)

    CompactionConfig(
      resetCompactionPriorityAtInterval = 3,
      actorExecutionContext = actorExecutionContext,
      compactionExecutionContext = compactionExecutionContext,
      levelZeroFlattenParallelism = availableCompactionThreads,
      levelZeroMergeParallelism = availableCompactionThreads,
      multiLevelTaskParallelism = availableCompactionThreads,
      levelSegmentAssignmentParallelism = availableCompactionThreads,
      groupedSegmentDefragParallelism = availableCompactionThreads,
      defragmentedSegmentParallelism = availableCompactionThreads,
      pushStrategy = PushStrategy.OnOverflow
    )
  }

  def optimiseWrites(): OptimiseWrites =
    OptimiseWrites.RandomOrder

  def atomic(): Atomic =
    Atomic.Off
}
