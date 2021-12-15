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

package swaydb.config.compaction

import java.util.concurrent.ExecutorService
import scala.concurrent.ExecutionContext

object CompactionConfig {

  def create(resetCompactionPriorityAtInterval: Int,
             actorExecutionContext: ExecutorService,
             compactionExecutionContext: ExecutorService,
             pushStrategy: PushStrategy): CompactionConfig =
    CompactionConfig(
      resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval,
      actorExecutionContext = ExecutionContext.fromExecutorService(actorExecutionContext),
      compactionExecutionContext = ExecutionContext.fromExecutorService(compactionExecutionContext),
      pushStrategy = pushStrategy
    )

  def apply(resetCompactionPriorityAtInterval: Int,
            actorExecutionContext: ExecutionContext,
            compactionExecutionContext: ExecutionContext,
            pushStrategy: PushStrategy): CompactionConfig =
    if (resetCompactionPriorityAtInterval <= 0)
      throw new Exception(s"Invalid resetCompactionPriorityAtInterval $resetCompactionPriorityAtInterval. Should be greater than zero.")
    else
      new CompactionConfig(
        resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval,
        actorExecutionContext = actorExecutionContext,
        compactionExecutionContext = compactionExecutionContext,
        pushStrategy = pushStrategy
      )
}
/**
 * Configures Compaction strategy.
 *
 * @param executionContext                  [[ExecutionContext]] assigned to compaction.
 * @param resetCompactionPriorityAtInterval Example: if there are 7 Levels then setting this to 2 will
 *                                          run compaction on a maximum of two levels consecutively before
 *                                          re-ordering/re-prioritising/re-computing compaction priority.
 */

case class CompactionConfig private(resetCompactionPriorityAtInterval: Int,
                                    actorExecutionContext: ExecutionContext,
                                    compactionExecutionContext: ExecutionContext,
                                    pushStrategy: PushStrategy)
