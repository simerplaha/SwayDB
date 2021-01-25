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

package swaydb.data.compaction

import java.util.concurrent.ExecutorService
import scala.concurrent.ExecutionContext

object CompactionConfig {

  def create(resetCompactionPriorityAtInterval: Int,
             compactionService: ExecutorService,
             pushStrategy: PushStrategy): CompactionConfig =
    CompactionConfig(
      resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval,
      compactionExecutionContext = ExecutionContext.fromExecutorService(compactionService),
      pushStrategy = pushStrategy
    )

  def apply(resetCompactionPriorityAtInterval: Int,
            compactionExecutionContext: ExecutionContext,
            pushStrategy: PushStrategy): CompactionConfig =
    if (resetCompactionPriorityAtInterval <= 0)
      throw new Exception(s"Invalid resetCompactionPriorityAtInterval $resetCompactionPriorityAtInterval. Should be greater than zero.")
    else
      new CompactionConfig(
        resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval,
        executionContext = compactionExecutionContext,
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
                                    executionContext: ExecutionContext,
                                    pushStrategy: PushStrategy)
