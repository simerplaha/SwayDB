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

import swaydb.data.config.ConfigWizard

import java.util.concurrent.ExecutorService
import scala.concurrent.ExecutionContext

sealed trait CompactionExecutionContext
object CompactionExecutionContext {

  def create(compactionService: ExecutorService,
             resetCompactionPriorityAtInterval: Int): Create =
    Create(
      compactionExecutionContext = ExecutionContext.fromExecutorService(compactionService),
      resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval
    )

  object Create {
    def apply(compactionExecutionContext: ExecutionContext,
              resetCompactionPriorityAtInterval: Int): Create =
      if (resetCompactionPriorityAtInterval <= 0)
        throw new Exception(s"Invalid resetCompactionPriorityAtInterval $resetCompactionPriorityAtInterval. Should be greater than zero.")
      else
        new Create(
          compactionExecutionContext = compactionExecutionContext,
          resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval
        )
  }

  /**
   * Starts a new compaction group. Assigning [[Create]] to a Level's default config via [[ConfigWizard]]
   * (see DefaultPersistentConfig for example) will start a new compaction group. If all the subsequent
   * Level's configs are [[Shared]] then they will join this group's compaction thread.
   *
   * @param compactionExecutionContext        used to execute compaction jobs
   * @param resetCompactionPriorityAtInterval Example: if there are 7 Levels then setting this to 2 will
   *                                          run compaction on a maximum of two levels consecutively before
   *                                          re-ordering/re-prioritising/re-computing compaction priority.
   */
  case class Create private(compactionExecutionContext: ExecutionContext,
                            resetCompactionPriorityAtInterval: Int) extends CompactionExecutionContext

  def shared(): CompactionExecutionContext.Shared =
    Shared

  /**
   * Shared joins the previous created ([[Create]]) compaction group.
   */
  sealed trait Shared extends CompactionExecutionContext
  case object Shared extends Shared

}
