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

package swaydb.data.compaction

import java.util.concurrent.ExecutorService

import swaydb.data.config.ConfigWizard

import scala.concurrent.ExecutionContext

sealed trait CompactionExecutionContext
object CompactionExecutionContext {

  def create(service: ExecutorService,
             parallelMerge: ParallelMerge): Create =
    Create(
      executionContext = ExecutionContext.fromExecutorService(service),
      parallelMerge = parallelMerge
    )

  /**
   * Starts a new compaction group. Assigning [[Create]] to a Level's default config via [[ConfigWizard]]
   * (see DefaultPersistentConfig for example) will start a new compaction group. If all the subsequent
   * Level's configs are [[Shared]] then they will join this group's compaction thread.
   *
   * @param executionContext used to execute compaction jobs
   * @param parallelMerge    see [[ParallelMerge]]
   */
  case class Create(executionContext: ExecutionContext,
                    parallelMerge: ParallelMerge) extends CompactionExecutionContext

  def shared(): CompactionExecutionContext.Shared =
    Shared

  /**
   * Shared joins the previous created ([[Create]]) compaction group.
   */
  sealed trait Shared extends CompactionExecutionContext
  case object Shared extends Shared

}
