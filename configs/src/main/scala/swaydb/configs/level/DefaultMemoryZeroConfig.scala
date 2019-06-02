/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.configs.level

import java.util.concurrent.Executors

import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.config._

import scala.concurrent.ExecutionContext

object DefaultMemoryZeroConfig {

  private lazy val compactionExecutionContext =
    new ExecutionContext {
      val threadPool = Executors.newSingleThreadExecutor()

      def execute(runnable: Runnable) =
        threadPool execute runnable

      def reportFailure(exception: Throwable): Unit =
        System.err.println("Execution context failure", exception)
    }

  /**
    * Default configuration for a single level zero only Memory database.
    */
  def apply(mapSize: Int,
            acceleration: Level0Meter => Accelerator): LevelZeroMemoryConfig =
    ConfigWizard
      .addMemoryLevel0(
        mapSize = mapSize,
        acceleration = acceleration,
        compactionExecutionContext = CompactionExecutionContext.Create(compactionExecutionContext)
      )
}
