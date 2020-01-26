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
 */

package swaydb.configs.level

import java.nio.file.Path
import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.config._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object DefaultPersistentZeroConfig extends LazyLogging {

  private lazy val compactionExecutionContext =
    new ExecutionContext {
      val threadPool = Executors.newSingleThreadExecutor(SingleThreadFactory.create())

      def execute(runnable: Runnable) =
        threadPool execute runnable

      def reportFailure(exception: Throwable): Unit = {
        val message = s"REPORT FAILURE! ${exception.getMessage}"
        println(message)
        logger.error(message, exception)
      }
    }

  /**
   * Default configuration for a single persistent level zero only database.
   */
  def apply(dir: Path,
            otherDirs: Seq[Dir],
            mapSize: Int,
            mmapMaps: Boolean,
            recoveryMode: RecoveryMode,
            acceleration: LevelZeroMeter => Accelerator): LevelZeroPersistentConfig =
    ConfigWizard
      .addPersistentLevel0(
        dir = dir,
        mapSize = mapSize,
        mmap = mmapMaps,
        recoveryMode = recoveryMode,
        acceleration = acceleration,
        throttle = _ => Duration.Zero,
        compactionExecutionContext = CompactionExecutionContext.Create(compactionExecutionContext)
      )
}
