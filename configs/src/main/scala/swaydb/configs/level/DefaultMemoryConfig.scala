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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.configs.level

import java.util.concurrent.Executors

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, Throttle}
import swaydb.data.config._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultMemoryConfig {

  private lazy val compactionExecutionContext =
    new ExecutionContext {
      val threadPool = Executors.newSingleThreadExecutor()

      def execute(runnable: Runnable) =
        threadPool execute runnable

      def reportFailure(exception: Throwable): Unit =
        System.err.println("Execution context failure", exception)
    }

  /**
   * Default configuration for 2 leveled Memory database.
   */
  def apply(mapSize: Int,
            segmentSize: Int,
            mightContainFalsePositiveRate: Double,
            deleteSegmentsEventually: Boolean,
            acceleration: LevelZeroMeter => Accelerator): SwayDBMemoryConfig =
    ConfigWizard
      .addMemoryLevel0(
        mapSize = mapSize,
        compactionExecutionContext = CompactionExecutionContext.Create(compactionExecutionContext),
        acceleration = acceleration,
        throttle = _ => Duration.Zero
      )
      .addMemoryLevel1(
        segmentSize = segmentSize,
        copyForward = false,
        deleteSegmentsEventually = deleteSegmentsEventually,
        mightContainIndex =
          MightContainIndex.Enable(
            falsePositiveRate = mightContainFalsePositiveRate,
            minimumNumberOfKeys = 100,
            updateMaxProbe = optimalMaxProbe => 1,
            ioStrategy = ioAction => IOStrategy.SynchronisedIO(cacheOnAccess = ioAction.isCompressed),
            compression = _ => Seq.empty
          ),
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle =
          _ =>
            Throttle(5.seconds, 5)
      )
}
