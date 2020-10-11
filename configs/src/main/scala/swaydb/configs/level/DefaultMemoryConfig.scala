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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.configs.level

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultMemoryConfig extends LazyLogging {

  /**
   * Default configuration for 2 leveled Memory database.
   */
  def apply(mapSize: Int,
            appliedFunctionsMapSize: Int,
            clearAppliedFunctionsOnBoot: Boolean,
            minSegmentSize: Int,
            maxKeyValuesPerSegment: Int,
            deleteSegmentsEventually: Boolean,
            mergeParallelism: Int,
            acceleration: LevelZeroMeter => Accelerator,
            levelZeroThrottle: LevelZeroMeter => FiniteDuration,
            lastLevelThrottle: LevelMeter => Throttle,
            optimiseWrites: OptimiseWrites,
            atomic: Atomic)(implicit executionContext: ExecutionContext): SwayDBMemoryConfig =
    ConfigWizard
      .withMemoryLevel0(
        mapSize = mapSize,
        appliedFunctionsMapSize = appliedFunctionsMapSize,
        clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
        compactionExecutionContext = CompactionExecutionContext.Create(executionContext, mergeParallelism),
        optimiseWrites = optimiseWrites,
        atomic = atomic,
        acceleration = acceleration,
        throttle = levelZeroThrottle
      )
      .withMemoryLevel1(
        minSegmentSize = minSegmentSize,
        maxKeyValuesPerSegment = maxKeyValuesPerSegment,
        copyForward = false,
        deleteSegmentsEventually = deleteSegmentsEventually,
        compactionExecutionContext = CompactionExecutionContext.Shared,
        throttle = lastLevelThrottle
      )
}
