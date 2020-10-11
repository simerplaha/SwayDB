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

package swaydb.memory

import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{ActorConfig, FileCache}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultConfigs {

  def mergeParallelism(): Int =
    Runtime.getRuntime.availableProcessors() - 1 // -1 for the compaction thread.

  def fileCache(implicit ec: ExecutionContext): FileCache.On =
    FileCache.On(
      maxOpen = 1000,
      actorConfig =
        ActorConfig.TimeLoop(
          name = s"${this.getClass.getName} - FileCache TimeLoop Actor",
          delay = 10.seconds,
          ec = ec
        )
    )

  def levelZeroThrottle(meter: LevelZeroMeter): FiniteDuration =
    swaydb.persistent.DefaultConfigs.levelZeroThrottle(meter)

  def lastLevelThrottle(meter: LevelMeter): Throttle =
    swaydb.persistent.DefaultConfigs.levelSixThrottle(meter)

  def optimiseWrites(): OptimiseWrites =
    swaydb.persistent.DefaultConfigs.optimiseWrites()

  def atomic(): Atomic =
    swaydb.persistent.DefaultConfigs.atomic()
}
