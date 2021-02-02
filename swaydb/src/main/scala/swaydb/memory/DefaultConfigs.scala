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

package swaydb.memory

import swaydb.ActorConfig
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config.FileCache
import swaydb.utils.StorageUnits._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultConfigs {

  //4098 being the default file-system blockSize.
  def mapSize: Int = 8.mb

  def segmentSize: Int = 4.mb

  def accelerator: LevelZeroMeter => Accelerator =
    Accelerator.brake(
      increaseMapSizeOnMapCount = 1,
      increaseMapSizeBy = 1,
      maxMapSize = mapSize,
      brakeOnMapCount = 6,
      brakeFor = 1.milliseconds,
      releaseRate = 0.01.millisecond,
      logAsWarning = false
    )

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

  def levelZeroThrottle(meter: LevelZeroMeter): LevelZeroThrottle =
    swaydb.persistent.DefaultConfigs.levelZeroThrottle(meter)

  def lastLevelThrottle(meter: LevelMeter): LevelThrottle =
    swaydb.persistent.DefaultConfigs.levelSixThrottle(meter)

}
