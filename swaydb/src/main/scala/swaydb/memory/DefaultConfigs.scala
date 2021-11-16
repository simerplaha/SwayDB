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

package swaydb.memory

import swaydb.ActorConfig
import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.config.compaction.{LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.config.FileCache
import swaydb.utils.StorageUnits._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object DefaultConfigs {

  //4098 being the default file-system blockSize.
  def logSize: Int = 8.mb

  def segmentSize: Int = 4.mb

  def accelerator: LevelZeroMeter => Accelerator =
    Accelerator.brake(
      increaseLogSizeOnMapCount = 1,
      increaseLogSizeBy = 1,
      maxLogSize = logSize,
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
