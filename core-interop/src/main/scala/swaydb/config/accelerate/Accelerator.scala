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

package swaydb.config.accelerate

import swaydb.config.accelerate.builder.{BrakeBuilder, NoBrakesBuilder}
import swaydb.utils.Java.OptionalConverter
import swaydb.utils.StorageUnits._

import java.util.Optional
import scala.concurrent.duration._

/**
 * Default Accelerator implementation.
 */
object Accelerator {

  def apply(nextLogSize: Int, brake: Optional[Brake]): Accelerator =
    new Accelerator(
      nextLogSize = nextLogSize,
      brake = brake.asScala
    )

  /**
   * http://swaydb.io/configuring-levels/acceleration
   */
  private def nextLogSize(mapCount: Int,
                          increaseLogSizeBy: Int,
                          maxLogSize: Int,
                          level0Meter: LevelZeroMeter) =
    if (level0Meter.logsCount < mapCount)
      level0Meter.defaultLogSize
    else
      (level0Meter.currentLogSize * increaseLogSizeBy) min maxLogSize

  /**
   * http://swaydb.io/configuring-levels/acceleration/brake
   */
  def brake(increaseLogSizeOnMapCount: Int = 4,
            increaseLogSizeBy: Int = 2,
            maxLogSize: Int = 24.mb,
            brakeOnMapCount: Int = 6,
            brakeFor: FiniteDuration = 50.milliseconds,
            releaseRate: FiniteDuration = 1.millisecond,
            logAsWarning: Boolean = true)(levelZeroMeter: LevelZeroMeter): Accelerator =
    Accelerator(
      nextLogSize =
        nextLogSize(increaseLogSizeOnMapCount, increaseLogSizeBy, maxLogSize, levelZeroMeter),
      brake =
        if (levelZeroMeter.logsCount < brakeOnMapCount)
          None
        else
          Some(
            Brake(
              brakeFor = brakeFor * (levelZeroMeter.logsCount - brakeOnMapCount + 1),
              releaseRate = releaseRate,
              logAsWarning = logAsWarning
            )
          )
    )

  def brakeBuilder(): BrakeBuilder.Step0 =
    BrakeBuilder.builder()

  def noBrakesBuilder(): NoBrakesBuilder.Step0 =
    NoBrakesBuilder.builder()

  /**
   * http://swaydb.io/configuring-levels/acceleration/noBrakes
   */

  def noBrakes(onMapCount: Int = 6,
               increaseLogSizeBy: Int = 2,
               maxLogSize: Int = 24.mb)(level0Meter: LevelZeroMeter): Accelerator =
    Accelerator(
      nextLogSize =
        nextLogSize(
          mapCount = onMapCount,
          increaseLogSizeBy = increaseLogSizeBy,
          maxLogSize = maxLogSize,
          level0Meter = level0Meter
        ),
      brake =
        None
    )

  def cruise(levelZeroMeter: LevelZeroMeter): Accelerator =
    Accelerator(
      nextLogSize = levelZeroMeter.defaultLogSize,
      brake = None
    )
}

/**
 * http://swaydb.io/configuring-levels/acceleration
 */
case class Accelerator(nextLogSize: Int,
                       brake: Option[Brake])
