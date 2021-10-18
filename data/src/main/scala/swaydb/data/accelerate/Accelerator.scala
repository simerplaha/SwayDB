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

package swaydb.data.accelerate

import swaydb.data.accelerate.builder.{BrakeBuilder, NoBrakesBuilder}
import swaydb.utils.Java.OptionalConverter
import swaydb.utils.StorageUnits._

import java.util.Optional
import scala.concurrent.duration._

/**
 * Default Accelerator implementation.
 */
object Accelerator {

  def apply(nextMapSize: Long, brake: Optional[Brake]): Accelerator =
    new Accelerator(
      nextMapSize = nextMapSize,
      brake = brake.asScala
    )

  /**
   * http://swaydb.io/configuring-levels/acceleration
   */
  private def nextMapSize(mapCount: Int,
                          increaseMapSizeBy: Int,
                          maxMapSize: Long,
                          level0Meter: LevelZeroMeter) =
    if (level0Meter.mapsCount < mapCount)
      level0Meter.defaultMapSize
    else
      (level0Meter.currentMapSize * increaseMapSizeBy) min maxMapSize

  /**
   * http://swaydb.io/configuring-levels/acceleration/brake
   */
  def brake(increaseMapSizeOnMapCount: Int = 4,
            increaseMapSizeBy: Int = 2,
            maxMapSize: Long = 24.mb,
            brakeOnMapCount: Int = 6,
            brakeFor: FiniteDuration = 50.milliseconds,
            releaseRate: FiniteDuration = 1.millisecond,
            logAsWarning: Boolean = true)(levelZeroMeter: LevelZeroMeter): Accelerator =
    Accelerator(
      nextMapSize =
        nextMapSize(increaseMapSizeOnMapCount, increaseMapSizeBy, maxMapSize, levelZeroMeter),
      brake =
        if (levelZeroMeter.mapsCount < brakeOnMapCount)
          None
        else
          Some(
            Brake(
              brakeFor = brakeFor * (levelZeroMeter.mapsCount - brakeOnMapCount + 1),
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
               increaseMapSizeBy: Int = 2,
               maxMapSize: Long = 24.mb)(level0Meter: LevelZeroMeter): Accelerator =
    Accelerator(
      nextMapSize =
        nextMapSize(
          mapCount = onMapCount,
          increaseMapSizeBy = increaseMapSizeBy,
          maxMapSize = maxMapSize,
          level0Meter = level0Meter
        ),
      brake =
        None
    )

  def cruise(level0Meter: LevelZeroMeter): Accelerator =
    Accelerator(
      nextMapSize = level0Meter.defaultMapSize,
      brake = None
    )
}

/**
 * http://swaydb.io/configuring-levels/acceleration
 */
case class Accelerator(nextMapSize: Long,
                       brake: Option[Brake])
