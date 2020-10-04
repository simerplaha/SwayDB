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

package swaydb.data.accelerate

import java.util.Optional

import swaydb.data.accelerate.builder.{BrakeBuilder, NoBrakesBuilder}
import swaydb.data.util.Java._
import swaydb.data.util.StorageUnits._

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
