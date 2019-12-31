/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.data.accelerate

import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._

/**
 * Default Accelerator implementation.
 */
object Accelerator {

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
            releaseRate: FiniteDuration = 1.millisecond)(levelZeroMeter: LevelZeroMeter): Accelerator =
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
              releaseRate = releaseRate
            )
          )
    )

  /**
   * http://swaydb.io/configuring-levels/acceleration/noBrakes
   */

  def noBrakes(onMapCount: Int = 6,
               increaseMapSizeBy: Int = 2,
               maxMapSize: Long = 24.mb)(level0Meter: LevelZeroMeter): Accelerator =
    Accelerator(
      nextMapSize =
        nextMapSize(onMapCount, increaseMapSizeBy, maxMapSize, level0Meter),
      brake =
        None
    )

  def cruise(level0Meter: LevelZeroMeter): Accelerator =
    Accelerator(
      nextMapSize =
        level0Meter.defaultMapSize,
      brake =
        None
    )
}

/**
 * http://swaydb.io/configuring-levels/acceleration
 */
case class Accelerator(nextMapSize: Long,
                       brake: Option[Brake])
