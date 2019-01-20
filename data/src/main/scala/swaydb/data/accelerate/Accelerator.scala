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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.accelerate

import scala.concurrent.duration._
import swaydb.data.util.StorageUnits._

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
                          level0Meter: Level0Meter) =
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
            releaseRate: FiniteDuration = 1.millisecond)(level0Meter: Level0Meter): Accelerator =
    Accelerator(
      nextMapSize =
        nextMapSize(increaseMapSizeOnMapCount, increaseMapSizeBy, maxMapSize, level0Meter),
      brake =
        if (level0Meter.mapsCount < brakeOnMapCount)
          None
        else
          Some(
            Brake(
              brakeFor = brakeFor * (level0Meter.mapsCount - brakeOnMapCount + 1),
              releaseRate = releaseRate
            )
          )
    )

  /**
    * http://swaydb.io/configuring-levels/acceleration/noBrakes
    */

  def noBrakes(onMapCount: Int = 6,
               increaseMapSizeBy: Int = 2,
               maxMapSize: Long = 24.mb)(level0Meter: Level0Meter): Accelerator =
    Accelerator(
      nextMapSize =
        nextMapSize(onMapCount, increaseMapSizeBy, maxMapSize, level0Meter),
      brake =
        None
    )

  def cruise(level0Meter: Level0Meter): Accelerator =
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
