/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import org.scalatest.{Matchers, WordSpec}
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._

class AcceleratorSpec extends WordSpec with Matchers {

  "BlockingBrakeAccelerator" should {
    "not apply brakes and not increase the map size when map count is not reached" in {
      Accelerator.brake(
        increaseMapSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 10.millisecond,
        releaseRate = 0.5.millisecond,
        increaseMapSizeBy = 5,
        maxMapSize = 10.mb
      )(
        level0Meter =
          Level0Meter(
            defaultMapSize = 4.mb,
            currentMapSize = 4.mb,
            mapsCount = 1
          )
      ) shouldBe Accelerator(nextMapSize = 4.mb, brake = None)
    }

    "increase the map size when the map count is reached and apply brakes" in {
      Accelerator.brake(
        increaseMapSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 10.millisecond,
        releaseRate = 0.5.millisecond,
        increaseMapSizeBy = 2,
        maxMapSize = 10.mb
      )(
        level0Meter =
          Level0Meter(
            defaultMapSize = 4.mb,
            currentMapSize = 4.mb,
            mapsCount = 2
          )
      ) shouldBe Accelerator(8.mb, Some(Brake(brakeFor = 10.millisecond, releaseRate = 0.5.millisecond)))
    }

    "increase the map size when the map count is reached and multiple brakes with the number of overflown maps" in {
      Accelerator.brake(
        increaseMapSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 10.millisecond,
        releaseRate = 0.5.millisecond,
        increaseMapSizeBy = 4,
        maxMapSize = 20.mb
      )(
        level0Meter =
          Level0Meter(
            defaultMapSize = 4.mb,
            currentMapSize = 4.mb,
            mapsCount = 5
          )
      ) shouldBe Accelerator(16.mb, Some(Brake(brakeFor = 40.millisecond, releaseRate = 0.5.millisecond)))
    }

    "not allow mapSize to go over maxMapSize" in {
      Accelerator.brake(
        increaseMapSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 1.second,
        releaseRate = 100.millisecond,
        increaseMapSizeBy = 50,
        maxMapSize = 10.mb
      )(
        level0Meter =
          Level0Meter(
            defaultMapSize = 4.mb,
            currentMapSize = 4.mb,
            mapsCount = 2
          )
      ) shouldBe Accelerator(10.mb, Some(Brake(brakeFor = 1.second, releaseRate = 100.millisecond)))
    }

    "increment the currentMapSize and not the defaultMapSize" in {
      Accelerator.brake(
        increaseMapSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 1.second,
        releaseRate = 100.millisecond,
        increaseMapSizeBy = 2,
        maxMapSize = 20.mb
      )(
        level0Meter =
          Level0Meter(
            defaultMapSize = 4.mb,
            currentMapSize = 8.mb,
            mapsCount = 2
          )
      ) shouldBe Accelerator(16.mb, Some(Brake(brakeFor = 1.second, releaseRate = 100.millisecond)))
    }

  }

}
