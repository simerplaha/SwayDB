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

package swaydb

import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}

import scala.concurrent.duration._

case object CommonConfigs {
  //4098 being the default file-system blockSize.
  def mapSize: Int = 4098.bytes * 1000

  def segmentSize: Int = 4098.bytes * 500

  def accelerator: LevelZeroMeter => Accelerator =
    Accelerator.brake(
      increaseMapSizeOnMapCount = 3,
      increaseMapSizeBy = 2,
      maxMapSize = CommonConfigs.mapSize * 3,
      brakeOnMapCount = 6,
      brakeFor = 10.milliseconds,
      releaseRate = 0.1.millisecond,
      logAsWarning = false
    )

  def mergeParallelism(): Int =
    Runtime.getRuntime.availableProcessors() / 3

  def optimiseWrites(): OptimiseWrites =
    OptimiseWrites.RandomOrder

  def atomic(): Atomic =
    Atomic.Off

}
