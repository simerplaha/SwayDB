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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.data.accelerate

class NoBrakesBuilder {
  private var onMapCount: Int = _
  private var increaseMapSizeBy: Int = _
  private var maxMapSize: Long = _
}

object NoBrakesBuilder {

  class Step0(builder: NoBrakesBuilder) {
    def onMapCount(onMapCount: Int) = {
      builder.onMapCount = onMapCount
      new Step1(builder)
    }
  }

  class Step1(builder: NoBrakesBuilder) {
    def increaseMapSizeBy(increaseMapSizeBy: Int) = {
      builder.increaseMapSizeBy = increaseMapSizeBy
      new Step2(builder)
    }
  }

  class Step2(builder: NoBrakesBuilder) {
    def maxMapSize(maxMapSize: Long) = {
      builder.maxMapSize = maxMapSize
      new Step3(builder)
    }
  }

  class Step3(builder: NoBrakesBuilder) {
    def level0Meter(level0Meter: LevelZeroMeter) =
      Accelerator.noBrakes(
        onMapCount = builder.onMapCount,
        increaseMapSizeBy = builder.increaseMapSizeBy,
        maxMapSize = builder.maxMapSize
      )(level0Meter)
  }

  def builder() = new Step0(new NoBrakesBuilder())
}