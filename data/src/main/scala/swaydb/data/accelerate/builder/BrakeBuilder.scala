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

package swaydb.data.accelerate.builder

import java.time.Duration

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration

class BrakeBuilder {
  private var increaseMapSizeOnMapCount: Int = _
  private var increaseMapSizeBy: Int = _
  private var maxMapSize: Long = _
  private var brakeOnMapCount: Int = _
  private var brakeFor: FiniteDuration = _
  private var releaseRate: FiniteDuration = _
  private var logAsWarning: Boolean = _
}

object BrakeBuilder {

  class Step0(builder: BrakeBuilder) {
    def increaseMapSizeOnMapCount(increaseMapSizeOnMapCount: Int) = {
      builder.increaseMapSizeOnMapCount = increaseMapSizeOnMapCount
      new Step1(builder)
    }
  }

  class Step1(builder: BrakeBuilder) {
    def increaseMapSizeBy(increaseMapSizeBy: Int) = {
      builder.increaseMapSizeBy = increaseMapSizeBy
      new Step2(builder)
    }
  }

  class Step2(builder: BrakeBuilder) {
    def maxMapSize(maxMapSize: Long) = {
      builder.maxMapSize = maxMapSize
      new Step3(builder)
    }
  }

  class Step3(builder: BrakeBuilder) {
    def brakeOnMapCount(brakeOnMapCount: Int) = {
      builder.brakeOnMapCount = brakeOnMapCount
      new Step4(builder)
    }
  }

  class Step4(builder: BrakeBuilder) {
    def brakeFor(brakeFor: Duration) = {
      builder.brakeFor = brakeFor.toScala
      new Step5(builder)
    }
  }

  class Step5(builder: BrakeBuilder) {
    def releaseRate(releaseRate: Duration) = {
      builder.releaseRate = releaseRate.toScala
      new Step6(builder)
    }
  }

  class Step6(builder: BrakeBuilder) {
    def logAsWarning(logAsWarning: Boolean) = {
      builder.logAsWarning = logAsWarning
      new Step7(builder)
    }
  }

  class Step7(builder: BrakeBuilder) {
    def levelZeroMeter(levelZeroMeter: LevelZeroMeter) =
      Accelerator.brake(
        increaseMapSizeOnMapCount = builder.increaseMapSizeOnMapCount,
        increaseMapSizeBy = builder.increaseMapSizeBy,
        maxMapSize = builder.maxMapSize,
        brakeOnMapCount = builder.brakeOnMapCount,
        brakeFor = builder.brakeFor,
        releaseRate = builder.releaseRate,
        logAsWarning = builder.logAsWarning
      )(levelZeroMeter)
  }

  def builder() = new Step0(new BrakeBuilder())
}
