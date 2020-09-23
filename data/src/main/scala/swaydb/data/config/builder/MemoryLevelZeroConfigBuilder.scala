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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.config.builder

import swaydb.data.OptimiseWrites
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.config.ConfigWizard
import swaydb.data.util.Java.JavaFunction

import scala.concurrent.duration.FiniteDuration

/**
 * Java Builder class for [[swaydb.data.config.MemoryLevelZeroConfig]]
 */
class MemoryLevelZeroConfigBuilder {
  private var mapSize: Long = _
  private var appliedFunctionsMapSize: Long = _
  private var clearAppliedFunctionsOnBoot: Boolean = _
  private var compactionExecutionContext: CompactionExecutionContext.Create = _
  private var acceleration: LevelZeroMeter => Accelerator = _
  private var optimiseWrites: OptimiseWrites = _
}

object MemoryLevelZeroConfigBuilder {

  class Step0(builder: MemoryLevelZeroConfigBuilder) {
    def mapSize(mapSize: Long) = {
      builder.mapSize = mapSize
      new Step1(builder)
    }
  }

  class Step1(builder: MemoryLevelZeroConfigBuilder) {
    def appliedFunctionsMapSize(mapSize: Long) = {
      builder.appliedFunctionsMapSize = mapSize
      new Step2(builder)
    }
  }

  class Step2(builder: MemoryLevelZeroConfigBuilder) {
    def clearAppliedFunctionsOnBoot(clear: Boolean) = {
      builder.clearAppliedFunctionsOnBoot = clear
      new Step3(builder)
    }
  }

  class Step3(builder: MemoryLevelZeroConfigBuilder) {
    def compactionExecutionContext(compactionExecutionContext: CompactionExecutionContext.Create) = {
      builder.compactionExecutionContext = compactionExecutionContext
      new Step4(builder)
    }
  }

  class Step4(builder: MemoryLevelZeroConfigBuilder) {
    def acceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      builder.acceleration = acceleration.apply
      new Step5(builder)
    }
  }

  class Step5(builder: MemoryLevelZeroConfigBuilder) {
    def acceleration(optimiseWrites: OptimiseWrites) = {
      builder.optimiseWrites = optimiseWrites
      new Step6(builder)
    }
  }

  class Step6(builder: MemoryLevelZeroConfigBuilder) {
    def throttle(throttle: JavaFunction[LevelZeroMeter, FiniteDuration]) =
      ConfigWizard.withMemoryLevel0(
        mapSize = builder.mapSize,
        appliedFunctionsMapSize = builder.appliedFunctionsMapSize,
        clearAppliedFunctionsOnBoot = builder.clearAppliedFunctionsOnBoot,
        compactionExecutionContext = builder.compactionExecutionContext,
        optimiseWrites = builder.optimiseWrites,
        acceleration = builder.acceleration,
        throttle = throttle.apply
      )
  }

  def builder() = new Step0(new MemoryLevelZeroConfigBuilder())
}
