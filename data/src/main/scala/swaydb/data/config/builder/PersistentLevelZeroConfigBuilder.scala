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

package swaydb.data.config.builder

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.LevelZeroThrottle
import swaydb.data.config.{ConfigWizard, MMAP, RecoveryMode}
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.utils.Java.JavaFunction

import java.nio.file.Path

/**
 * Java Builder class for [[swaydb.data.config.PersistentLevelZeroConfig]]
 */
class PersistentLevelZeroConfigBuilder {
  private var dir: Path = _
  private var mapSize: Long = _
  private var appliedFunctionsMapSize: Long = _
  private var clearAppliedFunctionsOnBoot: Boolean = _
  private var mmap: MMAP.Map = _
  private var recoveryMode: RecoveryMode = _
  private var acceleration: LevelZeroMeter => Accelerator = _
  private var optimiseWrites: OptimiseWrites = _
  private var atomic: Atomic = _
}

object PersistentLevelZeroConfigBuilder {

  class Step0(builder: PersistentLevelZeroConfigBuilder) {
    def dir(dir: Path) = {
      builder.dir = dir
      new Step1(builder)
    }
  }

  class Step1(builder: PersistentLevelZeroConfigBuilder) {
    def mapSize(mapSize: Long) = {
      builder.mapSize = mapSize
      new Step2(builder)
    }
  }

  class Step2(builder: PersistentLevelZeroConfigBuilder) {
    def appliedFunctionsMapSize(mapSize: Long) = {
      builder.appliedFunctionsMapSize = mapSize
      new Step3(builder)
    }
  }

  class Step3(builder: PersistentLevelZeroConfigBuilder) {
    def clearAppliedFunctionsOnBoot(clear: Boolean) = {
      builder.clearAppliedFunctionsOnBoot = clear
      new Step4(builder)
    }
  }

  class Step4(builder: PersistentLevelZeroConfigBuilder) {
    def mmap(mmap: MMAP.Map) = {
      builder.mmap = mmap
      new Step5(builder)
    }
  }

  class Step5(builder: PersistentLevelZeroConfigBuilder) {
    def recoveryMode(recoveryMode: RecoveryMode) = {
      builder.recoveryMode = recoveryMode
      new Step6(builder)
    }
  }

  class Step6(builder: PersistentLevelZeroConfigBuilder) {
    def acceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      builder.acceleration = acceleration.apply
      new Step8(builder)
    }
  }

  class Step8(builder: PersistentLevelZeroConfigBuilder) {
    def acceleration(optimiseWrites: OptimiseWrites) = {
      builder.optimiseWrites = optimiseWrites
      new Step9(builder)
    }
  }

  class Step9(builder: PersistentLevelZeroConfigBuilder) {
    def atomic(atomic: Atomic) = {
      builder.atomic = atomic
      new Step10(builder)
    }
  }

  class Step10(builder: PersistentLevelZeroConfigBuilder) {
    def throttle(throttle: JavaFunction[LevelZeroMeter, LevelZeroThrottle]) =
      ConfigWizard.withPersistentLevel0(
        dir = builder.dir,
        mapSize = builder.mapSize,
        appliedFunctionsMapSize = builder.appliedFunctionsMapSize,
        clearAppliedFunctionsOnBoot = builder.clearAppliedFunctionsOnBoot,
        mmap = builder.mmap,
        recoveryMode = builder.recoveryMode,
        optimiseWrites = builder.optimiseWrites,
        acceleration = builder.acceleration,
        atomic = builder.atomic,
        throttle = throttle.apply
      )
  }

  def builder() = new Step0(new PersistentLevelZeroConfigBuilder())
}
