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

package swaydb.config.builder

import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.config.compaction.LevelZeroThrottle
import swaydb.config.{ConfigWizard, MMAP, RecoveryMode}
import swaydb.config.{Atomic, OptimiseWrites}
import swaydb.utils.Java.JavaFunction

import java.nio.file.Path

/**
 * Java Builder class for [[swaydb.config.PersistentLevelZeroConfig]]
 */
class PersistentLevelZeroConfigBuilder {
  private var dir: Path = _
  private var logSize: Int = _
  private var appliedFunctionsLogSize: Int = _
  private var clearAppliedFunctionsOnBoot: Boolean = _
  private var mmap: MMAP.Log = _
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
    def logSize(logSize: Int) = {
      builder.logSize = logSize
      new Step2(builder)
    }
  }

  class Step2(builder: PersistentLevelZeroConfigBuilder) {
    def appliedFunctionsLogSize(logSize: Int) = {
      builder.appliedFunctionsLogSize = logSize
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
    def mmap(mmap: MMAP.Log) = {
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
        logSize = builder.logSize,
        appliedFunctionsLogSize = builder.appliedFunctionsLogSize,
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
