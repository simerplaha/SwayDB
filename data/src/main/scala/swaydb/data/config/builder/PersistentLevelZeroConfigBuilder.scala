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

package swaydb.data.config.builder

import java.nio.file.Path

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.config.{ConfigWizard, PersistentLevelZeroConfig, RecoveryMode}
import swaydb.data.util.Java.JavaFunction

import scala.concurrent.duration.FiniteDuration

/**
 * Java Builder class for [[PersistentLevelZeroConfig]]
 */
class PersistentLevelZeroConfigBuilder {
  private var dir: Path = _
  private var mapSize: Long = _
  private var mmap: Boolean = _
  private var recoveryMode: RecoveryMode = _
  private var compactionExecutionContext: CompactionExecutionContext.Create = _
  private var acceleration: LevelZeroMeter => Accelerator = _
}

object PersistentLevelZeroConfigBuilder {

  class Step0(builder: PersistentLevelZeroConfigBuilder) {
    def withDir(dir: Path) = {
      builder.dir = dir
      new Step1(builder)
    }
  }

  class Step1(builder: PersistentLevelZeroConfigBuilder) {
    def withMapSize(mapSize: Long) = {
      builder.mapSize = mapSize
      new Step2(builder)
    }
  }

  class Step2(builder: PersistentLevelZeroConfigBuilder) {
    def withMmap(mmap: Boolean) = {
      builder.mmap = mmap
      new Step3(builder)
    }
  }

  class Step3(builder: PersistentLevelZeroConfigBuilder) {
    def withRecoveryMode(recoveryMode: RecoveryMode) = {
      builder.recoveryMode = recoveryMode
      new Step4(builder)
    }
  }

  class Step4(builder: PersistentLevelZeroConfigBuilder) {
    def withCompactionExecutionContext(compactionExecutionContext: CompactionExecutionContext.Create) = {
      builder.compactionExecutionContext = compactionExecutionContext
      new Step5(builder)
    }
  }

  class Step5(builder: PersistentLevelZeroConfigBuilder) {
    def withAcceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      builder.acceleration = acceleration.apply
      new Step6(builder)
    }
  }

  class Step6(builder: PersistentLevelZeroConfigBuilder) {
    def withThrottle(throttle: JavaFunction[LevelZeroMeter, FiniteDuration]) =
      ConfigWizard.withPersistentLevel0(
        dir = builder.dir,
        mapSize = builder.mapSize,
        mmap = builder.mmap,
        recoveryMode = builder.recoveryMode,
        compactionExecutionContext = builder.compactionExecutionContext,
        acceleration = builder.acceleration,
        throttle = throttle.apply
      )
  }

  def builder() = new Step0(new PersistentLevelZeroConfigBuilder())
}
