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

package swaydb.data.config.builder

import java.time.Duration

import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config.MemoryLevelConfig
import swaydb.data.util.Java.JavaFunction

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration

/**
 * Java Builder class for [[MemoryLevelConfig]]
 */
class MemoryLevelConfigBuilder {
  private var minSegmentSize: Int = _
  private var maxKeyValuesPerSegment: Int = _
  private var copyForward: Boolean = _
  private var deleteDelay: FiniteDuration = _
  private var compactionExecutionContext: CompactionExecutionContext = _
}

object MemoryLevelConfigBuilder {

  class Step0(builder: MemoryLevelConfigBuilder) {
    def minSegmentSize(minSegmentSize: Int) = {
      builder.minSegmentSize = minSegmentSize
      new Step1(builder)
    }
  }

  class Step1(builder: MemoryLevelConfigBuilder) {
    def maxKeyValuesPerSegment(maxKeyValuesPerSegment: Int) = {
      builder.maxKeyValuesPerSegment = maxKeyValuesPerSegment
      new Step2(builder)
    }
  }

  class Step2(builder: MemoryLevelConfigBuilder) {
    def copyForward(copyForward: Boolean) = {
      builder.copyForward = copyForward
      new Step3(builder)
    }
  }

  class Step3(builder: MemoryLevelConfigBuilder) {
    def deleteDelay(deleteDelay: Duration) = {
      builder.deleteDelay = deleteDelay.toScala
      new Step4(builder)
    }
  }

  class Step4(builder: MemoryLevelConfigBuilder) {
    def compactionExecutionContext(compactionExecutionContext: CompactionExecutionContext) = {
      builder.compactionExecutionContext = compactionExecutionContext
      new Step5(builder)
    }
  }

  class Step5(builder: MemoryLevelConfigBuilder) {
    def throttle(throttle: JavaFunction[LevelMeter, Throttle]) =
      new MemoryLevelConfig(
        minSegmentSize = builder.minSegmentSize,
        maxKeyValuesPerSegment = builder.maxKeyValuesPerSegment,
        copyForward = builder.copyForward,
        deleteDelay = builder.deleteDelay,
        compactionExecutionContext = builder.compactionExecutionContext,
        throttle = throttle.apply
      )
  }

  def builder() = new Step0(new MemoryLevelConfigBuilder())
}
