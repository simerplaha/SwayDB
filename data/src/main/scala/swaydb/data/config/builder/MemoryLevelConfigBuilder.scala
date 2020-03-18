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

import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config.MemoryLevelConfig
import swaydb.data.util.Java.JavaFunction

/**
 * Java Builder class for [[MemoryLevelConfig]]
 */
class MemoryLevelConfigBuilder {
  private var minSegmentSize: Int = _
  private var maxKeyValuesPerSegment: Int = _
  private var copyForward: Boolean = _
  private var deleteSegmentsEventually: Boolean = _
  private var compactionExecutionContext: CompactionExecutionContext = _
  private var throttle: JavaFunction[LevelMeter, Throttle] = _
}

object MemoryLevelConfigBuilder {

  class Step0(builder: MemoryLevelConfigBuilder) {
    def withMinSegmentSize(minSegmentSize: Int) = {
      builder.minSegmentSize = minSegmentSize
      new Step1(builder)
    }
  }

  class Step1(builder: MemoryLevelConfigBuilder) {
    def withMaxKeyValuesPerSegment(maxKeyValuesPerSegment: Int) = {
      builder.maxKeyValuesPerSegment = maxKeyValuesPerSegment
      new Step2(builder)
    }
  }

  class Step2(builder: MemoryLevelConfigBuilder) {
    def withCopyForward(copyForward: Boolean) = {
      builder.copyForward = copyForward
      new Step3(builder)
    }
  }

  class Step3(builder: MemoryLevelConfigBuilder) {
    def withDeleteSegmentsEventually(deleteSegmentsEventually: Boolean) = {
      builder.deleteSegmentsEventually = deleteSegmentsEventually
      new Step4(builder)
    }
  }

  class Step4(builder: MemoryLevelConfigBuilder) {
    def withCompactionExecutionContext(compactionExecutionContext: CompactionExecutionContext) = {
      builder.compactionExecutionContext = compactionExecutionContext
      new Step5(builder)
    }
  }

  class Step5(builder: MemoryLevelConfigBuilder) {
    def withThrottle(throttle: JavaFunction[LevelMeter, Throttle]) = {
      builder.throttle = throttle
      new Step6(builder)
    }
  }

  class Step6(builder: MemoryLevelConfigBuilder) {
    def build(): MemoryLevelConfig =
      MemoryLevelConfig(
        minSegmentSize = builder.minSegmentSize,
        maxKeyValuesPerSegment = builder.maxKeyValuesPerSegment,
        copyForward = builder.copyForward,
        deleteSegmentsEventually = builder.deleteSegmentsEventually,
        compactionExecutionContext = builder.compactionExecutionContext,
        throttle = builder.throttle.apply
      )
  }

  def builder() = new Step0(new MemoryLevelConfigBuilder())
}