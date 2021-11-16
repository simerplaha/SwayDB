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

import swaydb.config.compaction.{LevelMeter, LevelThrottle}
import swaydb.config.MemoryLevelConfig
import swaydb.utils.Java.JavaFunction

import java.time.Duration
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration

/**
 * Java Builder class for [[MemoryLevelConfig]]
 */
class MemoryLevelConfigBuilder {
  private var minSegmentSize: Int = _
  private var maxKeyValuesPerSegment: Int = _
  private var deleteDelay: FiniteDuration = _
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
    def deleteDelay(deleteDelay: Duration) = {
      builder.deleteDelay = deleteDelay.toScala
      new Step3(builder)
    }
  }

  class Step3(builder: MemoryLevelConfigBuilder) {
    def throttle(throttle: JavaFunction[LevelMeter, LevelThrottle]) =
      new MemoryLevelConfig(
        minSegmentSize = builder.minSegmentSize,
        maxKeyValuesPerSegment = builder.maxKeyValuesPerSegment,
        deleteDelay = builder.deleteDelay,
        throttle = throttle.apply
      )
  }

  def builder() = new Step0(new MemoryLevelConfigBuilder())
}
