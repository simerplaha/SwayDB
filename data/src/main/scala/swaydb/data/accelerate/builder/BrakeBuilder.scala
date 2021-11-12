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

package swaydb.data.accelerate.builder

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}

import java.time.Duration
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.FiniteDuration

class BrakeBuilder {
  private var increaseLogSizeOnMapCount: Int = _
  private var increaseLogSizeBy: Int = _
  private var maxLogSize: Int = _
  private var brakeOnMapCount: Int = _
  private var brakeFor: FiniteDuration = _
  private var releaseRate: FiniteDuration = _
  private var logAsWarning: Boolean = _
}

object BrakeBuilder {

  class Step0(builder: BrakeBuilder) {
    def increaseLogSizeOnMapCount(increaseLogSizeOnMapCount: Int) = {
      builder.increaseLogSizeOnMapCount = increaseLogSizeOnMapCount
      new Step1(builder)
    }
  }

  class Step1(builder: BrakeBuilder) {
    def increaseLogSizeBy(increaseLogSizeBy: Int) = {
      builder.increaseLogSizeBy = increaseLogSizeBy
      new Step2(builder)
    }
  }

  class Step2(builder: BrakeBuilder) {
    def maxLogSize(maxLogSize: Int) = {
      builder.maxLogSize = maxLogSize
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
        increaseLogSizeOnMapCount = builder.increaseLogSizeOnMapCount,
        increaseLogSizeBy = builder.increaseLogSizeBy,
        maxLogSize = builder.maxLogSize,
        brakeOnMapCount = builder.brakeOnMapCount,
        brakeFor = builder.brakeFor,
        releaseRate = builder.releaseRate,
        logAsWarning = builder.logAsWarning
      )(levelZeroMeter)
  }

  def builder() = new Step0(new BrakeBuilder())
}
