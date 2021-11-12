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

class NoBrakesBuilder {
  private var onMapCount: Int = _
  private var increaseLogSizeBy: Int = _
  private var maxLogSize: Int = _
}

object NoBrakesBuilder {

  class Step0(builder: NoBrakesBuilder) {
    def onMapCount(onMapCount: Int) = {
      builder.onMapCount = onMapCount
      new Step1(builder)
    }
  }

  class Step1(builder: NoBrakesBuilder) {
    def increaseLogSizeBy(increaseLogSizeBy: Int) = {
      builder.increaseLogSizeBy = increaseLogSizeBy
      new Step2(builder)
    }
  }

  class Step2(builder: NoBrakesBuilder) {
    def maxLogSize(maxLogSize: Int) = {
      builder.maxLogSize = maxLogSize
      new Step3(builder)
    }
  }

  class Step3(builder: NoBrakesBuilder) {
    def level0Meter(level0Meter: LevelZeroMeter) =
      Accelerator.noBrakes(
        onMapCount = builder.onMapCount,
        increaseLogSizeBy = builder.increaseLogSizeBy,
        maxLogSize = builder.maxLogSize
      )(level0Meter)
  }

  def builder() = new Step0(new NoBrakesBuilder())
}
