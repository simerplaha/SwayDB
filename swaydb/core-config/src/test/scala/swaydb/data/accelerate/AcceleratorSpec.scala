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

package swaydb.config.accelerate

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.utils.StorageUnits._

import scala.concurrent.duration._

class AcceleratorSpec extends AnyWordSpec {

  "BlockingBrakeAccelerator" should {
    "not apply brakes and not increase the map size when map count is not reached" in {
      Accelerator.brake(
        increaseLogSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 10.millisecond,
        releaseRate = 0.5.millisecond,
        increaseLogSizeBy = 5,
        maxLogSize = 10.mb
      )(
        levelZeroMeter =
          new LevelZeroMeter {
            def defaultLogSize = 4.mb
            def currentLogSize = 4.mb
            def logsCount = 1
          }
      ) shouldBe Accelerator(nextLogSize = 4.mb, brake = None)
    }

    "increase the map size when the map count is reached and apply brakes" in {
      Accelerator.brake(
        increaseLogSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 10.millisecond,
        releaseRate = 0.5.millisecond,
        increaseLogSizeBy = 2,
        maxLogSize = 10.mb
      )(
        levelZeroMeter =
          new LevelZeroMeter {
            def defaultLogSize = 4.mb
            def currentLogSize = 4.mb
            def logsCount = 2
          }
      ) shouldBe Accelerator(8.mb, Some(Brake(brakeFor = 10.millisecond, releaseRate = 0.5.millisecond, logAsWarning = true)))
    }

    "increase the map size when the map count is reached and multiple brakes with the number of overflown maps" in {
      Accelerator.brake(
        increaseLogSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 10.millisecond,
        releaseRate = 0.5.millisecond,
        increaseLogSizeBy = 4,
        maxLogSize = 20.mb
      )(
        levelZeroMeter =
          new LevelZeroMeter {
            def defaultLogSize = 4.mb
            def currentLogSize = 4.mb
            def logsCount = 5
          }
      ) shouldBe Accelerator(16.mb, Some(Brake(brakeFor = 40.millisecond, releaseRate = 0.5.millisecond, logAsWarning = true)))
    }

    "not allow logSize to go over maxLogSize" in {
      Accelerator.brake(
        increaseLogSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 1.second,
        releaseRate = 100.millisecond,
        increaseLogSizeBy = 50,
        maxLogSize = 10.mb
      )(
        levelZeroMeter =
          new LevelZeroMeter {
            def defaultLogSize = 4.mb
            def currentLogSize = 4.mb
            def logsCount = 2
          }
      ) shouldBe Accelerator(10.mb, Some(Brake(brakeFor = 1.second, releaseRate = 100.millisecond, logAsWarning = true)))
    }

    "increment the currentLogSize and not the defaultLogSize" in {
      Accelerator.brake(
        increaseLogSizeOnMapCount = 2,
        brakeOnMapCount = 2,
        brakeFor = 1.second,
        releaseRate = 100.millisecond,
        increaseLogSizeBy = 2,
        maxLogSize = 20.mb
      )(
        levelZeroMeter =
          new LevelZeroMeter {
            def defaultLogSize = 4.mb
            def currentLogSize = 8.mb
            def logsCount = 2
          }
      ) shouldBe Accelerator(16.mb, Some(Brake(brakeFor = 1.second, releaseRate = 100.millisecond, logAsWarning = true)))
    }
  }
}
