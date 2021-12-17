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

package swaydb.core.log

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class BrakePedalSpec extends AnyWordSpec {

  "BrakePedal" should {
    "continue applying brakes and release brakes until complete" in {

      val pedal =
        new BrakePedal(
          brakeFor = 100.millisecond,
          releaseRate = 1.millisecond,
          logAsWarning = true
        )

      var brakesApplied = 1

      while (!pedal.applyBrakes())
        brakesApplied += 1

      //100 brakes should be applied
      brakesApplied shouldBe 100
    }
  }
}
