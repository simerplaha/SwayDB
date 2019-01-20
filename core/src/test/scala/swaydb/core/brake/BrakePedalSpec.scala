/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.brake

import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.duration._

class BrakePedalSpec extends WordSpec with Matchers {

  "BrakePedal" should {
    "continue applying brakes and release brakes until complete" in {

      val pedal = new BrakePedal(100.millisecond, 1.millisecond)

      var brakesApplied = 1

      while (!pedal.applyBrakes())
        brakesApplied += 1

      //100 brakes should be applied
      brakesApplied shouldBe 100
    }
  }
}
