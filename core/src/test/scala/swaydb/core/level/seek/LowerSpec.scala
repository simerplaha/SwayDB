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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level.seek

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.Value
import swaydb.core.{TestData, TestTimer}
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.serializers.Default._
import swaydb.serializers._

class LowerSpec extends AnyWordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore
  implicit val time = TestTimer.Empty

  "lowerFromValue" in {
    runThis(100.times) {
      Lower.lowerFromValue(key = 1, fromKey = 2, fromValue = Value.FromValue.Null).toOptionPut shouldBe empty
      Lower.lowerFromValue(key = 2, fromKey = 1, fromValue = Value.FromValue.Null).toOptionPut shouldBe empty

      Lower.lowerFromValue(key = 2, fromKey = 1, fromValue = randomFromValueOption(addPut = false)).toOptionPut shouldBe empty
      Lower.lowerFromValue(key = 1, fromKey = 2, fromValue = randomFromValueOption(addPut = false)).toOptionPut shouldBe empty

      Lower.lowerFromValue(key = 2, fromKey = 1, fromValue = Value.put(randomStringSliceOptional, Some(expiredDeadline()))).toOptionPut shouldBe empty
      Lower.lowerFromValue(key = 1, fromKey = 2, fromValue = Value.put(randomStringSliceOptional, Some(expiredDeadline()))).toOptionPut shouldBe empty

      val put = Value.put(randomStringSliceOptional, randomDeadlineOption(false))
      Lower.lowerFromValue(key = 2, fromKey = 1, fromValue = put) shouldBe put.toMemory(1)
      Lower.lowerFromValue(key = 1, fromKey = 2, fromValue = put).toOptionPut shouldBe empty
    }
  }
}
