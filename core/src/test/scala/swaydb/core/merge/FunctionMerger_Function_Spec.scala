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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.merge

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.{CommonAssertions, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._
import swaydb.data.slice.Slice

class FunctionMerger_Function_Spec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def compression = randomGroupingStrategyOption(randomNextInt(1000))

  "Merging Function into function" when {

    "times are in order" should {

      "always return PendingApply" in {

        implicit val testTimer = TestTimer.Incremental()

        runThis(1000.times) {
          val key = randomBytesSlice()

          //new but has older time than oldKeyValue
          val newKeyValue = randomFunctionKeyValue(key = key)

          //oldKeyValue but it has a newer time.
          val oldKeyValue = randomFixedKeyValue(key = key)

          //          println(s"oldKeyValue: $oldKeyValue")
          //          println(s"newKeyValue: $newKeyValue")

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = oldKeyValue,
            lastLevel = oldKeyValue.toLastLevelExpected
          )
        }
      }
    }
  }
}
