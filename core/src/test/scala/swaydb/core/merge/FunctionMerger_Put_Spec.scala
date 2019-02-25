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

package swaydb.core.merge

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.data._
import swaydb.core.{CommonAssertions, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._
import swaydb.data.slice.Slice

class FunctionMerger_Put_Spec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def groupingStrategy = randomGroupingStrategyOption(randomNextInt(1000))

  "Merging any function into Put" when {
    "times are in order" should {
      "always return new key-value" in {
        runThis(1000.times) {

          implicit val testTimer = eitherOne(TestTimer.Incremental(), TestTimer.Empty)
          val key = randomBytesSlice()

          val oldKeyValue = randomPutKeyValue(key = key)(testTimer)

          val functionOutput = randomFunctionOutput()
          val newKeyValue = randomFunctionKeyValue(key, functionOutput)

          //          println(s"oldKeyValue: $oldKeyValue")
          //          println(s"newKeyValue: $newKeyValue")
          //          println(s"function: ${functionStore.get(newKeyValue.function)}")
          //          println(s"functionOutput: $functionOutput")

          val expected =
            functionOutput match {
              case SwayFunctionOutput.Remove =>
                Memory.Remove(key, None, newKeyValue.time)

              case SwayFunctionOutput.Nothing =>
                oldKeyValue.copy(time = newKeyValue.time)

              case SwayFunctionOutput.Expire(deadline) =>
                oldKeyValue.copy(deadline = Some(deadline), time = newKeyValue.time)

              case SwayFunctionOutput.Update(value, deadline) =>
                oldKeyValue.copy(value = value, deadline = deadline.orElse(oldKeyValue.deadline), time = newKeyValue.time)
            }

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expected,
            lastLevel = expected.toLastLevelExpected
          )
        }
      }
    }
  }
}
