/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class FunctionMerger_Remove_Spec extends WordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  "Merging a key function into Remove" when {
    "times are in order" should {
      "always return new key-value" in {
        runThis(1000.times) {
          implicit val testTimer = eitherOne(TestTimer.Incremental(), TestTimer.Empty)
          val key = randomBytesSlice()

          val oldKeyValue = randomRemoveKeyValue(key = key)(testTimer)

          val functionOutput = randomFunctionOutput()
          val newKeyValue = createFunction(key = key, randomRequiresKeyOnlyWithOptionDeadlineFunction(functionOutput))
          //
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
                if (oldKeyValue.deadline.isEmpty)
                  oldKeyValue.copy(time = newKeyValue.time)
                else
                  oldKeyValue.copy(deadline = Some(deadline), time = newKeyValue.time)

              case SwayFunctionOutput.Update(value, deadline) =>
                if (oldKeyValue.deadline.isEmpty)
                  oldKeyValue.copy(time = newKeyValue.time)
                else
                  Memory.Update(key = key, value = value, deadline = deadline.orElse(oldKeyValue.deadline), time = newKeyValue.time)
            }

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expected,
            lastLevel = oldKeyValue.toLastLevelExpected
          )
        }
      }
    }
  }

  "Merging a function that requires value into Remove" when {
    "times are in order" should {
      "always return new key-value" in {

        implicit val testTimer = TestTimer.Incremental()

        runThis(1000.times) {
          val key = randomBytesSlice()

          val oldKeyValue = randomRemoveKeyValue(key = key)(testTimer)

          val functionOutput = randomFunctionOutput()

          val newKeyValue = createFunction(key, randomRequiresValueWithOptionalKeyAndDeadlineFunction(functionOutput))

          //          println(s"oldKeyValue: $oldKeyValue")
          //          println(s"newKeyValue: $newKeyValue")
          //          println(s"function: ${functionStore.get(newKeyValue.function)}")
          //          println(s"functionOutput: $functionOutput")

          val expected =
          //if the old remove has no deadline set, then this is a remove.
            if (oldKeyValue.deadline.isEmpty)
              oldKeyValue.copy(time = newKeyValue.time)
            else //else the result should be merged because value is unknown from Remove key-value.
              Memory.PendingApply(key, Slice(oldKeyValue.toFromValue().runRandomIO.right.value, newKeyValue.toFromValue().runRandomIO.right.value))

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expected,
            lastLevel = oldKeyValue.toLastLevelExpected
          )
        }
      }
    }
  }
}
