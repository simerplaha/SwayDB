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

package swaydb.core.merge

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data._
import swaydb.testkit.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class FunctionMerger_Put_Spec extends AnyWordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
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
