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

package swaydb.core.segment.data.merge

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.TestTimer
import swaydb.core.segment.data._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class FunctionMerger_Remove_Spec extends AnyWordSpec with Matchers {

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

          val expected: Memory.Fixed =
            functionOutput match {
              case SegmentFunctionOutput.Remove =>
                Memory.Remove(key, None, newKeyValue.time)

              case SegmentFunctionOutput.Nothing =>
                oldKeyValue.copy(time = newKeyValue.time)

              case SegmentFunctionOutput.Expire(deadline) =>
                if (oldKeyValue.deadline.isEmpty)
                  oldKeyValue.copy(time = newKeyValue.time)
                else
                  oldKeyValue.copy(deadline = Some(deadline), time = newKeyValue.time)

              case SegmentFunctionOutput.Update(value, deadline) =>
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
