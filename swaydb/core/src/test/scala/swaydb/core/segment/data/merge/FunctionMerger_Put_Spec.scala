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

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.log.timer.TestTimer
import swaydb.core.segment.{CoreFunctionStore, TestCoreFunctionStore}
import swaydb.core.segment.data._
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.data.merge.SegmentMergeTestKit._
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.SliceTestKit._
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class FunctionMerger_Put_Spec extends AnyWordSpec {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  private implicit val testFunctionStore: TestCoreFunctionStore = TestCoreFunctionStore()
  private implicit val functionStore: CoreFunctionStore = testFunctionStore.store

  "Merging any function into Put" when {
    "times are in order" should {
      "always return new key-value" in {
        runThis(1000.times) {

          implicit val testTimer: TestTimer = eitherOne(TestTimer.Incremental(), TestTimer.Empty)
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
              case SegmentFunctionOutput.Remove =>
                Memory.Remove(key, None, newKeyValue.time)

              case SegmentFunctionOutput.Nothing =>
                oldKeyValue.copy(time = newKeyValue.time)

              case SegmentFunctionOutput.Expire(deadline) =>
                oldKeyValue.copy(deadline = Some(deadline), time = newKeyValue.time)

              case SegmentFunctionOutput.Update(value, deadline) =>
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
