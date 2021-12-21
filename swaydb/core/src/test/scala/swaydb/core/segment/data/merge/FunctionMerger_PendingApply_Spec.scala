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
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.data.Memory
import swaydb.core.segment.data.merge.SegmentMergeTestKit._
import swaydb.effect.IOValues._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.SliceTestKit._
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._


class FunctionMerger_PendingApply_Spec extends AnyWordSpec {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  private implicit val testFunctionStore: TestCoreFunctionStore = TestCoreFunctionStore()
  private implicit val functionStore: CoreFunctionStore = testFunctionStore.store

  private implicit val testTimer = TestTimer.Incremental()

  "Merging Function into PendingApply with a single apply" when {
    "times are in order" should {
      "always return the same result as the Function being merged into Fixed" in {
        runThis(1000.times) {
          val key = genBytesSlice()

          val apply = randomApplyWithDeadline()
          //oldKeyValue but it has a newer time.
          val oldKeyValue = Memory.PendingApply(key = key, Slice(apply))

          //new but has older time than oldKeyValue
          val newKeyValue = randomFunctionKeyValue(key = key)

          val expected = FixedMerger(newKeyValue, apply.toMemory(key)).runRandomIO.get

          //          println(s"newKeyValue: $newKeyValue")
          //          println(s"old apply: $apply")
          //          println(s"oldKeyValue: $oldKeyValue")
          //          println(s"expected: $expected")
          //          println(s"function: ${functionStore.get(newKeyValue.function)}")

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expected.asInstanceOf[Memory.Fixed],
            lastLevel = expected.asInstanceOf[Memory.Fixed].toLastLevelExpected
          )
        }
      }
    }
  }

  "Merging Function into PendingApply with multiple apply" when {
    "times are in order" should {
      "always return the same result as the Function being merged into Fixed" in {
        runThis(1000.times) {
          val key = eitherOne(genBytesSlice(), Slice.emptyBytes)

          val oldApplies =
            (1 to randomIntMax(20)) map {
              _ => randomApplyWithDeadline()
            } toSlice

          val newKeyValue = randomFunctionKeyValue(key = key)

          val oldKeyValue = Memory.PendingApply(key = key, oldApplies)

          val expected = collapseMerge(newKeyValue, oldApplies)

          //          println(s"newKeyValue: $newKeyValue")
          //          println(s"old applies: $oldApplies")
          //          println(s"oldKeyValue: $oldKeyValue")
          ////          println(s"function result: ${functionStore.get(newKeyValue.function).get.asInstanceOf[coreFunction.Key].f(genBytesSlice())}")
          //          println(s"expected: $expected")
          //          println

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = expected.asInstanceOf[Memory.Fixed],
            lastLevel = expected.asInstanceOf[Memory.Fixed].toLastLevelExpected
          )
        }
      }
    }
  }
}
