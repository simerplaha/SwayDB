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

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb.OK
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.{TestData, TestTimer}
import swaydb.core.segment.data._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._

class FunctionMerger_Update_Spec extends AnyWordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  "Merging SwayFunction Key/Value/KeyValue into Update" when {
    "times are in order" should {
      "always return new key-value" in {

        implicit val testTimer: TestTimer = TestTimer.Empty

        runThis(1000.times) {
          val key = randomBytesSlice()

          val oldKeyValue = randomUpdateKeyValue(key = key)(testTimer)

          val functionOutput = randomFunctionOutput()

          val newKeyValue: Memory.Function =
            createFunction(
              key = key,
              eitherOne(
                SwayFunction.Key(_ => functionOutput),
                SwayFunction.KeyValue((_, _) => functionOutput),
                SwayFunction.Value(_ => functionOutput)
              )
            )

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
            lastLevel = oldKeyValue.toLastLevelExpected
          )
        }
      }
    }
  }

  "Merging SwayFunction that requires deadline KeyDeadline/KeyValueDeadline/ValueDeadline into Update" when {
    "times are in order" should {
      "always return new key-value" in {
        runThis(1000.times) {

          implicit val testTimer: TestTimer = eitherOne(TestTimer.Incremental(), TestTimer.Empty)
          val key = randomBytesSlice()

          val oldKeyValue = randomUpdateKeyValue(key = key)(testTimer)

          val functionOutput = randomFunctionOutput()

          val newKeyValue: Memory.Function =
            createFunction(
              key = key,
              eitherOne(
                SwayFunction.KeyDeadline((_, _) => functionOutput),
                SwayFunction.ValueDeadline((_, _) => functionOutput),
                SwayFunction.KeyValueDeadline((_, _, _) => functionOutput)
              )
            )

          val expected =
            if (oldKeyValue.deadline.isDefined)
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
            else
              Memory.PendingApply(key = key, applies = Slice(oldKeyValue.toFromValue().runRandomIO.right.value, newKeyValue.toFromValue().runRandomIO.right.value))

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

  "Merging Function into Update" when {
    "key is required but key is not supplied" should {
      "stash the updates" in {
        runThis(100.times) {
          //mock functions are never called
          implicit val testTimer = TestTimer.Incremental()
          Seq(
            mock[SwayFunction.Key],
            mock[SwayFunction.KeyDeadline],
            mock[SwayFunction.KeyValue],
            mock[SwayFunction.KeyValueDeadline]
          ) foreach {
            swayFunction =>
              val oldKeyValue = randomUpdateKeyValue(Slice.emptyBytes)
              val newKeyValue = createFunction(Slice.emptyBytes, swayFunction)

              assertMerge(
                newKeyValue = newKeyValue,
                oldKeyValue = oldKeyValue,
                expected = Memory.PendingApply(Slice.emptyBytes, Slice(oldKeyValue.toFromValue().runRandomIO.right.value, newKeyValue.toFromValue().runRandomIO.right.value)),
                lastLevel = None
              )
          }
        }
      }
    }

    "the deadline is required but deadline is not set in update" should {
      "stash the updates" in {
        runThis(100.times) {
          //mock functions are never called
          implicit val testTimer = TestTimer.Incremental()
          Seq(
            mock[SwayFunction.ValueDeadline]
          ) foreach {
            swayFunction =>
              val oldKeyValue = randomUpdateKeyValue(1, deadline = None)
              val newKeyValue = createFunction(1, swayFunction)

              assertMerge(
                newKeyValue = newKeyValue,
                oldKeyValue = oldKeyValue,
                expected = Memory.PendingApply(1, Slice(oldKeyValue.toFromValue().runRandomIO.right.value, newKeyValue.toFromValue().runRandomIO.right.value)),
                lastLevel = None
              )
          }
        }
      }
    }

    "deadline is set" should {
      "success when key, deadline and value is set" in {
        runThis(100.times) {
          //mock functions are never called
          implicit val testTimer = TestTimer.Incremental()
          val output = SwayFunctionOutput.Update((randomStringOption: Slice[Byte]).asSliceOption(), Some(randomDeadline()))

          Seq(
            SwayFunction.Key(_ => output),
            SwayFunction.KeyValue((_, _) => output),
            SwayFunction.KeyDeadline((_, _) => output),
            SwayFunction.KeyValueDeadline((_, _, _) => output),
            SwayFunction.Value(_ => output),
            SwayFunction.ValueDeadline((_, _) => output)
          ) foreach {
            swayFunction =>
              val oldKeyValue = randomUpdateKeyValue(1, deadline = Some(randomDeadline()))
              val newKeyValue = createFunction(1, swayFunction)

              val expect = Memory.Update(1, output.value, output.deadline, newKeyValue.time)

              assertMerge(
                newKeyValue = newKeyValue,
                oldKeyValue = oldKeyValue,
                expected = expect,
                lastLevelExpect = Memory.Null
              )
          }
        }
      }
    }

    "result in pure final value" when {
      "multiple levels merge the same function multiple times" in {
        runThis(10.times) {
          //Suppose Level1 and Level2 merge a function which Level2 and Level3
          //had already merge resulting in a new function in Level3.
          //The final merge between the previous two merges
          //should still result in the same value

          val function =
            SwayFunction.KeyValue(
              (key, value) => {
                key shouldBe 1.serialise
                SwayFunctionOutput.Update(value.getC.readInt() + 1, None)
              }
            )

          val functionId = functionIdGenerator.incrementAndGet()
          functionStore.put(functionId, function) shouldBe OK.instance

          val functionLevel1 = Memory.Function(1, function = functionId, time = Time(3))
          val functionLevel2 = Memory.Function(1, function = functionId, time = Time(2))
          val functionLevel3 = Memory.Function(1, function = functionId, time = Time(1))

          //merge LEVEL1 and LEVEL2
          val level1And2Merge =
            TestData.merge(
              newKeyValues = Slice(functionLevel1),
              oldKeyValues = Slice(functionLevel2),
              isLastLevel = false
            )

          level1And2Merge should contain only Memory.PendingApply(1, Slice(Value.Function(functionId, Time(2)), Value.Function(functionId, Time(3))))

          //merge LEVEL3 and LEVEL3
          val level2And3Merge =
            TestData.merge(
              newKeyValues = Slice(functionLevel2),
              oldKeyValues = Slice(functionLevel3),
              isLastLevel = false
            )

          level2And3Merge should contain only Memory.PendingApply(1, Slice(Value.Function(functionId, Time(1)), Value.Function(functionId, Time(2))))

          //merge the result of above two merges
          val finalFunctionMerge =
            TestData.merge(
              newKeyValues = level1And2Merge.toSlice,
              oldKeyValues = level2And3Merge.toSlice,
              isLastLevel = false
            )

          //final merge results in pending functions
          finalFunctionMerge should contain only
            Memory.PendingApply(
              key = 1,
              applies =
                Slice(
                  Value.Function(functionId, Time(1)),
                  //pending apply merges do not check for duplicate timed
                  //values because this is a very rare occurrence.
                  Value.Function(functionId, Time(2)),
                  Value.Function(functionId, Time(2)),
                  Value.Function(functionId, Time(3))
                )
            )

          val randomDeadline = randomDeadlineOption(expired = false)

          //put collapses all functions
          val putMerge =
            TestData.merge(
              newKeyValues = finalFunctionMerge.toSlice,
              oldKeyValues = Slice(Memory.Put(1, 0, randomDeadline, Time(0))),
              isLastLevel = randomBoolean()
            )

          //since there were 3 functions the final result is 3 even though the
          //functions were merge in random order.
          putMerge should contain only Memory.Put(1, 3, randomDeadline, Time(3))
        }
      }
    }
  }
}
