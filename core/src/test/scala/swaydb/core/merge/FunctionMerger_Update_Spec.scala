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

package swaydb.core.merge

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Sliced

class FunctionMerger_Update_Spec extends AnyWordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Sliced[Byte]] = TimeOrder.long
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
          val output = SwayFunctionOutput.Update((randomStringOption: Sliced[Byte]).asSliceOption(), Some(randomDeadline()))

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
  }
}
