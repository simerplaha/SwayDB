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

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.{SwayFunctionOutput, Value}
import swaydb.core.merge.{FixedMerger, FunctionMerger, PendingApplyMerger}
import swaydb.core.segment.ThreadReadState
import swaydb.core.{TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Sliced

class GetSomeSpec extends AnyWordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return Some" when {
    "put is not expired" in {
      runThis(10.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val keyValue = randomPutKeyValue(1, deadline = randomDeadlineOption(false))
        getFromCurrentLevel.get _ expects (1: Sliced[Byte], *) returning keyValue

        Get(1, ThreadReadState.random) shouldBe keyValue
      }
    }

    "remove has time left" in {
      runThis(10.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val remove = randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))
        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val expect = put.copy(deadline = remove.deadline.orElse(put.deadline), time = remove.time)

        getFromCurrentLevel.get _ expects (1: Sliced[Byte], *) returning remove
        getFromNextLevel.get _ expects (1: Sliced[Byte], *) returning put

        Get(1, ThreadReadState.random) shouldBe expect
      }
    }

    "update has time left" in {
      runThis(10.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val update = randomUpdateKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val expect = put.copy(deadline = update.deadline.orElse(put.deadline), value = update.value, time = update.time)

        getFromCurrentLevel.get _ expects (1: Sliced[Byte], *) returning update
        getFromNextLevel.get _ expects (1: Sliced[Byte], *) returning put

        Get(1, ThreadReadState.random) shouldBe expect
      }
    }

    "functions either update or update expiry but do not remove" in {
      runThis(10.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val function =
          randomKeyValueFunctionKeyValue(
            key = 1,
            output =
              eitherOne(
                SwayFunctionOutput.Expire(randomDeadline(false)),
                SwayFunctionOutput.Update(randomStringSliceOptional, randomDeadlineOption(false))
              )
          )

        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val expect = FunctionMerger(function, put).runRandomIO.right.value

        getFromCurrentLevel.get _ expects (1: Sliced[Byte], *) returning function
        getFromNextLevel.get _ expects (1: Sliced[Byte], *) returning put

        Get(1, ThreadReadState.random) shouldBe expect
      }
    }

    "pending applies that do not expire or remove" in {
      runThis(10.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val pendingApply =
          randomPendingApplyKeyValue(
            key = 1,
            deadline = Some(randomDeadline(false)),
            functionOutput =
              eitherOne(
                SwayFunctionOutput.Expire(randomDeadline(false)),
                SwayFunctionOutput.Update(randomStringSliceOptional, randomDeadlineOption(false))
              )
          )

        val put =
          randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        val expected = PendingApplyMerger(pendingApply, put).runRandomIO.right.value

        getFromCurrentLevel.get _ expects (1: Sliced[Byte], *) returning pendingApply
        getFromNextLevel.get _ expects (1: Sliced[Byte], *) returning put

        Get(1, ThreadReadState.random) shouldBe expected
      }
    }

    "range's fromValue is put" in {
      runThis(10.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val fromValue = Value.put(Slice.Null, randomDeadlineOption(false))

        val range = randomRangeKeyValue(1, 10, fromValue)

        getFromCurrentLevel.get _ expects (1: Sliced[Byte], *) returning range

        Get(1, ThreadReadState.random) shouldBe fromValue.toMemory(1)
      }
    }

    "range's fromValue or/and rangeValue is a function that does not removes or expires" in {
      runThis(30.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val functionValue =
          randomKeyValueFunctionKeyValue(
            key = 1,
            output =
              eitherOne(
                SwayFunctionOutput.Expire(randomDeadline(false)),
                SwayFunctionOutput.Update(randomStringSliceOptional, randomDeadlineOption(false))
              )
          )

        val range = randomRangeKeyValue(1, 10, eitherOne(Value.FromValue.Null, functionValue.toRangeValue().runRandomIO.right.value), functionValue.toRangeValue().runRandomIO.right.value)
        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        val expected = FixedMerger(functionValue, put).runRandomIO.right.value

        getFromCurrentLevel.get _ expects (1: Sliced[Byte], *) returning range
        //next level can return anything it will be removed.
        getFromNextLevel.get _ expects (1: Sliced[Byte], *) returning put

        Get(1, ThreadReadState.random) shouldBe expected
      }
    }
  }
}
