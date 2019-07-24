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

package swaydb.core.level.seek

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OptionValues, WordSpec}
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.{SwayFunctionOutput, Value}
import swaydb.core.merge.{FixedMerger, FunctionMerger, PendingApplyMerger}
import swaydb.core.{TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.io.Core.IO.Error.ErrorHandler

class GetSomeSpec extends WordSpec with Matchers with MockFactory with OptionValues {

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
        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(keyValue))

        Get(1).runIO.value shouldBe keyValue
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

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(remove))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(put)).asDeferred

        Get(1).runIO.value shouldBe expect
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

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(update))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(put)).asDeferred

        Get(1).runIO.value shouldBe expect
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
                SwayFunctionOutput.Update(randomStringOption, randomDeadlineOption(false))
              )
          )

        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(expired = false))
        val expect = FunctionMerger(function, put).runIO

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(function))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(put)).asDeferred

        Get(1).runIO.value shouldBe expect
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
                SwayFunctionOutput.Update(randomStringOption, randomDeadlineOption(false)),
              )
          )

        val put =
          randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        val expected = PendingApplyMerger(pendingApply, put).runIO

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(pendingApply))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(put)).asDeferred

        Get(1).runIO.value shouldBe expected
      }
    }

    "range's fromValue is put" in {
      runThis(10.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val fromValue = Value.put(None, randomDeadlineOption(false))

        val range = randomRangeKeyValue(1, 10, Some(fromValue))

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(range))

        Get(1).runIO.value shouldBe fromValue.toMemory(1)
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
                SwayFunctionOutput.Update(randomStringOption, randomDeadlineOption(false)),
              )
          )

        val range = randomRangeKeyValue(1, 10, eitherOne(None, Some(functionValue.toRangeValue().runIO)), functionValue.toRangeValue().runIO)
        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        val expected = FixedMerger(functionValue, put).runIO

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(range))
        //next level can return anything it will be removed.
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(put)).asDeferred

        Get(1).runIO.value shouldBe expected
      }
    }
  }
}
