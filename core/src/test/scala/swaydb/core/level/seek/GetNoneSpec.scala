/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{KeyValue, SwayFunctionOutput, Value}
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.{TestData, TestTimer}
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class GetNoneSpec extends AnyWordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return None" when {
    "put is expired" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "removed" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRemoveKeyValue(1, randomExpiredDeadlineOption())

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "remove has time left but next Level returns None" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning KeyValue.Put.Null

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "remove has time left but next Level Put is expired" in {
      runThis(100.times) {
        implicit val testTimer = TestTimer.Decremental()

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "update has no next Level put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomUpdateKeyValue(1, deadline = randomDeadlineOption(false))
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning KeyValue.Put.Null

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "update has next Level expired put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomUpdateKeyValue(1, deadline = randomDeadlineOption(false))
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "function has no next Level put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomFunctionKeyValue(1)
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning KeyValue.Put.Null

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "function has next Level expired put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomFunctionKeyValue(1)
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "pending applies has no next Level put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomPendingApplyKeyValue(1)
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning KeyValue.Put.Null

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "pending applies has next Level expired put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomPendingApplyKeyValue(1)
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "pending applies has Level put that resulted in expiry" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val pendingApply =
          randomPendingApplyKeyValue(
            key = 1,
            deadline = Some(expiredDeadline()),
            functionOutput =
              eitherOne(
                SwayFunctionOutput.Remove,
                SwayFunctionOutput.Expire(expiredDeadline()),
                SwayFunctionOutput.Update(randomStringOption, Some(expiredDeadline()))
              )
          )

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning pendingApply
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1, deadline = Some(expiredDeadline()))

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "range's fromValue is removed or expired" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val fromValue =
          eitherOne(
            Value.remove(None),
            Value.update(Slice.Null, Some(expiredDeadline())),
            Value.put(Slice.Null, Some(expiredDeadline()))
          )

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRangeKeyValue(1, 10, fromValue)

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }

    "range's fromValue or/and rangeValue is a function that removes or expired" in {
      runThis(30.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val functionValue =
          randomKeyValueFunctionKeyValue(
            key = 1,
            output =
              eitherOne(
                SwayFunctionOutput.Remove,
                SwayFunctionOutput.Expire(expiredDeadline()),
                SwayFunctionOutput.Update(randomStringOption, Some(expiredDeadline()))
              )
          ).toRangeValue()

        getFromCurrentLevel.get _ expects(1: Slice[Byte], *) returning randomRangeKeyValue(1, 10, eitherOne(Value.FromValue.Null, functionValue), functionValue)
        //next level can return anything it will be removed.
        getFromNextLevel.get _ expects(1: Slice[Byte], *) returning randomPutKeyValue(1)

        Get(1, ThreadReadState.random).toOptionPut shouldBe empty
      }
    }
  }
}
