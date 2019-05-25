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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.seek

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OptionValues, WordSpec}
import swaydb.core.CommonAssertions._
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.{SwayFunctionOutput, Value}
import swaydb.core.{TestData, TestTimer}
import swaydb.data.{Reserve, IO}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class GetNoneSpec extends WordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return None" when {
    "put is expired" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline()))))

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "removed" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomRemoveKeyValue(1, randomExpiredDeadlineOption())))

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "remove has time left but next Level returns None" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO.none

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "remove has time left but next Level Put is expired" in {
      runThis(100.times) {
        implicit val testTimer = TestTimer.Decremental()

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline())))).asAsync

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "update has no next Level put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomUpdateKeyValue(1, deadline = randomDeadlineOption(false))))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO.none

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "update has next Level expired put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomUpdateKeyValue(1, deadline = randomDeadlineOption(false))))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline())))).asAsync

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "function has no next Level put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomFunctionKeyValue(1)))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO.none

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "function has next Level expired put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomFunctionKeyValue(1)))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline())))).asAsync

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "pending applies has no next Level put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPendingApplyKeyValue(1)))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO.none

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "pending applies has next Level expired put" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPendingApplyKeyValue(1)))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline())))).asAsync

        Get(1).assertGetOpt shouldBe empty
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
                SwayFunctionOutput.Update(randomStringOption, Some(expiredDeadline())),
              )
          )

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(pendingApply))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline())))).asAsync

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "range's fromValue is removed or expired" in {
      runThis(100.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val fromValues =
          eitherOne(
            Value.remove(None),
            Value.update(None, Some(expiredDeadline())),
            Value.put(None, Some(expiredDeadline()))
          )

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomRangeKeyValue(1, 10, Some(fromValues))))

        Get(1).assertGetOpt shouldBe empty
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
                SwayFunctionOutput.Update(randomStringOption, Some(expiredDeadline())),
              )
          ).toRangeValue().assertGet

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomRangeKeyValue(1, 10, eitherOne(None, Some(functionValue)), functionValue)))
        //next level can return anything it will be removed.
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(1))).asAsync

        Get(1).assertGetOpt shouldBe empty
      }
    }

    "next Level returns IO.Async" in {
      runThis(1.times) {
        implicit val testTimer = eitherOne(TestTimer.Decremental(), TestTimer.Empty)

        implicit val getFromCurrentLevel = mock[CurrentGetter]
        implicit val getFromNextLevel = mock[NextGetter]

        val busy = IO.Error.DecompressingIndex(Reserve(()))

        getFromCurrentLevel.get _ expects (1: Slice[Byte]) returning IO(Some(randomPendingApplyKeyValue(1)))
        getFromNextLevel.get _ expects (1: Slice[Byte]) returning IO.Async(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline()))), busy)

        val io = Get(1)

        val ioStillBusy = io.safeGet
        ioStillBusy.isSuccess shouldBe false

        Reserve.setFree(busy.busy)
        io.safeGet.assertGetOpt shouldBe empty
      }
    }
  }

}
