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

package swaydb.core.finder

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OptionValues, WordSpec}
import scala.util.Try
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TryAssert._
import swaydb.core.data.{KeyValue, SwayFunctionOutput, Value}
import swaydb.core.util.TryUtil
import swaydb.core.{TestData, TestTimeGenerator}
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
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline()))))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "removed" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomRemoveKeyValue(1, randomExpiredDeadlineOption())))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "remove has time left but next Level returns None" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))))
        getFromNextLevel expects (1: Slice[Byte]) returning TryUtil.successNone

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "remove has time left but next Level Put is expired" in {
      runThis(10.times) {
        implicit val timeGenerator = TestTimeGenerator.Decremental()

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomRemoveKeyValue(1, Some(randomDeadline(expired = false)))))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline()))))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "update has no next Level put" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomUpdateKeyValue(1, deadline = randomDeadlineOption(false))))
        getFromNextLevel expects (1: Slice[Byte]) returning TryUtil.successNone

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "update has next Level expired put" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomUpdateKeyValue(1, deadline = randomDeadlineOption(false))))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline()))))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "function has no next Level put" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomFunctionKeyValue(1)))
        getFromNextLevel expects (1: Slice[Byte]) returning TryUtil.successNone

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "function has no next Level expired put" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomFunctionKeyValue(1)))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline()))))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "pending applies has no next Level put" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomPendingApplyKeyValue(1)))
        getFromNextLevel expects (1: Slice[Byte]) returning TryUtil.successNone

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "pending applies has no next Level expired put" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomPendingApplyKeyValue(1)))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline()))))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "pending applies has Level put that resulted in expiry" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

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

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(pendingApply))
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(randomPutKeyValue(1, deadline = Some(expiredDeadline()))))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "range's fromValue is removed or expired" in {
      runThis(10.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

        val fromValues =
          eitherOne(
            Value.remove(None),
            Value.update(None, Some(expiredDeadline())),
            Value.put(None, Some(expiredDeadline()))
          )

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomRangeKeyValue(1, 10, Some(fromValues))))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }

    "range's fromValue or/and rangeValue is a function that removes or expired" in {
      runThis(30.times) {
        implicit val timeGenerator = eitherOne(TestTimeGenerator.Decremental(), TestTimeGenerator.Empty)

        val getFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]]
        val getFromNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]]

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

        getFromCurrentLevel expects (1: Slice[Byte]) returning Try(Some(randomRangeKeyValue(1, 10, eitherOne(None, Some(functionValue)), functionValue)))
        //next level can return anything it will be removed.
        getFromNextLevel expects (1: Slice[Byte]) returning Try(Some(randomPutKeyValue(1)))

        Get(1, getFromCurrentLevel, getFromNextLevel).assertGetOpt shouldBe empty
      }
    }
  }

}
