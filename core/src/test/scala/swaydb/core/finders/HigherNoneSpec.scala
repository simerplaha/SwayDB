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

package swaydb.core.finders

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, OptionValues, WordSpec}
import scala.util.Try
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TryAssert._
import swaydb.core.data._
import swaydb.core.util.TryUtil
import swaydb.core.{TestData, TestTimeGenerator}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class HigherNoneSpec extends WordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return None" when {

    "current Level has higher expired or updated fixed key-value and next Level is empty" in {
      /**
        * 0, 2, 3, 4
        * empty
        */

      runThis(100.times) {
        implicit val timeGenerator = TestTimeGenerator.Decremental()

        val higherFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]](FunctionName(Symbol("higherFromCurrentLevel")))
        val get = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("get")))
        val higherInNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("higherInNextLevel")))

        inSequence {
          //@formatter:off
          higherFromCurrentLevel expects (0: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(2)))
          higherInNextLevel      expects (0: Slice[Byte]) returning TryUtil.successNone
          higherFromCurrentLevel expects (2: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(3)))
          higherFromCurrentLevel expects (3: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(4)))
          higherFromCurrentLevel expects (4: Slice[Byte]) returning TryUtil.successNone
          //@formatter:on
        }
        Higher(0, higherFromCurrentLevel, get, higherInNextLevel).assertGetOpt shouldBe empty
      }
    }

    "current Level has higher expired or updated fixed key-value and next Level is not empty empty" in {
      runThis(100.times) {
        implicit val timeGenerator = TestTimeGenerator.Decremental()

        /**
          * 0      2  3  4  5
          * _  1   2        5  6
          */

        val higherFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]](FunctionName(Symbol("higherFromCurrentLevel")))
        val get = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("get")))
        val higherInNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("higherInNextLevel")))

        inSequence {
          //@formatter:off
          higherFromCurrentLevel expects (0: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(2)))
          higherInNextLevel      expects (0: Slice[Byte]) returning Try(Some(randomExpiredPutKeyValue(1)))
          higherFromCurrentLevel expects (2: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(3)))
          higherInNextLevel      expects (2: Slice[Byte]) returning Try(Some(randomExpiredPutKeyValue(5)))
          higherFromCurrentLevel expects (3: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(4)))
          higherFromCurrentLevel expects (4: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(5)))
          higherFromCurrentLevel expects (5: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(6)))
          higherInNextLevel      expects (5: Slice[Byte]) returning Try(Some(randomExpiredPutKeyValue(6)))
          higherFromCurrentLevel expects (6: Slice[Byte]) returning TryUtil.successNone
          higherInNextLevel      expects (6: Slice[Byte]) returning TryUtil.successNone
          //@formatter:on
        }

        Higher(0, higherFromCurrentLevel, get, higherInNextLevel).assertGetOpt shouldBe empty
      }
    }

    "ranges exists & next Level is empty" in {
      runThis(1000.times) {
        implicit val timeGenerator = TestTimeGenerator.Decremental()

        /**
          * (input) - 10
          * Level0  - 10-20, 20-30, 30, 35, 40-50
          * Level1  - empty
          */

        val higherFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]](FunctionName(Symbol("higherFromCurrentLevel")))
        val get = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("get")))
        val higherInNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("higherInNextLevel")))

        //for initial range do not add deadline or Remove so that 10 key gets read from lower level.
        inSequence {
          //@formatter:off
          higherFromCurrentLevel expects (10: Slice[Byte]) returning Try(Some(randomRangeKeyValueWithFromValueExpiredDeadline(10, 20, None, randomRangeValue(deadline = None, addRemoves = false))))
          higherInNextLevel      expects (10: Slice[Byte]) returning TryUtil.successNone
          get                    expects (20: Slice[Byte]) returning TryUtil.successNone
          higherFromCurrentLevel expects (20: Slice[Byte]) returning Try(Some(randomRangeKeyValueWithFromValueExpiredDeadline(20, 30))) //fromValue is set it will
          get                    expects (30: Slice[Byte]) returning Try(Some(randomExpiredPutKeyValue(30)))
          higherFromCurrentLevel expects (30: Slice[Byte]) returning Try(Some(randomDeadUpdateOrExpiredPut(35)))
          higherFromCurrentLevel expects (35: Slice[Byte]) returning Try(Some(randomRangeKeyValueWithFromValueExpiredDeadline(40, 50)))
          get                    expects (50: Slice[Byte]) returning TryUtil.successNone
          higherFromCurrentLevel expects (50: Slice[Byte]) returning TryUtil.successNone
          //@formatter:on
        }

        Higher(10, higherFromCurrentLevel, get, higherInNextLevel).assertGetOpt shouldBe empty
      }
    }

    "ranges exists & next Level is non empty and key overlap with upper Levels ranges" in {
      runThis(100.times) {
        implicit val timeGenerator = TestTimeGenerator.Decremental()

        /**
          * (input) - 10
          * Level0  - 10 - 20, 20-30, 31
          * Level1  -  11 19   20     31
          */

        val higherFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]](FunctionName(Symbol("higherFromCurrentLevel")))
        val get = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("get")))
        val higherInNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("higherInNextLevel")))

        //for initial range do not add deadline or Remove so that 10 key gets read from lower level.
        inSequence {
          //@formatter:off
          higherFromCurrentLevel expects (10: Slice[Byte]) returning Try(Some(randomRangeKeyValueWithFromValueExpiredDeadline(10, 20, None, randomRemoveFunctionValue())))
          higherInNextLevel      expects (10: Slice[Byte]) returning Try(Some(randomPutKeyValue(11)))
          higherInNextLevel      expects (11: Slice[Byte]) returning Try(Some(randomPutKeyValue(19)))
          higherInNextLevel      expects (19: Slice[Byte]) returning Try(Some(randomPutKeyValue(20)))
          get                    expects (20: Slice[Byte]) returning TryUtil.successNone
          higherFromCurrentLevel expects (20: Slice[Byte]) returning Try(Some(randomRangeKeyValueWithFromValueExpiredDeadline(20, 30, None, randomFunctionValue(SwayFunctionOutput.Update(None, Some(expiredDeadline()))))))
          higherInNextLevel      expects (20: Slice[Byte]) returning Try(Some(randomPutKeyValue(31)))
          get                    expects (30: Slice[Byte]) returning TryUtil.successNone
          higherFromCurrentLevel expects (30: Slice[Byte]) returning Try(Some(randomRemoveKeyValue(31, None)))
          higherFromCurrentLevel expects (31: Slice[Byte]) returning TryUtil.successNone
          higherInNextLevel      expects (31: Slice[Byte]) returning TryUtil.successNone
          //@formatter:on
        }

        Higher(10, higherFromCurrentLevel, get, higherInNextLevel).assertGetOpt shouldBe empty
      }
    }

    "all key-values are range removed" in {
      runThis(1000.times) {
        implicit val timeGenerator = TestTimeGenerator.Decremental()

        /**
          * (input) - 10
          * Level0  - 10-20 (remove)
          * Level1  - empty
          */

        val higherFromCurrentLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.SegmentResponse]]](FunctionName(Symbol("higherFromCurrentLevel")))
        val get = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("get")))
        val higherInNextLevel = mockFunction[Slice[Byte], Try[Option[KeyValue.ReadOnly.Put]]](FunctionName(Symbol("higherInNextLevel")))

        val removeValue =
          eitherOne(
            Value.remove(None),
            Value.remove(Some(expiredDeadline())),
            Value.update(None, Some(expiredDeadline()))
          )


        //for initial range do not add deadline or Remove so that 10 key gets read from lower level.
        inSequence {
          //@formatter:off
          higherFromCurrentLevel expects (10: Slice[Byte]) returning Try(Some(randomRangeKeyValueWithFromValueExpiredDeadline(10, 20, None, removeValue)))
          get                    expects (20: Slice[Byte]) returning TryUtil.successNone
          higherFromCurrentLevel expects (20: Slice[Byte]) returning TryUtil.successNone
          higherInNextLevel      expects (20: Slice[Byte]) returning TryUtil.successNone
          //@formatter:on
        }

        Higher(10, higherFromCurrentLevel, get, higherInNextLevel).assertGetOpt shouldBe empty
      }
    }
  }
}
