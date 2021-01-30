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
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.data.{KeyValue, Value}
import swaydb.core.level.LevelSeek
import swaydb.core.{TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

class HigherRangeNoneSpec extends AnyWordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return None" when {
    implicit val testTimer = TestTimer.Decremental()

    "1" when {
      //0->9
      //0 - 10
      //x
      "range value is not removed or expired" in {
        runThis(100.times) {

          implicit val testTimer = TestTimer.Empty

          (0 to 9) foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]

              inSequence {
                //@formatter:off
                current.higher        _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(0, 10, rangeValue = randomUpdateRangeValue()))
                next.higher           _ expects (key: Slice[Byte], *)  returning KeyValue.Put.Null
                current.get           _ expects (10: Slice[Byte], *)   returning KeyValue.Put.Null
                current.higher        _ expects (10: Slice[Byte], *)   returning LevelSeek.None
                //@formatter:on
              }
              Higher(key: Slice[Byte]).right.value shouldBe empty
          }
        }
      }

      //0->9
      //0 - 10
      // 1-9
      "range value is removed or expired" in {
        //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
        runThis(100.times) {

          implicit val testTimer = TestTimer.Empty

          (0 to 9) foreach {
            key =>
              implicit val current = mock[CurrentWalker]
              implicit val next = mock[NextWalker]

              inSequence {
                //@formatter:off
                current.higher        _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(0, 10, Value.FromValue.Null, randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false)))
                current.get           _ expects (10: Slice[Byte], *)   returning KeyValue.Put.Null
                current.higher        _ expects (10: Slice[Byte], *)   returning LevelSeek.None
                next.higher           _ expects (10: Slice[Byte], *)   returning KeyValue.Put.Null
                //@formatter:on
              }
              Higher(key: Slice[Byte]).right.value shouldBe empty
          }
        }
      }
    }

    "2" when {

      //0
      //  1 - 10
      //  x
      "range value is not removed or expired" in {
        runThis(100.times) {

          implicit val testTimer = TestTimer.Empty

          implicit val current = mock[CurrentWalker]
          implicit val next = mock[NextWalker]

          inSequence {
            //@formatter:off
            current.higher        _ expects (0: Slice[Byte], *)    returning LevelSeek.Some(1, randomRangeKeyValue(1, 10, randomFromValueOption(addPut = false), randomUpdateRangeValue()))
            next.higher           _ expects (0: Slice[Byte], *)    returning KeyValue.Put.Null
            current.get           _ expects (10: Slice[Byte], *)   returning KeyValue.Put.Null
            current.higher        _ expects (10: Slice[Byte], *)   returning LevelSeek.None
            //@formatter:on
          }
          Higher(0: Slice[Byte]).leftSideValue.right.value shouldBe empty
        }
      }

      //0
      // 1 - 10
      // 1-9
      "range value is removed or expired" in {
        //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
        runThis(100.times) {

          implicit val testTimer = TestTimer.Empty
          implicit val current = mock[CurrentWalker]
          implicit val next = mock[NextWalker]

          val currentHigher = randomRangeKeyValue(1, 10, randomRemoveOrUpdateOrFunctionRemoveValueOption().getOrElse(Value.FromValue.Null), randomRemoveOrUpdateOrFunctionRemoveValue(addFunctions = false))

          inSequence {
            //@formatter:off
            current.higher        _ expects (0: Slice[Byte], *)    returning LevelSeek.Some(1, currentHigher)
            next.higher           _ expects (0: Slice[Byte], *)    returning KeyValue.Put.Null
            current.get           _ expects (10: Slice[Byte], *)   returning KeyValue.Put.Null
            current.higher        _ expects (10: Slice[Byte], *)   returning LevelSeek.None
            //@formatter:on
          }
          Higher(0: Slice[Byte]).right.value shouldBe empty
        }
      }
    }
  }
}
