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
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.KeyValue
import swaydb.core.level.LevelSeek
import swaydb.core.{TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice._
import swaydb.serializers.Default._
import swaydb.serializers._

class HigherFixedNoneSpec extends AnyWordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return None" when {
    implicit val testTimer = TestTimer.Decremental()

    //   0
    //   x
    //   x
    "1" in {

      implicit val current = mock[CurrentWalker]
      implicit val next = mock[NextWalker]

      inSequence {
        //@formatter:off
        current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.None
        next.higher           _ expects (0: Slice[Byte], *)  returning KeyValue.Put.Null
        //@formatter:on
      }
      Higher(0: Slice[Byte]).right.value shouldBe empty
    }

    //   0
    //     1
    //     x
    "2" in {
      runThis(100.times) {
        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        inSequence {
          //@formatter:off
          current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.Some(0, randomRemoveOrUpdateOrFunctionRemove(1))
          next.higher           _ expects (0: Slice[Byte], *)  returning KeyValue.Put.Null
          current.higher        _ expects (1: Slice[Byte], *)  returning LevelSeek.None
          //@formatter:on
        }
        Higher(0: Slice[Byte]).right.value shouldBe empty
      }
    }

    //   0
    //     1
    //     1
    "3" in {

      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        inSequence {
          //@formatter:off
          current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.Some(0, randomRemoveOrUpdateOrFunctionRemove(1))
          next.higher           _ expects (0: Slice[Byte], *)  returning randomPutKeyValue(1)
          current.higher        _ expects (1: Slice[Byte], *)  returning LevelSeek.None
          next.higher           _ expects (1: Slice[Byte], *)  returning KeyValue.Put.Null
          //@formatter:on
        }
        Higher(0: Slice[Byte]).right.value shouldBe empty
      }
    }

    //   0
    //     1 2
    //       2
    "4" in {

      runThis(100.times) {

        implicit val testTimer = TestTimer.Empty

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        inSequence {
          //@formatter:off
          current.higher        _ expects (0: Slice[Byte], *)  returning LevelSeek.Some(0, randomRemoveOrUpdateOrFunctionRemove(1))
          next.higher           _ expects (0: Slice[Byte], *)  returning randomPutKeyValue(2)
          current.higher        _ expects (1: Slice[Byte], *)  returning LevelSeek.Some(0, randomRemoveOrUpdateOrFunctionRemove(2))
          current.higher        _ expects (2: Slice[Byte], *)  returning LevelSeek.None
          next.higher           _ expects (2: Slice[Byte], *)  returning KeyValue.Put.Null
          //@formatter:on
        }
        Higher(0: Slice[Byte]).right.value shouldBe empty
      }
    }

    //   0
    //       2
    //     1
    //this test is not implemented as it would result in a put. See HigherFixedSomeSpec
  }
}
