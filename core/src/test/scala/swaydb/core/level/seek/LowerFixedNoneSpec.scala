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
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.data.KeyValue
import swaydb.core.level.LevelSeek
import swaydb.core.{TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

class LowerFixedNoneSpec extends AnyWordSpec with Matchers with MockFactory with OptionValues {

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
        current.lower         _ expects (0: Slice[Byte], *)  returning LevelSeek.None
        next.lower            _ expects (0: Slice[Byte], *)  returning KeyValue.Put.Null
        //@formatter:on
      }
      Lower(0: Slice[Byte]).right.value shouldBe empty
    }


    //     1
    //   0
    //   x
    "2" in {

      runThis(1.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        inSequence {
          //@formatter:off
          current.lower         _ expects (1: Slice[Byte], *)  returning LevelSeek.Some(1, randomRemoveOrUpdateOrFunctionRemove(0))
          next.lower            _ expects (1: Slice[Byte], *)  returning KeyValue.Put.Null
          current.lower         _ expects (0: Slice[Byte], *)  returning LevelSeek.None
          //@formatter:on
        }
        Lower(1: Slice[Byte]).right.value shouldBe empty
      }
    }


    //     1
    //   0
    //   0
    "3" in {

      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        inSequence {
          //@formatter:off
          current.lower         _ expects (1: Slice[Byte], *)  returning LevelSeek.Some(1, randomRemoveOrUpdateOrFunctionRemove(0))
          next.lower            _ expects (1: Slice[Byte], *)  returning randomPutKeyValue(0)
          current.lower         _ expects (0: Slice[Byte], *)  returning LevelSeek.None
          next.lower            _ expects (0: Slice[Byte], *)  returning KeyValue.Put.Null
          //@formatter:on
        }
        Lower(1: Slice[Byte]).right.value shouldBe empty
      }
    }


    //       2
    //   0  1
    //   0
    "4" in {

      runThis(100.times) {

        implicit val testTimer = TestTimer.Empty

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        inSequence {
          //@formatter:off
          current.lower         _ expects (2: Slice[Byte], *)  returning LevelSeek.Some(1, randomRemoveOrUpdateOrFunctionRemove(1))
          next.lower            _ expects (2: Slice[Byte], *)  returning randomPutKeyValue(0)
          current.lower         _ expects (1: Slice[Byte], *)  returning LevelSeek.Some(1, randomRemoveOrUpdateOrFunctionRemove(0))
          current.lower         _ expects (0: Slice[Byte], *)  returning LevelSeek.None
          next.lower            _ expects (0: Slice[Byte], *)  returning KeyValue.Put.Null
          //@formatter:on
        }
        Lower(2: Slice[Byte]).right.value shouldBe empty
      }
    }

    //       2
    //     1
    //   0
    //this test is not implemented as it would result in a put. See LowerFixedSomeSpec
  }
}
