/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import swaydb.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

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
