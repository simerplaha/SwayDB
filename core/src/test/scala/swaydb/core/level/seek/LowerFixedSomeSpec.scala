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
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.level.LevelSeek
import swaydb.core.merge.FixedMerger
import swaydb.core.{TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

class LowerFixedSomeSpec extends AnyWordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return Some" when {
    implicit val testTimer = TestTimer.Decremental()

    //     1
    //   0
    //   x
    "1" in {

      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val put = randomPutKeyValue(0, deadline = randomDeadlineOption(false))

        inSequence {
          //@formatter:off
          current.lower         _ expects (1: Slice[Byte], *)  returning LevelSeek.Some(1, put)
          next.lower            _ expects (1: Slice[Byte], *)  returning KeyValue.Put.Null
          //@formatter:on
        }
        Lower(1: Slice[Byte]).right.value.value shouldBe put
      }
    }

    //     1
    //   x
    //   0
    "2" in {

      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val put = randomPutKeyValue(0, deadline = randomDeadlineOption(false))

        inSequence {
          //@formatter:off
          current.lower         _ expects (1: Slice[Byte], *)  returning LevelSeek.None
          next.lower            _ expects (1: Slice[Byte], *)  returning put
          //@formatter:on
        }
        Lower(1: Slice[Byte]).right.value.value shouldBe put
      }
    }


    //     1
    //   0
    //   0
    "3" in {

      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperKeyValue = randomFixedKeyValue(0, includeRemoves = false, deadline = randomDeadlineOption(false), functionOutput = randomUpdateFunctionOutput())
        val lowerKeyValue = randomPutKeyValue(0, deadline = None)
        val expected = FixedMerger(upperKeyValue, lowerKeyValue).runRandomIO

        inSequence {
          //@formatter:off
          current.lower         _ expects (1: Slice[Byte], *)  returning LevelSeek.Some(1, upperKeyValue)
          next.lower            _ expects (1: Slice[Byte], *)  returning lowerKeyValue
          //@formatter:on
        }
        Lower(1: Slice[Byte]).runRandomIO.right.value.value shouldBe expected.right.value
      }
    }


    //       2
    //     1
    //   0
    "4" in {

      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperKeyValue = randomFixedKeyValue(1)
        val lowerKeyValue = randomPutKeyValue(0, deadline = randomDeadlineOption(false))

        val isUpperExpected =
          upperKeyValue match {
            case put: Memory.Put if put.hasTimeLeft() =>
              true
            case _ =>
              false
          }

        val expected =
          if (isUpperExpected) upperKeyValue else lowerKeyValue

        inSequence {
          //@formatter:off
          current.lower         _ expects (2: Slice[Byte], *)  returning LevelSeek.Some(1, upperKeyValue)
          next.lower            _ expects (2: Slice[Byte], *)  returning lowerKeyValue
          if(!isUpperExpected) {
            current.lower         _ expects (1: Slice[Byte], *)  returning LevelSeek.None
          }
          //@formatter:on
        }
        Lower(2: Slice[Byte]).runRandomIO.right.value.value shouldBe expected
      }
    }

    //       2
    //  0
    //     1
    "5" in {
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperKeyValue = randomFixedKeyValue(0)
        val lowerKeyValue = randomPutKeyValue(1)

        inSequence {
          //@formatter:off
          current.lower         _ expects (2: Slice[Byte], *) returning LevelSeek.Some(1, upperKeyValue)
          next.lower            _ expects (2: Slice[Byte], *) returning lowerKeyValue
          //@formatter:on
        }
        Lower(2: Slice[Byte]).runRandomIO.right.value.value shouldBe lowerKeyValue
      }
    }
  }
}
