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
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.merge.FixedMerger
import swaydb.core.{TestData, TestTimer}
import swaydb.data.IO
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class HigherFixedSomeSpec extends WordSpec with Matchers with MockFactory with OptionValues {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return Some" when {
    implicit val testTimer = TestTimer.Decremental()

    //   0
    //     1
    //     x
    "1" in {
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        inSequence {
          //@formatter:off
          current.higher        _ expects (0: Slice[Byte])  returning IO(Some(put))
          next.stateID          _ expects ()                returning 1
          next.higher           _ expects (0: Slice[Byte])  returning IO.none
          next.hasStateChanged  _ expects 1                 returning false
          //@formatter:on
        }
        Higher(0: Slice[Byte]).runIO.value shouldBe put
      }
    }

    //   0
    //     x
    //     1
    "2" in {

      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val put = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        inSequence {
          //@formatter:off
          current.higher        _ expects (0: Slice[Byte])  returning IO.none
          next.stateID          _ expects ()                returning 1
          next.higher           _ expects (0: Slice[Byte])  returning IO(Some(put)).asAsync
          next.hasStateChanged  _ expects 1                 returning false
          //@formatter:on
        }
        Higher(0: Slice[Byte]).runIO.value shouldBe put
      }
    }


    //   0
    //     1
    //     1
    "3" in {
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperKeyValue = randomFixedKeyValue(1, includeRemoves = false, deadline = randomDeadlineOption(false), functionOutput = randomUpdateFunctionOutput())
        val lowerKeyValue = randomPutKeyValue(1, deadline = None)
        val expected = FixedMerger(upperKeyValue, lowerKeyValue).runIO

        inSequence {
          //@formatter:off
          current.higher        _ expects (0: Slice[Byte])  returning IO(Some(upperKeyValue))
          next.stateID          _ expects ()                returning 1
          next.higher           _ expects (0: Slice[Byte])  returning IO(Some(lowerKeyValue)).asAsync
          next.hasStateChanged  _ expects 1                 returning false
          //@formatter:on
        }
        Higher(0: Slice[Byte]).runIO.value shouldBe expected
      }
    }


    //   0
    //     1
    //       2
    "4" in {

      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperKeyValue = randomFixedKeyValue(1)
        val lowerKeyValue = randomPutKeyValue(2, deadline = randomDeadlineOption(false))

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
          current.higher        _ expects (0: Slice[Byte])  returning IO(Some(upperKeyValue))
          next.stateID          _ expects ()                returning 1
          next.higher           _ expects (0: Slice[Byte])  returning IO(Some(lowerKeyValue)).asAsync
          next.hasStateChanged  _ expects 1                 returning false
          if(!isUpperExpected) {
            next.hasStateChanged  _ expects 1                 returning false
            current.higher        _ expects (1: Slice[Byte])  returning IO.none
            next.hasStateChanged  _ expects 1                 returning false
          }
          //@formatter:on
        }
        Higher(0: Slice[Byte]).runIO.value shouldBe expected
      }
    }

    //   0
    //       2
    //     1
    "5" in {
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperKeyValue = randomFixedKeyValue(2)
        val lowerKeyValue = randomPutKeyValue(1)

        inSequence {
          //@formatter:off
          current.higher        _ expects (0: Slice[Byte])  returning IO(Some(upperKeyValue))
          next.stateID          _ expects ()                returning 1
          next.higher           _ expects (0: Slice[Byte])  returning IO(Some(lowerKeyValue)).asAsync
          next.hasStateChanged  _ expects 1                 returning false
          //@formatter:on
        }
        Higher(0: Slice[Byte]).runIO.value shouldBe lowerKeyValue
      }
    }
  }
}
