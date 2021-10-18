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
import org.scalatest.OptionValues._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{Time, Value}
import swaydb.core.level.LevelSeek
import swaydb.core.merge.FixedMerger
import swaydb.core.{TestData, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

class LowerRangeSomeSpec extends AnyWordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore

  "return Some" when {
    implicit val testTimer = TestTimer.Empty

    //  2<- 10
    //0  -  10
    // 1 - 9
    "1" in {
      runThis(100.times) {

        implicit val testTimer = TestTimer.Empty

        (5 to 10).reverse foreach {
          key =>
            implicit val current = mock[CurrentWalker]
            implicit val next = mock[NextWalker]

            val rangeValue = randomUpdateRangeValue(value = randomStringSliceOptional, functionOutput = randomFunctionOutput(addRemoves = false, expiredDeadline = false))
            val upperLevel = randomRangeKeyValue(0, 10, Value.FromValue.Null, rangeValue = rangeValue)
            val lowerLower = randomPutKeyValue(key - 1, deadline = None)

            val expected = FixedMerger(upperLevel.rangeValue.toMemory(key - 1), lowerLower).runRandomIO

            inSequence {
              //@formatter:off
              current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, upperLevel)
              next.lower            _ expects (key: Slice[Byte], *)  returning lowerLower
              //@formatter:on
            }
            Lower(key: Slice[Byte]).runRandomIO.right.value.value shouldBe expected.right.value
        }
      }
    }

    // 1
    //0  -  10
    //0
    "1a" in {
      runThis(100.times) {

        implicit val testTimer = TestTimer.Empty

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val fromValue = eitherOne(Value.FromValue.Null, Value.put(randomStringSliceOptional, removeAfter = randomDeadlineOption(false)))
        val upperLevel = randomRangeKeyValue(0, 10, fromValue, randomRangeValue(addRemoves = false, deadline = randomDeadlineOption(false), functionOutput = randomFunctionOutput(false, false)))
        val lowerLevel = randomPutKeyValue(0, deadline = randomDeadlineOption(false))

        //if fromValue is defined it will be merged with lower else lower is returned
        val expected =
          IO(upperLevel.fetchFromOrElseRangeValueUnsafe)
            .map(currentFromValue => FixedMerger(currentFromValue.toMemory(0), lowerLevel).runRandomIO.right.value)
            .getOrElse(lowerLevel)

        inSequence {
          //@formatter:off
          current.lower         _ expects (1: Slice[Byte], *)    returning LevelSeek.Some(1, upperLevel)
          next.lower            _ expects (1: Slice[Byte], *)    returning lowerLevel
          //@formatter:on
        }
        Lower(1: Slice[Byte]).runRandomIO.right.value.value shouldBe expected
      }
    }

    //           15
    //0  -  10
    //      10
    "2" in {
      runThis(100.times) {

        implicit val testTimer = TestTimer.Empty

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        (11 to 15).reverse foreach {
          key =>
            val lowerLower = randomPutKeyValue(10, deadline = None)

            inSequence {
              //@formatter:off
              current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(0, 10))
              next.lower            _ expects (key: Slice[Byte], *)  returning lowerLower
              //@formatter:on
            }
            Lower(key: Slice[Byte]).runRandomIO.right.value.value shouldBe lowerLower
        }
      }
    }

    //           15
    //0  -  10
    //     9
    "3" in {
      runThis(100.times) {

        implicit val testTimer = TestTimer.Empty

        (10 to 15).reverse foreach {
          key =>
            implicit val current = mock[CurrentWalker]
            implicit val next = mock[NextWalker]

            val currentRangeValue = randomUpdateRangeValue()
            val lowerLower = randomPutKeyValue(9, deadline = None)

            val expected = FixedMerger(currentRangeValue.toMemory(9), lowerLower).runRandomIO.right.value

            inSequence {
              //@formatter:off
              current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(0, 10, rangeValue = currentRangeValue))
              next.lower            _ expects (key: Slice[Byte], *)  returning lowerLower
              //@formatter:on
            }

            Lower(key: Slice[Byte]).right.value.value shouldBe expected
        }
      }
    }

    //              10 <- 15
    //  1(None)  -  10
    //  1
    "4" in {
      runThis(100.times) {

        implicit val testTimer = TestTimer.Empty

        (10 to 15).reverse foreach {
          key =>
            implicit val current = mock[CurrentWalker]
            implicit val next = mock[NextWalker]

            val lowerLower = randomPutKeyValue(1, deadline = None)
            val rangeValue = randomRangeValue(addRemoves = false, deadline = randomDeadlineOption(false), functionOutput = randomFunctionOutput(false, false))

            val expected = FixedMerger(rangeValue.toMemory(1), lowerLower).runRandomIO.right.value

            inSequence {
              //@formatter:off
              current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(1, 10, fromValue = Value.FromValue.Null, rangeValue))
              next.lower            _ expects (key: Slice[Byte], *)  returning lowerLower
              //@formatter:on
            }
            Lower(key: Slice[Byte]).runRandomIO.right.value.value shouldBe expected
        }
      }
    }

    //                  10 <- 15
    //  1(Some-put)  -  10
    //0-1
    "5" in {
      runThis(100.times) {

        implicit val testTimer = TestTimer.Empty

        (10 to 15).reverse foreach {
          key =>
            implicit val current = mock[CurrentWalker]
            implicit val next = mock[NextWalker]

            val put = Value.Put(randomStringSliceOptional, None, Time.empty)

            inSequence {
              //@formatter:off
              current.lower         _ expects (key: Slice[Byte], *)  returning LevelSeek.Some(1, randomRangeKeyValue(1, 10, fromValue = put))
              next.lower            _ expects (key: Slice[Byte], *)  returning randomPutKeyValue(eitherOne[Int](0, 1), deadline = None)
              //@formatter:on
            }
            Lower(key: Slice[Byte]).runRandomIO.right.value.value shouldBe put.toMemory(1)
        }
      }
    }
  }
}
