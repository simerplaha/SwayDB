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
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.level.LevelSeek
import swaydb.core.segment.data._
import swaydb.core.{CoreTestData, TestTimer}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class HigherRangeSomeSpec extends AnyWordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = CoreTestData.functionStore
  implicit val testTimer = TestTimer.Empty

  "return Some" when {

    //0
    //0  - 3
    // 1,2,3,4
    "1" in {
      //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperRange = randomRangeKeyValue(0, 3, rangeValue = randomRemoveOrUpdateOrFunctionRemoveValue())
        val toKeyGet = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        inSequence {
          current.higher _ expects(0: Slice[Byte], *) returning LevelSeek.Some(1, upperRange)
          if (upperRange.rangeValue.isInstanceOf[Value.Function]) {
            next.higher _ expects(0: Slice[Byte], *) returning randomPutKeyValue(1, deadline = randomDeadlineOption(false))
            next.higher _ expects(1: Slice[Byte], *) returning randomPutKeyValue(2, deadline = randomDeadlineOption(false))
            next.higher _ expects(2: Slice[Byte], *) returning randomPutKeyValue(3, deadline = randomDeadlineOption(false))
            current.get _ expects(3: Slice[Byte], *) returning toKeyGet
          } else {
            current.get _ expects(3: Slice[Byte], *) returning toKeyGet
          }
        }
        Higher(0: Slice[Byte]).right.value.value shouldBe toKeyGet
      }
    }

    // 1
    //0  - 3
    // 1,2,3,4
    "2" in {
      //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperRange = randomRangeKeyValue(0, 3, rangeValue = randomRemoveOrUpdateOrFunctionRemoveValue())
        val toKeyGet = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        inSequence {
          current.higher _ expects(1: Slice[Byte], *) returning LevelSeek.Some(1, upperRange)
          if (upperRange.rangeValue.isInstanceOf[Value.Function]) {
            next.higher _ expects(1: Slice[Byte], *) returning randomPutKeyValue(2, deadline = randomDeadlineOption(false))
            next.higher _ expects(2: Slice[Byte], *) returning randomPutKeyValue(3, deadline = randomDeadlineOption(false))
            current.get _ expects(3: Slice[Byte], *) returning toKeyGet
          } else {
            current.get _ expects(3: Slice[Byte], *) returning toKeyGet
          }
        }
        Higher(1: Slice[Byte]).right.value.value shouldBe toKeyGet
      }
    }

    //   2
    //0  - 3
    // 1,2,3,4
    "3" in {
      //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperRange = randomRangeKeyValue(0, 3, rangeValue = randomRemoveOrUpdateOrFunctionRemoveValue())
        val toKeyGet = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        inSequence {
          current.higher _ expects(2: Slice[Byte], *) returning LevelSeek.Some(1, upperRange)
          if (upperRange.rangeValue.isInstanceOf[Value.Function]) {
            next.higher _ expects(2: Slice[Byte], *) returning randomPutKeyValue(3, deadline = randomDeadlineOption(false))
            current.get _ expects(3: Slice[Byte], *) returning toKeyGet
          } else {
            current.get _ expects(3: Slice[Byte], *) returning toKeyGet
          }
        }
        Higher(2: Slice[Byte]).right.value.value shouldBe toKeyGet
      }
    }

    //     3
    //0  - 3
    // 1,2,3,4
    "4" in {
      //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val result = randomPutKeyValue(4, deadline = randomDeadlineOption(false))

        inSequence {
          current.higher _ expects(3: Slice[Byte], *) returning LevelSeek.None
          next.higher _ expects(3: Slice[Byte], *) returning result
        }
        Higher(3: Slice[Byte]).right.value.value shouldBe result
      }
    }
  }
}
