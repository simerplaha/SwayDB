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

package swaydb.core.segment

import org.scalatest.OptionValues._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.{CoreSpecType, CoreTestSweeper}
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.segment.SegmentTestKit._
import swaydb.core.segment.ref.search.SegmentSearchTestKit._
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._

class SegmentLowerSpec extends AnyWordSpec {

  implicit val keyOrder = KeyOrder.default

  val keyValuesCount: Int = 100

  "Segment.lower" should {
    "value the lower key from the segment that has only 1 fixed key-value" in {
      CoreTestSweeper.foreach(CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          assertSegment(
            keyValues =
              Slice(randomFixedKeyValue(1)),

            assert =
              (keyValues, segment) => {
                val readState = ThreadReadState.random
                segment.lower(0, readState).toOption shouldBe empty
                segment.lower(1, readState).toOption shouldBe empty
                segment.lower(2, readState).toOption.value shouldBe keyValues.head
              }
          )
      }
    }

    "value the lower from the segment when there are no Range key-values" in {
      CoreTestSweeper.foreach(CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          //1, 2, 3
          assertSegment(
            keyValues =
              Slice(randomFixedKeyValue(1), randomFixedKeyValue(2), randomFixedKeyValue(3)),

            assert =
              (keyValues, segment) => {
                val readState = ThreadReadState.random

                segment.lower(0, readState).toOption shouldBe empty //smallest key in this segment is 1
                segment.lower(1, readState).toOption shouldBe empty

                segment.lower(2, readState).toOption.value shouldBe keyValues.head
                segment.lower(3, readState).toOption.value shouldBe keyValues(1)
                (4 to 10) foreach {
                  i =>
                    segment.lower(i, readState).toOption.value shouldBe keyValues(2)
                }
              }
          )
      }
    }

    "value the lower from the segment when there are Range key-values" in {
      //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
      CoreTestSweeper.foreachRepeat(100.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          assertSegment(
            keyValues = Slice(
              randomFixedKeyValue(1),
              randomRangeKeyValue(2, 5),
              randomFixedKeyValue(10),
              randomRangeKeyValue(11, 20),
              randomRangeKeyValue(20, 30)
            ),

            assert =
              (keyValues, segment) => {
                val readState = ThreadReadState.random

                //0
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(0, readState).toOption shouldBe empty
                //1
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(1, readState).toOption shouldBe empty
                //    2
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(2, readState).getUnsafe shouldBe keyValues(0)
                //     3
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(3, readState).getUnsafe shouldBe keyValues(1)
                //       4
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(4, readState).getUnsafe shouldBe keyValues(1)
                //        5
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(5, readState).getUnsafe shouldBe keyValues(1)
                //          6
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(6, readState).getUnsafe shouldBe keyValues(1)
                //            10
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(10, readState).getUnsafe shouldBe keyValues(1)
                //                 11
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(11, readState).getUnsafe shouldBe keyValues(2)
                //                   12
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(12, readState).getUnsafe shouldBe keyValues(3)
                //                    19
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(19, readState).getUnsafe shouldBe keyValues(3)
                //                      20
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(20, readState).getUnsafe shouldBe keyValues(3)
                //                              21
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(21, readState).getUnsafe shouldBe keyValues(4)
                //                                29
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(29, readState).getUnsafe shouldBe keyValues(4)
                //                                 30
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.lower(30, readState).getUnsafe shouldBe keyValues(4)
              }
          )
      }
    }

    "random" in {
      CoreTestSweeper.foreachRepeat(50.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          assertSegment(
            keyValues = randomizedKeyValues(keyValuesCount, addUpdates = true),
            assert = assertLower(_, _)
          )
      }
    }
  }
}
