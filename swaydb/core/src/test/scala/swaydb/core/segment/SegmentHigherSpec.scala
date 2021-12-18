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
import swaydb.testkit.RunThis._

class SegmentHigherSpec extends AnyWordSpec {

  val keyValuesCount: Int = 1000

  "Segment.higher" should {
    "value the higher key from the segment that has only 1 Remove key" in {
      CoreTestSweeper.foreachRepeat(50.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          assertSegment(
            keyValues = Slice(randomFixedKeyValue(1)),

            assert =
              (keyValue, segment) => {
                val readState = ThreadReadState.random

                segment.higher(0, readState).getUnsafe shouldBe keyValue.head
                segment.higher(1, readState).toOption shouldBe empty
                segment.higher(2, readState).toOption shouldBe empty
              }
          )
      }
    }

    "value the higher key from the segment that has only 1 Range key" in {
      CoreTestSweeper.foreachRepeat(50.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          assertSegment(
            keyValues = Slice(randomRangeKeyValue(1, 10)),

            assert =
              (keyValue, segment) => {
                val readState = ThreadReadState.random

                (0 to 9) foreach {
                  i =>
                    segment.higher(i, readState).getUnsafe shouldBe keyValue.head
                }

                (10 to 15) foreach {
                  i =>
                    segment.higher(i, readState).toOption shouldBe empty
                }
              }
          )
      }
    }

    "value the higher from the segment when there are no Range key-values" in {
      CoreTestSweeper.foreach(CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          //1, 2, 3
          assertSegment(
            keyValues = Slice(randomFixedKeyValue(1), randomFixedKeyValue(2), randomFixedKeyValue(3)),

            assert =
              (keyValues, segment) => {
                val readState = ThreadReadState.random

                segment.higher(0, readState).getUnsafe shouldBe keyValues(0)
                segment.higher(1, readState).getUnsafe shouldBe keyValues(1)
                segment.higher(2, readState).getUnsafe shouldBe keyValues(2)
                (3 to 10) foreach {
                  i =>
                    segment.higher(i, readState).toOption shouldBe empty
                }
              }
          )
      }
    }

    "value the higher from the segment when there are Range key-values" in {
      //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
      CoreTestSweeper.foreach(CoreSpecType.all) {
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
              //            randomGroup(Slice(randomFixedKeyValue(30), randomRangeKeyValue(40, 50))).toMemory
            ),

            assert =
              (keyValues, segment) => {
                val readState = ThreadReadState.random
                //0
                //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(0, readState).getUnsafe shouldBe keyValues(0)
                //1
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(1, readState).getUnsafe shouldBe keyValues(1)
                //    2
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(2, readState).getUnsafe shouldBe keyValues(1)
                //     3
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(3, readState).getUnsafe shouldBe keyValues(1)
                //       4
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(4, readState).getUnsafe shouldBe keyValues(1)
                //        5
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(5, readState).getUnsafe shouldBe keyValues(2)
                //          6
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(6, readState).getUnsafe shouldBe keyValues(2)
                //            10
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(10, readState).getUnsafe shouldBe keyValues(3)
                //                 11
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(11, readState).getUnsafe shouldBe keyValues(3)
                //                   12
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(12, readState).getUnsafe shouldBe keyValues(3)
                //                    19
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(19, readState).getUnsafe shouldBe keyValues(3)
                //                      20
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(20, readState).getUnsafe shouldBe keyValues(4)
                //                              21
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(21, readState).getUnsafe shouldBe keyValues(4)
                //                                29
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(29, readState).getUnsafe shouldBe keyValues(4)
                //                                                 50
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(50, readState).toOption shouldBe empty
                //                                                     51
                //1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                segment.higher(51, readState).toOption shouldBe empty
              }
          )
      }
    }

    "random" in {
      CoreTestSweeper.foreach(CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          assertSegment(
            keyValues = randomizedKeyValues(keyValuesCount),
            assert = assertHigher(_, _)
          )
      }
    }
  }
}
