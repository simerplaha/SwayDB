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
import swaydb.core.segment.SegmentTestKit._
import swaydb.effect.IOValues._
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._

class SegmentKeyValueCountSpec extends AnyWordSpec {

  implicit val keyOrder = KeyOrder.default

  val keyValuesCount: Int = 1000

  "Segment.keyValueCount" should {

    "return 1 when the Segment contains only 1 key-value" in {
      CoreTestSweeper.foreachRepeat(10.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          assertSegment(
            keyValues = randomizedKeyValues(1),

            assert =
              (keyValues, segment) => {
                keyValues should have size 1
                segment.keyValueCount.runRandomIO.get shouldBe keyValues.size
              }
          )
      }
    }

    "return the number of randomly generated key-values where there are no Groups" in {
      CoreTestSweeper.foreachRepeat(10.times, CoreSpecType.all) {
        (_sweeper, _specType) =>

          implicit val sweeper: CoreTestSweeper = _sweeper
          implicit val specType: CoreSpecType = _specType

          import sweeper.testCoreFunctionStore

          assertSegment(
            keyValues = randomizedKeyValues(keyValuesCount),

            assert =
              (keyValues, segment) => {
                segment.keyValueCount.runRandomIO.get shouldBe keyValues.size
              }
          )
      }
    }
  }
}
