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

package swaydb.core.segment.data.merge

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.TestTimer
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._

class FunctionMergerSpec extends AnyWordSpec with Matchers {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  "Merging Function into any key-value" when {
    "times are not in order" should {
      "always return old key-value" in {

        implicit val testTimer = TestTimer.Incremental()

        runThis(1000.times) {
          val key = randomBytesSlice()

          //new but has older time than oldKeyValue
          val newKeyValue = randomFunctionKeyValue(key = key)

          //oldKeyValue but it has a newer time.
          val oldKeyValue = randomFixedKeyValue(key = key)

          //          println(s"oldKeyValue: $oldKeyValue")
          //          println(s"newKeyValue: $newKeyValue")

          assertMerge(
            newKeyValue = newKeyValue,
            oldKeyValue = oldKeyValue,
            expected = oldKeyValue,
            lastLevel = oldKeyValue.toLastLevelExpected
          )
        }
      }
    }
  }
}
