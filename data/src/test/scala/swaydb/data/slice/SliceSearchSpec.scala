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

package swaydb.data.slice

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class SliceSearchSpec extends AnyWordSpec with Matchers {

  implicit val intOrder = Ordering.Int

  "binarySearch" when {

    "slice is empty" in {
      val slice = Slice.empty[Int]
      slice.binarySearch(10, Int.MinValue) shouldBe Int.MinValue
    }

    "slice has one element" in {
      val slice = Slice.of[Int](1)
      slice add 1
      slice.binarySearch(1, Int.MinValue) shouldBe 1
    }

    "slice has many elements" in {
      val slice = Slice.range(1, 10000)

      Random.shuffle((1 to slice.size).toList) foreach {
        i =>
          slice.binarySearch(i, Int.MinValue) shouldBe i
      }
    }
  }
}
