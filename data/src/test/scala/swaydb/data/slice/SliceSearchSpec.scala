/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.data.slice

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class SliceSearchSpec extends WordSpec with Matchers {

  implicit val intOrder = Ordering.Int

  "binarySearch" when {

    "slice is empty" in {
      val slice = Slice.empty[Int]
      slice.binarySearch(10, Int.MinValue) shouldBe Int.MinValue
    }

    "slice has one element" in {
      val slice = Slice.create[Int](1)
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
