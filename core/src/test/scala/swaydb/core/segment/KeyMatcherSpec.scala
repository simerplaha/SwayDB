/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import swaydb.core.TestBase
import swaydb.core.data.Persistent._
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.RangeValueSerializers.UnitPutSerializer
import swaydb.core.segment.format.one.KeyMatcher
import swaydb.core.segment.format.one.MatchResult._
import swaydb.serializers.Default._
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers._

class KeyMatcherSpec extends TestBase {

  implicit val integer = new Ordering[Slice[Byte]] {
    def compare(a: Slice[Byte], b: Slice[Byte]): Int =
      IntSerializer.read(a).compareTo(IntSerializer.read(b))
  }

  /**
    * These implicits are just to make it easier to read the test cases.
    * The tests are normally for the match to Key in the following array
    *
    * -1, 0, 1, 2
    *
    * Tests check for keys to match in all positions (before and after each key)
    */
  implicit def IntToSomeKeyValue(int: Int): Option[Put] =
    Some(int)

  implicit def IntToKeyValue(int: Int): Put =
    Put(int, Reader(Slice.create[Byte](0)), 0, 0, 0, 0, 0)

  implicit def IntTupleToRange(tuple: (Int, Int)): Range =
    Range(UnitPutSerializer.rangeId, tuple._1, tuple._2, Reader(Slice.create[Byte](0)), 0, 0, 0, 0, 0)

  implicit def IntTupleToRangeOption(tuple: (Int, Int)): Option[Range] =
    Some(tuple)

  "KeyMatcher.Exact" should {

    "find matches when " in {
      //-1
      //   0, 1, 2
      KeyMatcher.Get(-1).apply(previous = 0, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = 0, next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = 1, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = 1, next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = 0, next = 1, hasMore = false) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = 0, next = 1, hasMore = true) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = 0, next = 2, hasMore = false) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = 0, next = 2, hasMore = true) shouldBe Stop

      //0
      //0, 1, 2
      KeyMatcher.Get(0).apply(previous = 0, next = None, hasMore = false) shouldBe Matched(0)
      KeyMatcher.Get(0).apply(previous = 0, next = None, hasMore = true) shouldBe Matched(0)
      KeyMatcher.Get(0).apply(previous = 1, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Get(0).apply(previous = 1, next = None, hasMore = true) shouldBe Stop
      //next should never be fetched if previous was a match. This should not occur in actual scenarios.
      //    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Matched(0)
      //    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Matched(0)
      //    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Matched(0)
      //    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Matched(0)

      //   1
      //0, 1, 2
      KeyMatcher.Get(1).apply(previous = 0, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Get(1).apply(previous = 0, next = None, hasMore = true) shouldBe Next
      KeyMatcher.Get(1).apply(previous = 1, next = None, hasMore = false) shouldBe Matched(1)
      KeyMatcher.Get(1).apply(previous = 1, next = None, hasMore = true) shouldBe Matched(1)
      KeyMatcher.Get(1).apply(previous = 0, next = 1, hasMore = false) shouldBe Matched(1)
      KeyMatcher.Get(1).apply(previous = 0, next = 1, hasMore = true) shouldBe Matched(1)
      KeyMatcher.Get(1).apply(previous = 0, next = 2, hasMore = false) shouldBe Stop
      KeyMatcher.Get(1).apply(previous = 0, next = 2, hasMore = true) shouldBe Stop

      //         3
      //0, 1, 2
      KeyMatcher.Get(3).apply(previous = 2, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Get(3).apply(previous = 2, next = None, hasMore = true) shouldBe Next
      KeyMatcher.Get(3).apply(previous = 3, next = None, hasMore = false) shouldBe Matched(3)
      KeyMatcher.Get(3).apply(previous = 3, next = None, hasMore = true) shouldBe Matched(3)
      KeyMatcher.Get(3).apply(previous = 2, next = 3, hasMore = false) shouldBe Matched(3)
      KeyMatcher.Get(3).apply(previous = 2, next = 3, hasMore = true) shouldBe Matched(3)
      KeyMatcher.Get(3).apply(previous = 2, next = 4, hasMore = false) shouldBe Stop
      KeyMatcher.Get(3).apply(previous = 2, next = 4, hasMore = true) shouldBe Stop
      KeyMatcher.Get(3).apply(previous = 0, next = 4, hasMore = true) shouldBe Stop

      //-1
      KeyMatcher.Get(-1).apply(previous = 0, next = (5, 10), hasMore = false) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = 0, next = (5, 10), hasMore = true) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = (5, 10), next = 20, hasMore = false) shouldBe Stop
      KeyMatcher.Get(-1).apply(previous = (5, 10), next = 20, hasMore = true) shouldBe Stop

      KeyMatcher.Get(0).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Get(0).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Get(0).apply(previous = (5, 10), next = 20, hasMore = false) shouldBe Stop
      KeyMatcher.Get(0).apply(previous = (5, 10), next = 20, hasMore = true) shouldBe Stop

      KeyMatcher.Get(5).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
      KeyMatcher.Get(5).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
      KeyMatcher.Get(6).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
      KeyMatcher.Get(6).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
      KeyMatcher.Get(9).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
      KeyMatcher.Get(9).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
      KeyMatcher.Get(10).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Next
      KeyMatcher.Get(10).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop

      KeyMatcher.Get(21).apply(previous = (5, 10), next = (20, 30), hasMore = false) shouldBe Matched((20, 30))
      KeyMatcher.Get(21).apply(previous = (5, 10), next = (20, 30), hasMore = true) shouldBe Matched((20, 30))
      KeyMatcher.Get(15).apply(previous = (5, 10), next = (20, 30), hasMore = true) shouldBe Stop
      KeyMatcher.Get(15).apply(previous = (5, 10), next = (20, 30), hasMore = false) shouldBe Stop
    }

    "read only up to the largest key and Stop iteration early if the next key is larger then the key to find" in {

      def find(toFind: Int) =
        (1 to 100).foldLeft(0) {
          case (iterationCount, next) =>
            val result = KeyMatcher.Get(toFind).apply(previous = next, next = Some(next + 1), hasMore = true)
            if (next + 1 == toFind) {
              result shouldBe Matched(toFind)
              iterationCount + 1
            } else if (next + 1 > toFind) {
              result shouldBe Stop
              iterationCount
            } else {
              result shouldBe Next
              iterationCount + 1
            }

        } shouldBe (toFind - 1)

      (1 to 100) foreach find

    }
  }

  "KeyMatcher.Lower" should {

    "find matches" in {
      //0, 1, 2
      KeyMatcher.Lower(-1).apply(previous = 0, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(-1).apply(previous = 0, next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(-1).apply(previous = 1, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(-1).apply(previous = 1, next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(-1).apply(previous = 0, next = 1, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(-1).apply(previous = 0, next = 1, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(-1).apply(previous = 0, next = 2, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(-1).apply(previous = 0, next = 2, hasMore = true) shouldBe Stop

      KeyMatcher.Lower(0).apply(previous = 0, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(0).apply(previous = 0, next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(0).apply(previous = 1, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(0).apply(previous = 1, next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(0).apply(previous = 0, next = 1, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(0).apply(previous = 0, next = 1, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(0).apply(previous = 0, next = 2, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(0).apply(previous = 0, next = 2, hasMore = true) shouldBe Stop

      KeyMatcher.Lower(1).apply(previous = 0, next = None, hasMore = false) shouldBe Matched(0)
      KeyMatcher.Lower(1).apply(previous = 0, next = None, hasMore = true) shouldBe Next
      KeyMatcher.Lower(1).apply(previous = 1, next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(1).apply(previous = 1, next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(1).apply(previous = 0, next = 1, hasMore = false) shouldBe Matched(0)
      KeyMatcher.Lower(1).apply(previous = 0, next = 1, hasMore = true) shouldBe Matched(0)
      KeyMatcher.Lower(1).apply(previous = 0, next = 2, hasMore = false) shouldBe Matched(0)
      KeyMatcher.Lower(1).apply(previous = 0, next = 2, hasMore = true) shouldBe Matched(0)

      KeyMatcher.Lower(2).apply(previous = 0, next = None, hasMore = false) shouldBe Matched(0)
      KeyMatcher.Lower(2).apply(previous = 0, next = None, hasMore = true) shouldBe Next
      KeyMatcher.Lower(2).apply(previous = 1, next = None, hasMore = false) shouldBe Matched(1)
      KeyMatcher.Lower(2).apply(previous = 1, next = None, hasMore = true) shouldBe Next
      KeyMatcher.Lower(2).apply(previous = 0, next = 1, hasMore = false) shouldBe Matched(1)
      KeyMatcher.Lower(2).apply(previous = 0, next = 1, hasMore = true) shouldBe Next
      KeyMatcher.Lower(2).apply(previous = 0, next = 2, hasMore = false) shouldBe Matched(0)
      KeyMatcher.Lower(2).apply(previous = 0, next = 2, hasMore = true) shouldBe Matched(0)

      KeyMatcher.Lower(3).apply(previous = 0, next = None, hasMore = false) shouldBe Matched(0)
      KeyMatcher.Lower(3).apply(previous = 0, next = None, hasMore = true) shouldBe Next
      KeyMatcher.Lower(3).apply(previous = 1, next = None, hasMore = false) shouldBe Matched(1)
      KeyMatcher.Lower(3).apply(previous = 1, next = None, hasMore = true) shouldBe Next
      KeyMatcher.Lower(3).apply(previous = 0, next = 1, hasMore = false) shouldBe Matched(1)
      KeyMatcher.Lower(3).apply(previous = 0, next = 1, hasMore = true) shouldBe Next
      KeyMatcher.Lower(3).apply(previous = 0, next = 2, hasMore = false) shouldBe Matched(2)
      KeyMatcher.Lower(3).apply(previous = 0, next = 2, hasMore = true) shouldBe Next

      KeyMatcher.Lower(3).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(3).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(4).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(4).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Stop
      KeyMatcher.Lower(5).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop
      KeyMatcher.Lower(5).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Stop

      KeyMatcher.Lower(6).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
      KeyMatcher.Lower(6).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
      KeyMatcher.Lower(10).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
      KeyMatcher.Lower(10).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))

      KeyMatcher.Lower(11).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
      KeyMatcher.Lower(11).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Next

      KeyMatcher.Lower(12).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Matched((5, 10))
      KeyMatcher.Lower(12).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Matched((5, 10))
      KeyMatcher.Lower(15).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Matched((5, 10))
      KeyMatcher.Lower(15).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Matched((5, 10))

      KeyMatcher.Lower(20).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
      KeyMatcher.Lower(20).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Next
      KeyMatcher.Lower(20).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Matched((15, 20))
      KeyMatcher.Lower(20).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Matched((15, 20))
      KeyMatcher.Lower(20).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Matched((15, 20))

      KeyMatcher.Lower(21).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Matched((15, 20))
      KeyMatcher.Lower(21).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Next
    }

    "read only minimum number of lower keys to fulfil the request" in {

      def find(toFind: Int) =
        (1 to 100).foldLeft(0) {
          case (iterationCount, next) =>
            val result = KeyMatcher.Lower(toFind).apply(previous = next, next = Some(next + 1), hasMore = true)
            if (next + 1 == toFind) {
              result shouldBe Matched(toFind - 1)
              iterationCount + 1
            } else if (next + 1 > toFind) {
              result shouldBe Stop
              iterationCount
            } else {
              result shouldBe Next
              iterationCount + 1
            }

        } shouldBe (toFind - 1)

      (1 to 100) foreach find

    }
  }

  "KeyMatcher.Higher" in {
    //0, 1, 2
    KeyMatcher.Higher(-1).apply(previous = 0, next = None, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Higher(-1).apply(previous = 0, next = None, hasMore = true) shouldBe Matched(0)
    KeyMatcher.Higher(-1).apply(previous = 1, next = None, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Higher(-1).apply(previous = 1, next = None, hasMore = true) shouldBe Matched(1)
    //    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Stop
    //    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Stop
    //    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Stop
    //    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Stop

    KeyMatcher.Higher(0).apply(previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(0).apply(previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(0).apply(previous = 1, next = None, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Higher(0).apply(previous = 1, next = None, hasMore = true) shouldBe Matched(1)
    KeyMatcher.Higher(0).apply(previous = 0, next = 1, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Higher(0).apply(previous = 0, next = 1, hasMore = true) shouldBe Matched(1)
    KeyMatcher.Higher(0).apply(previous = 0, next = 2, hasMore = false) shouldBe Matched(2)
    KeyMatcher.Higher(0).apply(previous = 0, next = 2, hasMore = true) shouldBe Matched(2)

    KeyMatcher.Higher(1).apply(previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(1).apply(previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(1).apply(previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(1).apply(previous = 1, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(1).apply(previous = 0, next = 1, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(1).apply(previous = 0, next = 1, hasMore = true) shouldBe Next
    KeyMatcher.Higher(1).apply(previous = 0, next = 2, hasMore = false) shouldBe Matched(2)
    KeyMatcher.Higher(1).apply(previous = 0, next = 2, hasMore = true) shouldBe Matched(2)

    KeyMatcher.Higher(2).apply(previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(2).apply(previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(2).apply(previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(2).apply(previous = 1, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(2).apply(previous = 0, next = 1, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(2).apply(previous = 0, next = 1, hasMore = true) shouldBe Next
    KeyMatcher.Higher(2).apply(previous = 0, next = 2, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(2).apply(previous = 0, next = 2, hasMore = true) shouldBe Next

    KeyMatcher.Higher(3).apply(previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(3).apply(previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(3).apply(previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(3).apply(previous = 1, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(3).apply(previous = 0, next = 1, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(3).apply(previous = 0, next = 1, hasMore = true) shouldBe Next
    KeyMatcher.Higher(3).apply(previous = 0, next = 2, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(3).apply(previous = 0, next = 2, hasMore = true) shouldBe Next

    KeyMatcher.Higher(3).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
    KeyMatcher.Higher(3).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
    KeyMatcher.Higher(4).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
    KeyMatcher.Higher(4).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
    KeyMatcher.Higher(5).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
    KeyMatcher.Higher(5).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))

    KeyMatcher.Higher(6).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))
    KeyMatcher.Higher(6).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
    KeyMatcher.Higher(10).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(10).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop

    KeyMatcher.Higher(11).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(11).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Next

    KeyMatcher.Higher(12).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Matched((15, 20))
    KeyMatcher.Higher(12).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Matched((15, 20))
    KeyMatcher.Higher(15).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Matched((15, 20))
    KeyMatcher.Higher(15).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Matched((15, 20))

    KeyMatcher.Higher(19).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Matched((15, 20))
    KeyMatcher.Higher(19).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Matched((15, 20))

    KeyMatcher.Higher(20).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(20).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(20).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Next
    KeyMatcher.Higher(20).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Stop
    KeyMatcher.Higher(20).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Next

    KeyMatcher.Higher(21).apply(previous = (5, 10), next = (15, 20), hasMore = false) shouldBe Stop
    KeyMatcher.Higher(21).apply(previous = (5, 10), next = (15, 20), hasMore = true) shouldBe Next
  }
}
