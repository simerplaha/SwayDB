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
import swaydb.core.data.Persistent.CreatedReadOnly
import swaydb.core.io.reader.Reader
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
  implicit def IntToKeyValue(int: Int): CreatedReadOnly =
    CreatedReadOnly(int, Reader(Slice.create[Byte](0)), 0, 0, 0, 0, 0)

  implicit def IntToSomeKeyValue(int: Int): Option[CreatedReadOnly] =
    Some(CreatedReadOnly(int, Reader(Slice.create[Byte](0)), 0, 0, 0, 0, 0))

  "KeyMatcher.Exact" in {
    //0, 1, 2
    KeyMatcher.Exact(-1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(-1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Stop
    KeyMatcher.Exact(-1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(-1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Stop
    KeyMatcher.Exact(-1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(-1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Stop
    KeyMatcher.Exact(-1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(-1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Stop

    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Matched(0)
    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Stop
    //next should never be fetched if previous was a match. This should not occur in actual scenarios.
    //    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Matched(0)
    //    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Matched(0)
    //    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Matched(0)
    //    KeyMatcher.Exact(0).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Matched(0)

    KeyMatcher.Exact(1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Exact(1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Exact(1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Matched(1)
    KeyMatcher.Exact(1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Exact(1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Matched(1)
    KeyMatcher.Exact(1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Stop

    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 2, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 2, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 3, next = None, hasMore = false) shouldBe Matched(3)
    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 3, next = None, hasMore = true) shouldBe Matched(3)
    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 2, next = 3, hasMore = false) shouldBe Matched(3)
    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 2, next = 3, hasMore = true) shouldBe Matched(3)
    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 2, next = 4, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 2, next = 4, hasMore = true) shouldBe Stop
    KeyMatcher.Exact(3).apply[CreatedReadOnly](previous = 0, next = 4, hasMore = true) shouldBe Stop

    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 4, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 4, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 5, next = None, hasMore = false) shouldBe Matched(5)
    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 5, next = None, hasMore = true) shouldBe Matched(5)
    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 4, next = 5, hasMore = false) shouldBe Matched(5)
    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 4, next = 5, hasMore = true) shouldBe Matched(5)
    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 2, next = 6, hasMore = false) shouldBe Stop
    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 2, next = 6, hasMore = true) shouldBe Stop
    KeyMatcher.Exact(5).apply[CreatedReadOnly](previous = 0, next = 6, hasMore = true) shouldBe Stop
  }

  "KeyMatcher.Lower" in {
    //0, 1, 2
    KeyMatcher.Lower(-1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Lower(-1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Stop
    KeyMatcher.Lower(-1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Lower(-1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Stop
    //    KeyMatcher.Lower(-1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Stop
    //    KeyMatcher.Lower(-1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Stop
    //    KeyMatcher.Lower(-1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Stop
    //    KeyMatcher.Lower(-1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Stop

    KeyMatcher.Lower(0).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Lower(0).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Stop
    KeyMatcher.Lower(0).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Lower(0).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Stop
    //    KeyMatcher.Lower(0).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Stop
    //    KeyMatcher.Lower(0).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Stop
    //    KeyMatcher.Lower(0).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Stop
    //    KeyMatcher.Lower(0).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Stop

    KeyMatcher.Lower(1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Lower(1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Lower(1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Lower(1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Stop
    KeyMatcher.Lower(1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Lower(1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Matched(0)
    KeyMatcher.Lower(1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Lower(1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Matched(0)

    KeyMatcher.Lower(2).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Lower(2).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Lower(2).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Lower(2).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Lower(2).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Lower(2).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Next
    KeyMatcher.Lower(2).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Lower(2).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Matched(0)

    KeyMatcher.Lower(3).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Lower(3).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Lower(3).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Lower(3).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Lower(3).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Lower(3).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Next
    KeyMatcher.Lower(3).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Matched(2)
    KeyMatcher.Lower(3).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Next
  }

  "KeyMatcher.Higher" in {
    //0, 1, 2
    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Matched(0)
    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Matched(0)
    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Matched(1)
    //    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Stop
    //    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Stop
    //    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Stop
    //    KeyMatcher.Higher(-1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Stop

    KeyMatcher.Higher(0).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(0).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(0).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Higher(0).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Matched(1)
    KeyMatcher.Higher(0).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Matched(1)
    KeyMatcher.Higher(0).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Matched(1)
    KeyMatcher.Higher(0).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Matched(2)
    KeyMatcher.Higher(0).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Matched(2)

    KeyMatcher.Higher(1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(1).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(1).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(1).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Next
    KeyMatcher.Higher(1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Matched(2)
    KeyMatcher.Higher(1).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Matched(2)

    KeyMatcher.Higher(2).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(2).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(2).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(2).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(2).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(2).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Next
    KeyMatcher.Higher(2).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(2).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Next

    KeyMatcher.Higher(3).apply[CreatedReadOnly](previous = 0, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(3).apply[CreatedReadOnly](previous = 0, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(3).apply[CreatedReadOnly](previous = 1, next = None, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(3).apply[CreatedReadOnly](previous = 1, next = None, hasMore = true) shouldBe Next
    KeyMatcher.Higher(3).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(3).apply[CreatedReadOnly](previous = 0, next = 1, hasMore = true) shouldBe Next
    KeyMatcher.Higher(3).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = false) shouldBe Stop
    KeyMatcher.Higher(3).apply[CreatedReadOnly](previous = 0, next = 2, hasMore = true) shouldBe Next

  }

}
