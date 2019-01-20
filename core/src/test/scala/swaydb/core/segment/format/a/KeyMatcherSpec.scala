/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.format.a

import swaydb.core.TestBase
import swaydb.core.data.Persistent._
import swaydb.core.group.compression.GroupDecompressor
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.KeyMatcher
import swaydb.core.segment.format.a.MatchResult._
import swaydb.core.segment.format.a.entry.reader.value.{LazyGroupValueReader, LazyRangeValueReader, LazyValueReader}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TryAssert._
import swaydb.core.data.{Persistent, Time}
import swaydb.data.repairAppendix.MaxKey

class KeyMatcherSpec extends TestBase {

  implicit val integer = new KeyOrder[Slice[Byte]] {
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

  implicit def toPut(int: Int): Persistent.Fixed =
    Put(int, None, LazyValueReader(Reader(Slice.emptyBytes), 0, 0), Time.empty,  0, 0, 0, 0, 0)

  implicit def toSomePut(int: Int): Option[Persistent.Fixed] =
    Some(int)

  object RangeImplicits {

    /**
      * Convenience implicits to make it easier to read the test cases.
      * A tuple indicates a range's (fromKey, toKey)
      */

    implicit def toRange(tuple: (Int, Int)): Persistent.Range =
      Persistent.Range(tuple._1, tuple._2, LazyRangeValueReader(Reader(Slice.emptyBytes), 0, 0), 0, 0, 0, 0, 0)

    implicit def toSomeRange(tuple: (Int, Int)): Option[Persistent.Range] =
      Some(tuple)
  }

  object GroupImplicits {

    /**
      * Convenience implicits to make it easier to read the test cases.
      * A tuple (_1, _2) indicates a Groups minKey & maxKey where the maxKey is [[MaxKey.Fixed]]
      *
      * A tuple of tuple (_1, (_1, _2)) indicates a Groups minKey & maxKey where the maxKey is [[MaxKey.Range]]
      */
    implicit def toGroupFixed(tuple: (Int, Int)): Group =
      Group(
        _minKey = tuple._1,
        _maxKey = MaxKey.Fixed(tuple._2),
        groupDecompressor = GroupDecompressor(Reader(Slice.emptyBytes), 0),
        lazyGroupValueReader = LazyGroupValueReader(Reader(Slice.emptyBytes), 0, 0),
        valueReader = Reader(Slice.emptyBytes),
        nextIndexOffset = 0,
        nextIndexSize = 0,
        indexOffset = 0,
        valueOffset = 0,
        valueLength = 0,
        None
      )

    implicit def toGroupRange(tuple: (Int, (Int, Int))): Group =
      Group(
        _minKey = tuple._1,
        _maxKey = MaxKey.Range(tuple._2._1, tuple._2._2),
        groupDecompressor = GroupDecompressor(Reader(Slice.emptyBytes), 0),
        lazyGroupValueReader = LazyGroupValueReader(Reader(Slice.emptyBytes), 0, 0),
        valueReader = Reader(Slice.emptyBytes),
        nextIndexOffset = 0,
        nextIndexSize = 0,
        indexOffset = 0,
        valueOffset = 0,
        valueLength = 0,
        None
      )

    implicit def toSomeGroupFixed(tuple: (Int, Int)): Option[Group] =
      Some(tuple)

    implicit def toSomeGroupRange(tuple: (Int, (Int, Int))): Option[Group] =
      Some(tuple)
  }

  "KeyMatcher" should {
    "Get" when {
      "Put" in {
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
      }

      //range tests
      "Range" in {
        import RangeImplicits._

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

      //group tests
      "Group" in {
        import GroupImplicits._

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
        KeyMatcher.Get(10).apply(previous = (5, 10), next = None, hasMore = true) shouldBe Matched((5, 10))
        KeyMatcher.Get(10).apply(previous = (5, 10), next = None, hasMore = false) shouldBe Matched((5, 10))

        KeyMatcher.Get(15).apply(previous = (5, 10), next = (20, 30), hasMore = true) shouldBe Stop
        KeyMatcher.Get(15).apply(previous = (5, 10), next = (20, 30), hasMore = false) shouldBe Stop
        KeyMatcher.Get(21).apply(previous = (5, 10), next = (20, 30), hasMore = true) shouldBe Matched((20, 30))
        KeyMatcher.Get(21).apply(previous = (5, 10), next = (20, 30), hasMore = false) shouldBe Matched((20, 30))
        KeyMatcher.Get(30).apply(previous = (5, 10), next = (20, 30), hasMore = true) shouldBe Matched((20, 30))
        KeyMatcher.Get(30).apply(previous = (5, 10), next = (20, 30), hasMore = false) shouldBe Matched((20, 30))
        KeyMatcher.Get(31).apply(previous = (5, 10), next = (20, 30), hasMore = true) shouldBe Next
        KeyMatcher.Get(31).apply(previous = (5, 10), next = (20, 30), hasMore = false) shouldBe Stop

        //GROUP WHEN THERE MAX KEY IS A RANGE (EXCLUSIVE)
        KeyMatcher.Get(-1).apply(previous = 0, next = (5, (10, 20)), hasMore = false) shouldBe Stop
        KeyMatcher.Get(-1).apply(previous = 0, next = (5, (10, 20)), hasMore = true) shouldBe Stop
        KeyMatcher.Get(-1).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop
        KeyMatcher.Get(-1).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Stop
        KeyMatcher.Get(-1).apply(previous = (5, (10, 20)), next = 20, hasMore = false) shouldBe Stop
        KeyMatcher.Get(-1).apply(previous = (5, (10, 20)), next = 20, hasMore = true) shouldBe Stop

        KeyMatcher.Get(0).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop
        KeyMatcher.Get(0).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Stop
        KeyMatcher.Get(0).apply(previous = (5, (10, 20)), next = 20, hasMore = false) shouldBe Stop
        KeyMatcher.Get(0).apply(previous = (5, (10, 20)), next = 20, hasMore = true) shouldBe Stop

        KeyMatcher.Get(5).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(5).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(6).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(6).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(9).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(9).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(10).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(10).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(15).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(15).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Get(20).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next
        KeyMatcher.Get(20).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop
        KeyMatcher.Get(21).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next
        KeyMatcher.Get(21).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop

        KeyMatcher.Get(15).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Stop
        KeyMatcher.Get(15).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Stop
        KeyMatcher.Get(21).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Get(21).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Get(30).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Get(30).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Get(31).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Get(31).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Get(40).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Next
        KeyMatcher.Get(40).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Stop
      }

      "up to the largest key and Stop iteration early if the next key is larger then the key to find" in {

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

    "Lower" when {
      "Put" in {
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

      }

      "Range" in {

        import RangeImplicits._

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

      "Group" in {
        import GroupImplicits._

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

        //Group when maxKey is range
        KeyMatcher.Lower(3).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop
        KeyMatcher.Lower(3).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Stop
        KeyMatcher.Lower(4).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop
        KeyMatcher.Lower(4).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Stop
        KeyMatcher.Lower(5).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop
        KeyMatcher.Lower(5).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Stop

        KeyMatcher.Lower(6).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(6).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(10).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(10).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(11).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(11).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(19).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(19).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(20).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(20).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(21).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(21).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next

        KeyMatcher.Lower(12).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(12).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(15).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(15).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((5, (10, 20)))

        KeyMatcher.Lower(20).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(20).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(20).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(20).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((5, (10, 20)))

        KeyMatcher.Lower(21).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(21).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next
        KeyMatcher.Lower(21).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Lower(21).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))

        KeyMatcher.Lower(30).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(30).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next
        KeyMatcher.Lower(30).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Lower(30).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))

        KeyMatcher.Lower(40).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(40).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next
        KeyMatcher.Lower(40).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Lower(40).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))

        KeyMatcher.Lower(41).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Lower(41).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next
        KeyMatcher.Lower(41).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Next
        KeyMatcher.Lower(41).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
      }

      "minimum number of lower keys to fulfil the request" in {

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

    "Higher" when {

      "Put" in {
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
      }

      "Range" in {
        import RangeImplicits._

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

      "Group" in {
        import GroupImplicits._

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

        //When max key is Range
        KeyMatcher.Higher(3).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(3).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(4).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(4).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(5).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(5).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))

        KeyMatcher.Higher(6).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(6).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(10).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(10).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(11).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(11).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))

        KeyMatcher.Higher(12).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(12).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Matched((5, (10, 20)))
        KeyMatcher.Higher(20).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop
        KeyMatcher.Higher(20).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next

        KeyMatcher.Higher(12).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(12).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(15).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(15).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))

        KeyMatcher.Higher(19).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(19).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))

        KeyMatcher.Higher(20).apply(previous = (5, (10, 20)), next = None, hasMore = false) shouldBe Stop
        KeyMatcher.Higher(20).apply(previous = (5, (10, 20)), next = None, hasMore = true) shouldBe Next
        KeyMatcher.Higher(20).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(20).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))

        KeyMatcher.Higher(21).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(21).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))

        KeyMatcher.Higher(30).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(30).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(39).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Matched((20, (30, 40)))
        KeyMatcher.Higher(39).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Matched((20, (30, 40)))

        KeyMatcher.Higher(40).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = false) shouldBe Stop
        KeyMatcher.Higher(40).apply(previous = (5, (10, 20)), next = (20, (30, 40)), hasMore = true) shouldBe Next

      }
    }
  }
}
