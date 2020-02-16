/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
package swaydb.core.level.zero


import org.scalatest.OptionValues._
import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestTimer
import swaydb.core.data.{Memory, MemoryOption, Value}
import swaydb.core.util.skiplist.SkipList
import swaydb.data.order.TimeOrder
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.serializers.Default._
import swaydb.serializers._

class LevelZeroSkipListMergerSpec extends WordSpec with Matchers {
  implicit val keyOrder = swaydb.data.order.KeyOrder.default
  implicit val merger = swaydb.core.level.zero.LevelZeroSkipListMerger
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder = TimeOrder.long

  import merger._

  "insert" should {
    "insert a Fixed value to an empty skipList" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      val put = Memory.put(1, "one")
      insert(1, put, skipList)
      skipList should have size 1

      skipList.asScala.head shouldBe ((1: Slice[Byte], put))
    }

    "insert multiple fixed key-values" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      (0 to 9) foreach {
        i =>
          insert(i, Memory.put(i, i), skipList)
      }

      skipList should have size 10

      skipList.asScala.zipWithIndex foreach {
        case ((key, value), index) =>
          key shouldBe (index: Slice[Byte])
          value shouldBe Memory.put(index, index)
      }
    }

    "insert multiple non-overlapping ranges" in {
      //10 | 20 | 40 | 100
      //1  | 10 | 30 | 50
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      insert(10, Memory.Range(10, 20, Value.FromValue.Null, Value.remove(None)), skipList)
      insert(30, Memory.Range(30, 40, Value.FromValue.Null, Value.update(40)), skipList)
      insert(50, Memory.Range(50, 100, Value.put(20), Value.remove(None)), skipList)

      val skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe ((10: Slice[Byte], Memory.Range(10, 20, Value.FromValue.Null, Value.remove(None))))
      skipListArray(1) shouldBe ((30: Slice[Byte], Memory.Range(30, 40, Value.FromValue.Null, Value.update(40))))
      skipListArray(2) shouldBe ((50: Slice[Byte], Memory.Range(50, 100, Value.put(20), Value.remove(None))))
    }

    "insert overlapping ranges when insert fromKey is less than existing range's fromKey" in {
      //1-15
      //  20
      //  10

      //result:
      //15 | 20
      //1  | 15

      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      insert(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)), skipList)
      insert(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40)), skipList)
      skipList should have size 3

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.update(40))
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(40))
      skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
    }

    "insert overlapping ranges when insert fromKey is less than existing range's from key and fromKey is set" in {
      //1-15
      //  20 (R - Put(20)
      //  10 (Put(10))

      //result:
      //10 | 15 | 20
      //1  | 10 | 15

      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      //insert with put
      insert(10, Memory.Range(10, 20, Value.put(10), Value.update(20)), skipList)
      insert(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40)), skipList)
      skipList should have size 3
      val skipListArray = skipList.asScala.toArray

      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, 10, Value.FromValue.Null, Value.update(40)))
      skipListArray(1) shouldBe(10: Slice[Byte], Memory.Range(10, 15, Value.put(40), Value.update(40)))
      skipListArray(2) shouldBe(15: Slice[Byte], Memory.Range(15, 20, Value.FromValue.Null, Value.update(20)))
    }

    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      //10
      //1
      insert(1, Memory.Range(1, 15, Value.FromValue.Null, Value.update(40)), skipList)
      insert(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)), skipList)
      skipList should have size 3

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.update(40))
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(20))
      skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
    }

    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey and fromKey is set" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      //15
      //1 (Put(1))
      insert(1, Memory.Range(1, 15, Value.put(1), Value.update(40)), skipList)
      insert(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)), skipList)

      skipList should have size 3

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 10, Value.put(1), Value.update(40))
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.update(20))
      skipList.get(15: Slice[Byte]).getS shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
    }

    "insert overlapping ranges without values set and no splits required" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      insert(1, Memory.Range(1, 5, Value.FromValue.Null, Value.update(5)), skipList)
      insert(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10)), skipList)
      insert(10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)), skipList)
      insert(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30)), skipList)
      insert(30, Memory.Range(30, 40, Value.FromValue.Null, Value.update(40)), skipList)
      insert(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50)), skipList)

      insert(10, Memory.Range(10, 100, Value.FromValue.Null, Value.update(100)), skipList)
      skipList should have size 7

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.FromValue.Null, Value.update(5))
      skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.FromValue.Null, Value.update(100))
      skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
      skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 40, Value.FromValue.Null, Value.update(100))
      skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(100))
      skipList.get(50: Slice[Byte]).getS shouldBe Memory.Range(50, 100, Value.FromValue.Null, Value.update(100))
    }

    "insert overlapping ranges with values set and no splits required" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      insert(1, Memory.Range(1, 5, Value.put(1), Value.update(5)), skipList)
      insert(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10)), skipList)
      insert(10, Memory.Range(10, 20, Value.put(10), Value.update(20)), skipList)
      insert(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30)), skipList)
      insert(30, Memory.Range(30, 40, Value.put(30), Value.update(40)), skipList)
      insert(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50)), skipList)

      insert(10, Memory.Range(10, 100, Value.FromValue.Null, Value.update(100)), skipList)
      skipList should have size 7

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.put(1), Value.update(5))
      skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 10, Value.FromValue.Null, Value.update(10))
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.put(100), Value.update(100))
      skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
      skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 40, Value.put(100), Value.update(100))
      skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(100))
      skipList.get(50: Slice[Byte]).getS shouldBe Memory.Range(50, 100, Value.FromValue.Null, Value.update(100))
    }

    "insert overlapping ranges with values set and splits required" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      insert(1, Memory.Range(1, 5, Value.put(1), Value.update(5)), skipList)
      insert(5, Memory.Range(5, 10, Value.FromValue.Null, Value.update(10)), skipList)
      insert(10, Memory.Range(10, 20, Value.put(10), Value.update(20)), skipList)
      insert(20, Memory.Range(20, 30, Value.FromValue.Null, Value.update(30)), skipList)
      insert(30, Memory.Range(30, 40, Value.put(30), Value.update(40)), skipList)
      insert(40, Memory.Range(40, 50, Value.FromValue.Null, Value.update(50)), skipList)

      insert(7, Memory.Range(7, 35, Value.FromValue.Null, Value.update(100)), skipList)
      skipList should have size 8

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 5, Value.put(1), Value.update(5))
      skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 7, Value.FromValue.Null, Value.update(10))
      skipList.get(7: Slice[Byte]).getS shouldBe Memory.Range(7, 10, Value.FromValue.Null, Value.update(100))
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.Range(10, 20, Value.put(100), Value.update(100))
      skipList.get(20: Slice[Byte]).getS shouldBe Memory.Range(20, 30, Value.FromValue.Null, Value.update(100))
      skipList.get(30: Slice[Byte]).getS shouldBe Memory.Range(30, 35, Value.put(100), Value.update(100))
      skipList.get(35: Slice[Byte]).getS shouldBe Memory.Range(35, 40, Value.FromValue.Null, Value.update(40))
      skipList.get(40: Slice[Byte]).getS shouldBe Memory.Range(40, 50, Value.FromValue.Null, Value.update(50))
    }

    "remove range should remove invalid entries" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      insert(1, Memory.put(1, 1), skipList)
      insert(2, Memory.put(2, 2), skipList)
      insert(4, Memory.put(4, 4), skipList)
      insert(5, Memory.put(5, 5), skipList)

      insert(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None)), skipList)
      skipList should have size 3

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
      skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None))
      skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(4: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(5: Slice[Byte]).getS shouldBe Memory.put(5, 5)

      insert(5, Memory.remove(5), skipList)
      skipList.get(5: Slice[Byte]).getS shouldBe Memory.remove(5)

      skipList should have size 3
    }

    "remove range when skipList is empty" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      insert(2, Memory.Range(2, 100, Value.FromValue.Null, Value.remove(None)), skipList)
      skipList should have size 1

      skipList.get(1: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 100, Value.FromValue.Null, Value.remove(None))
      skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(4: Slice[Byte]).toOptionS shouldBe empty
    }

    "remove range should clear removed entries when remove ranges overlaps the left edge" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      //1           -              10
      (1 to 10) foreach {
        i =>
          insert(i, Memory.put(i, i), skipList)
      }

      //1           -              10
      //       4    -      8
      insert(4, Memory.Range(4, 8, Value.FromValue.Null, Value.remove(None)), skipList)
      insert(8, Memory.remove(8), skipList)
      //1           -              10
      //   2    -    5
      insert(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None)), skipList)

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
      skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 4, Value.FromValue.Null, Value.remove(None))
      skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.FromValue.Null, Value.remove(None))
      skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 8, Value.FromValue.Null, Value.remove(None))
      skipList.get(6: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(7: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(8: Slice[Byte]).getS shouldBe Memory.remove(8)
      skipList.get(9: Slice[Byte]).getS shouldBe Memory.put(9, 9)
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
    }

    "remove range should clear removed entries when remove ranges overlaps the right edge" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      //1           -              10
      (1 to 10) foreach {
        i =>
          insert(i, Memory.put(i, i), skipList)
      }
      //1           -              10
      //   2    -    5
      insert(2, Memory.Range(2, 5, Value.FromValue.Null, Value.remove(None)), skipList)
      insert(5, Memory.remove(5), skipList)
      //1           -              10
      //       4    -      8
      insert(4, Memory.Range(4, 8, Value.FromValue.Null, Value.remove(None)), skipList)
      //      insert(8, Memory.remove(8), skipList)

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.put(1, 1)
      skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 4, Value.FromValue.Null, Value.remove(None))
      skipList.get(3: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.FromValue.Null, Value.remove(None))
      skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 8, Value.FromValue.Null, Value.remove(None))
      skipList.get(6: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(7: Slice[Byte]).toOptionS shouldBe empty
      skipList.get(8: Slice[Byte]).getS shouldBe Memory.put(8, 8)
      skipList.get(9: Slice[Byte]).getS shouldBe Memory.put(9, 9)
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
    }

    "insert fixed key-values into remove range" in {
      val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      //1           -              10
      insert(1, Memory.Range(1, 10, Value.FromValue.Null, Value.remove(None)), skipList)
      (1 to 10) foreach {
        i =>
          insert(i, Memory.put(i, i), skipList)
      }

      skipList.get(1: Slice[Byte]).getS shouldBe Memory.Range(1, 2, Value.put(1), Value.remove(None))
      skipList.get(2: Slice[Byte]).getS shouldBe Memory.Range(2, 3, Value.put(2), Value.remove(None))
      skipList.get(3: Slice[Byte]).getS shouldBe Memory.Range(3, 4, Value.put(3), Value.remove(None))
      skipList.get(4: Slice[Byte]).getS shouldBe Memory.Range(4, 5, Value.put(4), Value.remove(None))
      skipList.get(5: Slice[Byte]).getS shouldBe Memory.Range(5, 6, Value.put(5), Value.remove(None))
      skipList.get(6: Slice[Byte]).getS shouldBe Memory.Range(6, 7, Value.put(6), Value.remove(None))
      skipList.get(7: Slice[Byte]).getS shouldBe Memory.Range(7, 8, Value.put(7), Value.remove(None))
      skipList.get(8: Slice[Byte]).getS shouldBe Memory.Range(8, 9, Value.put(8), Value.remove(None))
      skipList.get(9: Slice[Byte]).getS shouldBe Memory.Range(9, 10, Value.put(9), Value.remove(None))
      skipList.get(10: Slice[Byte]).getS shouldBe Memory.put(10, 10)
    }
  }
}
