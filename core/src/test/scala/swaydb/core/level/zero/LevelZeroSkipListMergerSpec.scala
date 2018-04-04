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
package swaydb.core.level.zero

import java.util.concurrent.ConcurrentSkipListMap

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions
import swaydb.core.data.{Memory, Value}
import swaydb.core.data.Value._
import swaydb.core.level.zero.LevelZeroSkipListMerge._
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._

class LevelZeroSkipListMergerSpec extends WordSpec with Matchers with CommonAssertions {
  implicit val ordering = swaydb.order.KeyOrder.default

  "insert" should {
    "insert a Fixed value to an empty skipList" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      insert(1, Memory.Put(1, "one"), skipList)
      skipList should have size 1

      skipList.asScala.head shouldBe ((1: Slice[Byte], Memory.Put(1, "one")))
    }

    "insert multiple fixed key-values" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      (0 to 9) foreach {
        i =>
          insert(i, Memory.Put(i, i), skipList)
      }

      skipList should have size 10

      skipList.asScala.zipWithIndex foreach {
        case ((key, value), index) =>
          key shouldBe (index: Slice[Byte])
          value shouldBe Memory.Put(index, index)
      }
    }

    "insert multiple non-overlapping ranges" in {
      //10 | 20 | 40 | 100
      //1  | 10 | 30 | 50
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      insert(1, Memory.Range(1, 10, None, Value.Put(10)), skipList)
      insert(10, Memory.Range(10, 20, None, Value.Remove), skipList)
      insert(30, Memory.Range(30, 40, None, Value.Put(40)), skipList)
      insert(50, Memory.Range(50, 100, Some(Value.Put(20)), Value.Remove), skipList)

      val skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe ((1: Slice[Byte], Memory.Range(1, 10, None, Value.Put(10))))
      skipListArray(1) shouldBe ((10: Slice[Byte], Memory.Range(10, 20, None, Value.Remove)))
      skipListArray(2) shouldBe ((30: Slice[Byte], Memory.Range(30, 40, None, Value.Put(40))))
      skipListArray(3) shouldBe ((50: Slice[Byte], Memory.Range(50, 100, Some(Value.Put(20)), Value.Remove)))
    }

    "insert overlapping ranges when insert fromKey is less than existing range's fromKey" in {
      //1-15
      //  20
      //  10

      //result:
      //15 | 20
      //1  | 15

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      insert(10, Memory.Range(10, 20, None, Value.Put(20)), skipList)
      insert(1, Memory.Range(1, 15, None, Value.Put(40)), skipList)
      skipList should have size 2
      skipList.asScala.head shouldBe(1: Slice[Byte], Memory.Range(1, 15, None, Value.Put(40)))
      skipList.asScala.last shouldBe(15: Slice[Byte], Memory.Range(15, 20, None, Value.Put(20)))
    }

    "insert overlapping ranges when insert fromKey is less than existing range's from key and fromKey is set" in {
      //1-15
      //  20 (R - Put(20)
      //  10 (Put(10))

      //result:
      //10 | 15 | 20
      //1  | 10 | 15

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)

      //insert with put
      insert(10, Memory.Range(10, 20, Some(Value.Put(10)), Value.Put(20)), skipList)
      insert(1, Memory.Range(1, 15, None, Value.Put(40)), skipList)
      skipList should have size 3
      val skipListArray = skipList.asScala.toArray

      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, 10, None, Value.Put(40)))
      skipListArray(1) shouldBe(10: Slice[Byte], Memory.Range(10, 15, Some(Value.Put(40)), Value.Put(40)))
      skipListArray(2) shouldBe(15: Slice[Byte], Memory.Range(15, 20, None, Value.Put(20)))
    }

    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      //10
      //1
      insert(1, Memory.Range(1, 15, None, Value.Put(40)), skipList)
      insert(10, Memory.Range(10, 20, None, Value.Put(20)), skipList)
      skipList should have size 2
      skipList.asScala.head shouldBe(1: Slice[Byte], Memory.Range(1, 10, None, Value.Put(40)))
      skipList.asScala.last shouldBe(10: Slice[Byte], Memory.Range(10, 20, None, Value.Put(20)))
    }

    "insert overlapping ranges when insert fromKey is greater than existing range's fromKey and fromKey is set" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      //15
      //1 (Put(1))
      insert(1, Memory.Range(1, 15, Some(Value.Put(1)), Value.Put(40)), skipList)
      insert(10, Memory.Range(10, 20, None, Value.Put(20)), skipList)
      skipList should have size 2
      skipList.asScala.head shouldBe(1: Slice[Byte], Memory.Range(1, 10, Some(Value.Put(1)), Value.Put(40)))
      skipList.asScala.last shouldBe(10: Slice[Byte], Memory.Range(10, 20, None, Value.Put(20)))
    }

    "insert overlapping ranges without values set and no splits required" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      insert(1, Memory.Range(1, 5, None, Value.Put(5)), skipList)
      insert(5, Memory.Range(5, 10, None, Value.Put(10)), skipList)
      insert(10, Memory.Range(10, 20, None, Value.Put(20)), skipList)
      insert(20, Memory.Range(20, 30, None, Value.Put(30)), skipList)
      insert(30, Memory.Range(30, 40, None, Value.Put(40)), skipList)
      insert(40, Memory.Range(40, 50, None, Value.Put(50)), skipList)

      insert(10, Memory.Range(10, 100, None, Value.Put(100)), skipList)
      skipList should have size 3

      val skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, 5, None, Value.Put(5)))
      skipListArray(1) shouldBe(5: Slice[Byte], Memory.Range(5, 10, None, Value.Put(10)))
      skipListArray(2) shouldBe(10: Slice[Byte], Memory.Range(10, 100, None, Value.Put(100)))
    }

    "insert overlapping ranges with values set and no splits required" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      insert(1, Memory.Range(1, 5, Some(Value.Put(1)), Value.Put(5)), skipList)
      insert(5, Memory.Range(5, 10, None, Value.Put(10)), skipList)
      insert(10, Memory.Range(10, 20, Some(Value.Put(10)), Value.Put(20)), skipList)
      insert(20, Memory.Range(20, 30, None, Value.Put(30)), skipList)
      insert(30, Memory.Range(30, 40, Some(Value.Put(30)), Value.Put(40)), skipList)
      insert(40, Memory.Range(40, 50, None, Value.Put(50)), skipList)

      insert(10, Memory.Range(10, 100, None, Value.Put(100)), skipList)
      skipList should have size 4

      val skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, 5, Some(Value.Put(1)), Value.Put(5)))
      skipListArray(1) shouldBe(5: Slice[Byte], Memory.Range(5, 10, None, Value.Put(10)))
      skipListArray(2) shouldBe(10: Slice[Byte], Memory.Range(10, 30, Some(Value.Put(100)), Value.Put(100)))
      skipListArray(3) shouldBe(30: Slice[Byte], Memory.Range(30, 100, Some(Value.Put(100)), Value.Put(100)))
    }

    "insert overlapping ranges with values set and splits required" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      insert(1, Memory.Range(1, 5, Some(Value.Put(1)), Value.Put(5)), skipList)
      insert(5, Memory.Range(5, 10, None, Value.Put(10)), skipList)
      insert(10, Memory.Range(10, 20, Some(Value.Put(10)), Value.Put(20)), skipList)
      insert(20, Memory.Range(20, 30, None, Value.Put(30)), skipList)
      insert(30, Memory.Range(30, 40, Some(Value.Put(30)), Value.Put(40)), skipList)
      insert(40, Memory.Range(40, 50, None, Value.Put(50)), skipList)

      insert(7, Memory.Range(7, 35, None, Value.Put(100)), skipList)
      skipList should have size 7

      val skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, toKey = 5, fromValue = Some(Put(1)), rangeValue = Put(5)))
      skipListArray(1) shouldBe(5: Slice[Byte], Memory.Range(5, toKey = 7, fromValue = None, rangeValue = Put(10)))
      skipListArray(2) shouldBe(7: Slice[Byte], Memory.Range(7, toKey = 10, fromValue = None, rangeValue = Put(100)))
      skipListArray(3) shouldBe(10: Slice[Byte], Memory.Range(10, toKey = 30, fromValue = Some(Put(100)), rangeValue = Put(100)))
      skipListArray(4) shouldBe(30: Slice[Byte], Memory.Range(30, toKey = 35, fromValue = Some(Put(100)), rangeValue = Put(100)))
      skipListArray(5) shouldBe(35: Slice[Byte], Memory.Range(35, toKey = 40, fromValue = None, rangeValue = Put(40)))
      skipListArray(6) shouldBe(40: Slice[Byte], Memory.Range(40, toKey = 50, fromValue = None, rangeValue = Put(50)))
    }

    "insert overlapping ranges and fixed values with values set and splits required" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      insert(1, Memory.Range(1, 10, Some(Value.Put(1)), Value.Put(10)), skipList)
      insert(15, Memory.Put(15, 15), skipList)
      insert(20, Memory.Range(20, 25, Some(Value.Put(80)), Value.Remove), skipList)
      insert(25, Memory.Range(25, 30, Some(Value.Remove), Value.Put(25)), skipList)
      insert(40, Memory.Range(40, 50, None, Value.Put(30)), skipList)
      insert(50, Memory.Put(50, 50), skipList)
      insert(52, Memory.Remove(52), skipList)
      insert(53, Memory.Range(53, 60, None, Value.Put(60)), skipList)
      insert(60, Memory.Put(60, 60), skipList)
      insert(70, Memory.Range(70, 100, None, Value.Put(100)), skipList)
      insert(100, Memory.Remove(100), skipList)
      skipList should have size 11

      insert(7, Memory.Range(7, 35, None, Value.Put(100)), skipList)
      skipList should have size 12
      var skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, toKey = 7, fromValue = Some(Put(1)), rangeValue = Put(10)))
      skipListArray(1) shouldBe(7: Slice[Byte], Memory.Range(7, toKey = 15, fromValue = None, rangeValue = Put(100)))
      skipListArray(2) shouldBe(15: Slice[Byte], Memory.Range(15, toKey = 20, fromValue = Some(Put(100)), rangeValue = Put(100)))
      skipListArray(3) shouldBe(20: Slice[Byte], Memory.Range(20, toKey = 25, fromValue = Some(Put(100)), rangeValue = Remove))
      skipListArray(4) shouldBe(25: Slice[Byte], Memory.Range(25, toKey = 35, fromValue = Some(Remove), rangeValue = Put(100)))
      skipListArray(5) shouldBe(40: Slice[Byte], Memory.Range(40, toKey = 50, fromValue = None, rangeValue = Put(30)))
      skipListArray(6) shouldBe(50: Slice[Byte], Memory.Put(50, 50))
      skipListArray(7) shouldBe(52: Slice[Byte], Memory.Remove(52))
      skipListArray(8) shouldBe(53: Slice[Byte], Memory.Range(53, toKey = 60, fromValue = None, rangeValue = Put(60)))
      skipListArray(9) shouldBe(60: Slice[Byte], Memory.Put(60, 60))
      skipListArray(10) shouldBe(70: Slice[Byte], Memory.Range(70, toKey = 100, fromValue = None, rangeValue = Put(100)))
      skipListArray(11) shouldBe(100: Slice[Byte], Memory.Remove(100))

      insert(40, Memory.Range(40, 45, None, Value.Remove), skipList)
      skipList should have size 13
      skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, toKey = 7, fromValue = Some(Put(1)), rangeValue = Put(10)))
      skipListArray(1) shouldBe(7: Slice[Byte], Memory.Range(7, toKey = 15, fromValue = None, rangeValue = Put(100)))
      skipListArray(2) shouldBe(15: Slice[Byte], Memory.Range(15, toKey = 20, fromValue = Some(Put(100)), rangeValue = Put(100)))
      skipListArray(3) shouldBe(20: Slice[Byte], Memory.Range(20, toKey = 25, fromValue = Some(Put(100)), rangeValue = Remove))
      skipListArray(4) shouldBe(25: Slice[Byte], Memory.Range(25, toKey = 35, fromValue = Some(Remove), rangeValue = Put(100)))
      skipListArray(5) shouldBe(40: Slice[Byte], Memory.Range(40, toKey = 45, fromValue = None, rangeValue = Value.Remove))
      skipListArray(6) shouldBe(45: Slice[Byte], Memory.Range(45, toKey = 50, fromValue = None, rangeValue = Put(30)))
      skipListArray(7) shouldBe(50: Slice[Byte], Memory.Put(50, 50))
      skipListArray(8) shouldBe(52: Slice[Byte], Memory.Remove(52))
      skipListArray(9) shouldBe(53: Slice[Byte], Memory.Range(53, toKey = 60, fromValue = None, rangeValue = Put(60)))
      skipListArray(10) shouldBe(60: Slice[Byte], Memory.Put(60, 60))
      skipListArray(11) shouldBe(70: Slice[Byte], Memory.Range(70, toKey = 100, fromValue = None, rangeValue = Put(100)))
      skipListArray(12) shouldBe(100: Slice[Byte], Memory.Remove(100))

      insert(15, Memory.Range(15, 60, None, Value.Put(200)), skipList)
      skipList should have size 12
      skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, toKey = 7, fromValue = Some(Put(1)), rangeValue = Put(10)))
      skipListArray(1) shouldBe(7: Slice[Byte], Memory.Range(7, toKey = 15, fromValue = None, rangeValue = Put(100)))
      skipListArray(2) shouldBe(15: Slice[Byte], Memory.Range(15, toKey = 20, fromValue = Some(Put(200)), rangeValue = Put(200)))
      skipListArray(3) shouldBe(20: Slice[Byte], Memory.Range(20, toKey = 25, fromValue = Some(Put(200)), rangeValue = Remove))
      skipListArray(4) shouldBe(25: Slice[Byte], Memory.Range(25, toKey = 40, fromValue = Some(Remove), rangeValue = Put(200)))
      skipListArray(5) shouldBe(40: Slice[Byte], Memory.Range(40, toKey = 45, fromValue = None, rangeValue = Value.Remove))
      skipListArray(6) shouldBe(45: Slice[Byte], Memory.Range(45, toKey = 50, fromValue = None, rangeValue = Put(200)))
      skipListArray(7) shouldBe(50: Slice[Byte], Memory.Range(50, toKey = 52, fromValue = Some(Value.Put(200)), rangeValue = Put(200)))
      skipListArray(8) shouldBe(52: Slice[Byte], Memory.Range(52, toKey = 60, fromValue = Some(Value.Remove), rangeValue = Put(200)))
      skipListArray(9) shouldBe(60: Slice[Byte], Memory.Put(60, 60))
      skipListArray(10) shouldBe(70: Slice[Byte], Memory.Range(70, toKey = 100, fromValue = None, rangeValue = Put(100)))
      skipListArray(11) shouldBe(100: Slice[Byte], Memory.Remove(100))

      insert(12, Memory.Range(12, 80, None, Value.Remove), skipList)
      skipList should have size 5
      skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe(1: Slice[Byte], Memory.Range(1, toKey = 7, fromValue = Some(Put(1)), rangeValue = Put(10)))
      skipListArray(1) shouldBe(7: Slice[Byte], Memory.Range(7, toKey = 12, fromValue = None, rangeValue = Put(100)))
      skipListArray(2) shouldBe(12: Slice[Byte], Memory.Range(12, toKey = 80, fromValue = None, rangeValue = Remove))
      skipListArray(3) shouldBe(80: Slice[Byte], Memory.Range(80, toKey = 100, fromValue = None, rangeValue = Put(100)))
      skipListArray(4) shouldBe(100: Slice[Byte], Memory.Remove(100))

      insert(0, Memory.Range(0, 101, None, Value.Remove), skipList)
      skipList should have size 1
      skipListArray = skipList.asScala.toArray
      skipListArray(0) shouldBe(0: Slice[Byte], Memory.Range(0, toKey = 101, fromValue = None, rangeValue = Remove))
    }
  }

}
