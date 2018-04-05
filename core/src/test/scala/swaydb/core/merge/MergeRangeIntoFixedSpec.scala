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

package swaydb.core.merge

import swaydb.core.TestBase
import swaydb.core.data.{Memory, Transient, Value}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MergeRangeIntoFixedSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  import swaydb.core.map.serializer.RangeValueSerializers._

  "SegmentMerge.merge when merging Range key-value into Fixed key-values" should {
    "add smaller Range key-value to the left if it's does not overlap the range key-value" in {
      //1 - 10
      //    10
      val newKeyValues = Slice(Memory.Range(1, 10, Option.empty[Value.Put], Value.Put(10)))
      val oldKeyValues = Slice(Memory.Put(10, "new value"))

      val expected =
        Slice(
          Transient.Range(1, 10, Option.empty[Value.Put], Value.Put(10), 0.1, None),
          Transient.Put(10, "new value")
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed produces the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(10, "new value"), isLastLevel = true)
    }

    "add smaller Range key-value if it's does not overlap the range key-value and keep the input Put fromValue" in {
      //1 - 9
      //      10
      val newKeyValues = Slice(Memory.Range(1, 9, Some(Value.Put(1)), Value.Put(9)))
      val oldKeyValues = Slice(Memory.Put(10, 10))

      val expected =
        Slice(
          Transient.Range(1, 9, Some(Value.Put(1)), Value.Put(9), 0.1, None),
          Transient.Put(10, 10)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed produces the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Slice(Transient.Put(1, 1), Transient.Put(10, 10)).updateStats, isLastLevel = true)
    }

    "add smaller Range key-value if it's does not overlap the range key-value and keep the input Remove fromValue" in {
      //1 - 9
      //      10
      val newKeyValues = Slice(Memory.Range(1, 9, Some(Value.Remove), Value.Put(9)))
      val oldKeyValues = Slice(Memory.Put(10, "new value value"))
      val expected = Slice(
        Transient.Range[Value.Remove, Value.Put](1, 9, Some(Value.Remove), Value.Put(9), 0.1, None),
        Transient.Put(10, "new value value")
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed produces the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(10, "new value value"), isLastLevel = true)
    }

    "add larger Range key-value to the right if it's does not overlap the range key-value" in {
      //         11 - 20
      //      10
      val newKeyValues = Slice(Memory.Range(11, 20, Option.empty[Value.Put], Value.Put(9)))
      val oldKeyValues = Slice(Memory.Put(10, "new value"))
      val expected = Slice(
        Transient.Put(10, "new value"),
        Transient.Range(11, 20, Option.empty[Value.Put], Value.Put(9), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed produces the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(10, "new value"), isLastLevel = true)
    }

    "add larger Range key-value to the right if it's does not overlap the range key-value and keep the input Put fromValue" in {
      //         11 - 20
      //      10
      val newKeyValues = Slice(Memory.Range(11, 20, Some(Value.Put(11)), Value.Put(9)))
      val oldKeyValues = Slice(Memory.Put(10, 10))
      val expected = Slice(
        Transient.Put(10, 10),
        Transient.Range(11, 20, Some(Value.Put(11)), Value.Put(9), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed produces the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Slice(Transient.Put(10, 10), Transient.Put(11, 11)).updateStats, isLastLevel = true)
    }

    "add larger Range key-value if it's does not overlap the range key-value and keep the input Remove fromValue" in {
      //         11 - 20
      //      10
      val newKeyValues = Slice(Memory.Range(11, 20, Some(Value.Remove), Value.Put(9)))
      val oldKeyValues = Slice(Memory.Put(10, 10))

      val expected = Slice(
        Transient.Put(10, 10),
        Transient.Range[Value.Remove, Value.Put](11, 20, Some(Value.Remove), Value.Put(9), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed produces the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(10, 10), isLastLevel = true)
    }

    "split key-value with update value if range overlaps a fixed value" in {
      // 1  -   11
      //      10
      val newKeyValues = Slice(Memory.Range(1, 11, None, Value.Put("updated")))
      val oldKeyValues = Slice(Memory.Put(10, 10))

      val expected = Slice(
        Transient.Range[Value, Value](1, 10, None, Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](10, 11, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(10, "updated"), isLastLevel = true)
    }

    "split key-value with update value and keep range's Remove fromValue if range overlaps a fixed value" in {
      // 1  -   11
      //      10
      val newKeyValues = Slice(Memory.Range(1, 11, Some(Value.Remove), Value.Put("updated")))
      val oldKeyValues = Slice(Memory.Put(10, 10))

      val expected = Slice(
        Transient.Range[Value, Value](1, 10, Some(Value.Remove), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](10, 11, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(10, "updated"), isLastLevel = true)
    }

    "split key-value with update value and keep range's Put fromValue if range overlaps a fixed value" in {
      // 1  -   11
      //      10
      val newKeyValues = Slice(Memory.Range(1, 11, Some(Value.Put("1")), Value.Put("updated")))
      val oldKeyValues = Slice(Memory.Put(10, 10))

      val expected = Slice(
        Transient.Range[Value, Value](1, 10, Some(Value.Put("1")), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](10, 11, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Slice(Transient.Put(1, "1"), Transient.Put(10, "updated")).updateStats, isLastLevel = true)
    }

    "apply update Range to Put key-value when fixed key equiv to Range's fromKey" in {
      // 1  -   11
      // 1
      val newKeyValues = Slice(Memory.Range(1, 11, None, Value.Put("updated")))
      val oldKeyValues = Slice(Memory.Put(1, 1))

      val expected = Slice(
        Transient.Range[Value, Value](1, 11, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Slice(Transient.Put(1, "updated")).updateStats, isLastLevel = true)
    }

    "Remove Put key-value when fixed key equiv Range's fromKey and Range's fromKey is remove" in {
      // 1  -   11
      // 1
      val newKeyValues = Slice(Memory.Range(1, 11, Some(Value.Remove), Value.Put("updated")))
      val oldKeyValues = Slice(Memory.Put(1, 1))

      val expected = Slice(
        Transient.Range[Value, Value](1, 11, Some(Value.Remove), Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
    }

    "remove all key-values" in {
      //  0    -         25
      //    2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(0, 25, Some(Value.Remove), Value.Remove))
      val oldKeyValues =
        Slice(
          Memory.Put(2, "new value value"),
          Memory.Put(7, "new value value"),
          Memory.Remove(10),
          Memory.Put(20, "new value value")
        )
      val expected = Transient.Range[Value, Value](0, 25, Some(Value.Remove), Value.Remove, 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
    }

    "remove all key-values and return an empty result if it's the last level" in {
      //  0    -         25
      //    2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(0, 25, None, Value.Remove))
      val oldKeyValues =
        Slice(
          Memory.Put(2, 2),
          Memory.Put(7, 7),
          Memory.Remove(10),
          Memory.Put(20, 20)
        )

      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
      assertSkipListMerge(newKeyValues, oldKeyValues, newKeyValues)
    }

    "remove all key-values and return an empty result if it's the last level and range's from value is also Remove" in {
      //  0    -         25
      //    2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(0, 25, Some(Value.Remove), Value.Remove))
      val oldKeyValues =
        Slice(
          Memory.Put(2, 2),
          Memory.Put(7, 7),
          Memory.Remove(10),
          Memory.Put(20, 20)
        )

      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
      assertSkipListMerge(newKeyValues, oldKeyValues, newKeyValues)

    }

    "remove all key-values within the range only when range's last key does overlaps and existing key" in {
      //       3  -    20
      //  1, 2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(3, 20, None, Value.Remove))
      val oldKeyValues =
        Slice(
          Memory.Remove(1),
          Memory.Put(2, "new value value"),
          Memory.Put(7, "new value value"),
          Memory.Remove(10),
          Memory.Put(20, "new value value")
        )

      val expected = Slice(
        Transient.Remove(1),
        Transient.Put(2, "new value value"),
        Transient.Range[Value, Value](3, 20, None, Value.Remove, 0.1, None),
        Transient.Put(20, "new value value")
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level
      assertMerge(newKeyValues, oldKeyValues, expected.filter(_.isInstanceOf[Transient.Put]).updateStats, isLastLevel = true)
    }

    "remove all key-values within the range only when range's keys overlaps and existing key" in {
      //     2    -    20
      //  1, 2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(2, 20, None, Value.Remove))
      val oldKeyValues =
        Slice(
          Memory.Remove(1),
          Memory.Put(2, "new value value"),
          Memory.Put(7, "new value value"),
          Memory.Remove(10),
          Memory.Put(20, "new value value")
        )

      val expected = Slice(
        Transient.Remove(1),
        Transient.Range[Value, Value](2, 20, None, Value.Remove, 0.1, None),
        Transient.Put(20, "new value value")
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      assertMerge(newKeyValues, oldKeyValues, expected.filter(_.isInstanceOf[Transient.Put]).updateStats, isLastLevel = true)
    }

    "remove all key-values within the range only when range's first key does not overlap and existing key" in {
      // 1    -   8
      //     2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(1, 8, None, Value.Remove))
      val oldKeyValues =
        Slice(
          Memory.Put(2, "new value value"),
          Memory.Put(7, "new value value"),
          Memory.Remove(10),
          Memory.Put(20, "new value value")
        )

      val expected =
        Slice(
          Transient.Range[Value, Value](1, 8, None, Value.Remove, 0.1, None),
          Transient.Remove(10),
          Transient.Put(20, "new value value")
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      assertMerge(newKeyValues, oldKeyValues, expected.filter(_.isInstanceOf[Transient.Put]).updateStats, isLastLevel = true)
    }

    "remove all key-values but keep the range's fromValue" in {
      // 1            -          100
      // 1,  7, 10, 20
      val newKeyValues = Slice(Memory.Range(1, 100, Some(Value.Put(100)), Value.Remove))
      val oldKeyValues =
        Slice(
          Memory.Put(1, 1),
          Memory.Put(7, "new value value"),
          Memory.Remove(10),
          Memory.Put(20, "new value value")
        )

      val expected = Slice(Transient.Range[Value, Value](1, 100, Some(Value.Put(100)), Value.Remove, 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      assertMerge(newKeyValues, oldKeyValues, Transient.Put(1, 100), isLastLevel = true)
    }

    "update all key-values within the range only when range's first key does not overlap and existing key" in {
      // 1     -     15
      //     2, 7, 10, 20
      val newKeyValues = Slice(Memory.Range(1, 15, None, Value.Put(15)))
      val oldKeyValues =
        Slice(
          Memory.Put(2, "new value value"),
          Memory.Put(7, "new value value"),
          Memory.Remove(10),
          Memory.Put(20, "new value value")
        )
      val expected =
        Slice(
          Transient.Range[Value, Value](1, 2, None, Value.Put(15), 0.1, None),
          Transient.Range[Value, Value](2, 7, Some(Value.Put(15)), Value.Put(15), 0.1, None),
          Transient.Range[Value, Value](7, 10, Some(Value.Put(15)), Value.Put(15), 0.1, None),
          Transient.Range[Value, Value](10, 15, Some(Value.Remove), Value.Put(15), 0.1, None),
          Transient.Put(20, "new value value")
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      val lastLevelExpected = Slice(
        Transient.Put(2, 15),
        Transient.Put(7, 15),
        Transient.Put(20, "new value value")
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, lastLevelExpected, isLastLevel = true)
    }

    "update all key-values within the range only when range's last key does not overlap and existing key" in {
      //       6     -     30
      //     2, 7, 10, 20  30, 31
      val newKeyValues = Slice(Memory.Range(6, 30, None, Value.Put("updated")))
      val oldKeyValues =
        Slice(
          Memory.Put(2, "new value"),
          Memory.Put(7, "new value"),
          Memory.Remove(10),
          Memory.Put(20, "new value"),
          Memory.Put(30, "new value"),
          Memory.Remove(31)
        )
      val expected = Slice(
        Transient.Put(2, "new value"),
        Transient.Range[Value, Value](6, 7, None, Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](7, 10, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](10, 20, Some(Value.Remove), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](20, 30, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None),
        Transient.Put(30, "new value"),
        Transient.Remove(31)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      val lastLevelExpected =
        Slice(
          Transient.Put(2, "new value"),
          Transient.Put(7, "updated"),
          Transient.Put(20, "updated"),
          Transient.Put(30, "new value")
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, lastLevelExpected, isLastLevel = true)
    }

    "update all key-values when range's last key does not overlap and existing key" in {
      //     2         -           40
      //     2, 7, 10, 20  30, 31
      val newKeyValues = Slice(Memory.Range(2, 40, Some(Value.Remove), Value.Put("updated")))
      val oldKeyValues =
        Slice(
          Memory.Put(2, "old value"),
          Memory.Put(7, "old value"),
          Memory.Remove(10),
          Memory.Put(20, "old value"),
          Memory.Put(30, "old value"),
          Memory.Remove(31)
        )
      val expected = Slice(
        Transient.Range[Value, Value](2, 7, Some(Value.Remove), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](7, 10, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](10, 20, Some(Value.Remove), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](20, 30, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](30, 31, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](31, 40, Some(Value.Remove), Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      val lastLevelExpected = Slice(
        Transient.Put(7, "updated"),
        Transient.Put(20, "updated"),
        Transient.Put(30, "updated")
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, lastLevelExpected, isLastLevel = true)
    }

    "update all key-values when there are multiple new ranges" in {
      //     2    -   11,       31  -   51
      //  1, 2, 7, 10,   20  30, 35, 50,  53, 80
      val newKeyValues =
      Slice(
        Memory.Range(2, 11, None, Value.Put("updated")),
        Memory.Range(31, 51, Some(Value.Remove), Value.Put("updated 2"))
      )

      val oldKeyValues =
        Slice(
          Memory.Put(1, "old value"),
          Memory.Put(2, "old value"),
          Memory.Put(7, "old value"),
          Memory.Remove(10),
          Memory.Put(20, "old value"),
          Memory.Remove(30),
          Memory.Remove(35),
          Memory.Put(50, "old value"),
          Memory.Put(53, "old value"),
          Memory.Put(80, "old value")
        )

      val expected = Slice(
        Transient.Put(1, "old value"),
        Transient.Range[Value, Value](2, 7, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](7, 10, Some(Value.Put("updated")), Value.Put("updated"), 0.1, None),
        Transient.Range[Value, Value](10, 11, Some(Value.Remove), Value.Put("updated"), 0.1, None),
        Transient.Put(20, "old value"),
        Transient.Remove(30),
        Transient.Range[Value, Value](31, 35, Some(Value.Remove), Value.Put("updated 2"), 0.1, None),
        Transient.Range[Value, Value](35, 50, Some(Value.Remove), Value.Put("updated 2"), 0.1, None),
        Transient.Range[Value, Value](50, 51, Some(Value.Put("updated 2")), Value.Put("updated 2"), 0.1, None),
        Transient.Put(53, "old value"),
        Transient.Put(80, "old value")
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      val lastLevelExpected = Slice(
        Transient.Put(1, "old value"),
        Transient.Put(2, "updated"),
        Transient.Put(7, "updated"),
        Transient.Put(20, "old value"),
        Transient.Put(50, "updated 2"),
        Transient.Put(53, "old value"),
        Transient.Put(80, "old value")
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, lastLevelExpected, isLastLevel = true)
    }

  }

}
