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
import swaydb.core.data.{KeyValue, Memory, Transient, Value}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MergeFixedIntoRangeSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  import swaydb.core.map.serializer.RangeValueSerializers._

  "SegmentMerge.merge when merging Fixed key-value into a Range key-value" should {
    "add Fixed smaller key-value if it's does not overlap the range key-value" in {
      //1
      //  2 - 3
      val newKeyValues = Slice(Memory.Put(1, "new value"))
      val oldKeyValues = Slice(Memory.Range(2, 3, Option.empty[Value.Put], Value.Put(10)))

      val expected = Slice(Transient.Put(1, "new value"), Transient.Range(2, 3, Option.empty[Value.Put], Value.Put(10), 0.1, None)).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(1, "new value"), isLastLevel = true)
    }

    "add Fixed larger key-value if it does not overlap the range key-value" in {
      //      4
      //2 - 3
      val newKeyValues = Slice(Memory.Put(4, "four"))
      val oldKeyValues = Slice(Memory.Range(2, 3, Option.empty[Value.Put], Value.Put(10)))

      val expected = Seq(Transient.Range(2, 3, Option.empty[Value.Put], Value.Put(10), 0.1, None), Transient.Put(4, "four")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(4, "four"), isLastLevel = true)
    }

    "update the range's fromValue with Put if the input key-values key matches range's fromKey" in {
      //2
      //2 - 3
      val newKeyValues = Slice(Memory.Put(2, 2))
      val oldKeyValues = Slice(Memory.Range(2, 3, Option.empty[Value.Put], Value.Put(10)))

      val expected = Slice(Transient.Range(2, 3, Some(Value.Put(2)), Value.Put(10), 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(2, 2), isLastLevel = true)
    }

    "update the range's fromValue with Remove if the input key-values key matches range's fromKey" in {
      //2
      //2 - 3
      val newKeyValues = Slice(Memory.Remove(2))
      val oldKeyValues = Slice(Memory.Range(2, 3, Option.empty[Value.Remove], Value.Put(10)))

      val expected = Slice(Transient.Range[Value.Remove, Value.Put](2, 3, Some(Value.Remove), Value.Put(10), 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
    }

    "split the range's if the input key-values key overlaps range's second key" in {
      //  11
      //10   -   20
      val newKeyValues = Slice(Memory.Put(11, 11))
      val oldKeyValues = Slice(Memory.Range(10, 20, Option.empty[Value.Put], Value.Put(10)))

      val expected: Slice[KeyValue.WriteOnly] =
        Seq(
          Transient.Range(10, 11, Option.empty[Value.Put], Value.Put(10), 0.1, None),
          Transient.Range(11, 20, Some(Value.Put(11)), Value.Put(10), 0.1, None)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(11, 11), isLastLevel = true)
    }

    "split the range's if the input key-value key overlaps range's mid key" in {
      //    15
      //10   -   20
      val newKeyValues = Slice(Memory.Put(15, 15))
      val oldKeyValues = Slice(Memory.Range(10, 20, Option.empty[Value.Put], Value.Put("ranges value")))

      val expected =
        Seq(
          Transient.Range(10, 15, Option.empty[Value.Put], Value.Put("ranges value"), 0.1, None),
          Transient.Range(15, 20, Some(Value.Put(15)), Value.Put("ranges value"), 0.1, None)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(15, 15), isLastLevel = true)
    }

    "split the range if the input key-values key overlaps range's mid key" in {
      //  12,  18
      //10   -   20
      val newKeyValues = Slice(Memory.Put(12, 12), Memory.Remove(18))
      val oldKeyValues = Slice(Memory.Range(10, 20, Option.empty[Value.Put], Value.Put("ranges value")))

      val expected =
        Seq(
          Transient.Range(10, 12, Option.empty[Value.Put], Value.Put("ranges value"), 0.1, None),
          Transient.Range(12, 18, Some(Value.Put(12)), Value.Put("ranges value"), 0.1, None),
          Transient.Range[Value.Remove, Value.Put](18, 20, Some(Value.Remove), Value.Put("ranges value"), 0.1, None)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(12, 12), isLastLevel = true)
    }

    "add the input Put key-value as a new key-value if the key is equal to range's toKey" in {
      //         20
      //10   -   20
      val newKeyValues = Slice(Memory.Put(20, 20))
      val oldKeyValues = Slice(Memory.Range(10, 20, Option.empty[Value.Put], Value.Put("ranges value")))

      val expected =
        Seq(
          Transient.Range(10, 20, Option.empty[Value.Put], Value.Put("ranges value"), 0.1, None),
          Transient.Put(20, 20)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(20, 20), isLastLevel = true)
    }

    "split the range if the input key-values key overlaps range's multiple keys (random mix test)" in {
      //9, 10, 11, 15, 18,    23,      27,  30
      //   10      -     20        25   -   30
      val newKeyValues =
      Slice(
        Memory.Put(9, 9),
        Memory.Put(10, 10),
        Memory.Put(11, 11),
        Memory.Remove(15),
        Memory.Put(18, 18),
        Memory.Remove(21),
        Memory.Put(23, 23),
        Memory.Remove(25),
        Memory.Put(27, 27),
        Memory.Put(30, 30)
      )

      val oldKeyValues =
        Slice(
          Memory.Range(10, 20, Option.empty[Value.Put], Value.Put("ranges value 1")),
          Memory.Range(25, 30, Some(Value.Put(25)), Value.Put("ranges value 2"))
        )

      val expected =
        Seq(
          Transient.Put(9, 9),
          Transient.Range(10, 11, Some(Value.Put(10)), Value.Put("ranges value 1"), 0.1, None),
          Transient.Range(11, 15, Some(Value.Put(11)), Value.Put("ranges value 1"), 0.1, None),
          Transient.Range[Value.Remove, Value.Put](15, 18, Some(Value.Remove), Value.Put("ranges value 1"), 0.1, None),
          Transient.Range(18, 20, Some(Value.Put(18)), Value.Put("ranges value 1"), 0.1, None),
          Transient.Remove(21),
          Transient.Put(23, 23),
          Transient.Range[Value.Remove, Value.Put](25, 27, Some(Value.Remove), Value.Put("ranges value 2"), 0.1, None),
          Transient.Range[Value.Put, Value.Put](27, 30, Some(Value.Put(27)), Value.Put("ranges value 2"), 0.1, None),
          Transient.Put(30, 30)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last level check
      val expectedInLastLevel =
        Slice(
          Transient.Put(9, 9),
          Transient.Put(10, 10),
          Transient.Put(11, 11),
          Transient.Put(18, 18),
          Transient.Put(23, 23),
          Transient.Put(27, 27),
          Transient.Put(30, 30)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expectedInLastLevel, isLastLevel = true)
    }
  }

}
