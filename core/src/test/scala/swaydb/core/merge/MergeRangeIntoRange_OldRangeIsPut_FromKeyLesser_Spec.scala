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

class MergeRangeIntoRange_OldRangeIsPut_FromKeyLesser_Spec extends TestBase {

  implicit val ordering = KeyOrder.default

  import swaydb.core.map.serializer.RangeValueSerializers._

  "SegmentMerge.merge for Ranges when ranges do not overlap" should {
    "add new Range to the left if it's smaller then the old's range fromKey and new Range's toKey overlaps old Range's fromKey and new Ranges's fromValue is None" in {
      //1 - 10
      //    10 - 20
      val newKeyValues = Slice(Memory.Range(1, 10, None, Value.Put(1)))
      val oldKeyValues = Slice(Memory.Range(10, 20, None, Value.Put(10)))

      val expected = Slice(
        Transient.Range[Value, Value](1, 10, None, Value.Put(1), 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "add new Range to the left if it's smaller then the old's range fromKey and new Range's toKey overlaps old Range's fromKey and new Ranges's fromValue is Remove" in {
      //1 - 10
      //    10 - 20
      val newKeyValues = Slice(Memory.Range(1, 10, Some(Value.Remove), Value.Put(1)))
      val oldKeyValues = Slice(Memory.Range(10, 20, None, Value.Put(10)))

      val expected = Slice(
        Transient.Range[Value, Value](1, 10, Some(Value.Remove), Value.Put(1), 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "add new Range to the left if it's smaller then the old's range fromKey and new Range's toKey overlaps old Range's fromKey and new Ranges's fromValue is Put" in {
      //1 - 10
      //    10 - 20
      val newKeyValues = Slice(Memory.Range(1, 10, Some(Value.Put(1)), Value.Put(1)))
      val oldKeyValues = Slice(Memory.Range(10, 20, None, Value.Put(10)))

      val expected = Slice(
        Transient.Range[Value, Value](1, 10, Some(Value.Put(1)), Value.Put(1), 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 1), isLastLevel = true)
    }

    "add new Range to the left if it's smaller then the old's range's fromKey and new Range's toKey does not overlaps old Range's fromKey and new Range's fromValue is Remove" in {
      //1 - 10
      //      11 - 20
      val newKeyValues = Slice(Memory.Range(1, 10, Some(Value.Remove), Value.Put(1)))
      val oldKeyValues = Slice(Memory.Range(11, 20, None, Value.Put(10)))
      val expected = Slice(
        Transient.Range[Value, Value](1, 10, Some(Value.Remove), Value.Put(1), 0.1, None),
        Transient.Range[Value, Value](11, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "add new Range to the left if it's smaller then the old's range's fromKey and new Range's toKey does not overlaps old Range's fromKey and new Range's fromValue is Put" in {
      //1 - 10
      //      11 - 20
      val newKeyValues = Slice(Memory.Range(1, 10, Some(Value.Put("one")), Value.Put(1)))
      val oldKeyValues = Slice(Memory.Range(11, 20, None, Value.Put(10)))
      val expected = Slice(
        Transient.Range[Value, Value](1, 10, Some(Value.Put("one")), Value.Put(1), 0.1, None),
        Transient.Range[Value, Value](11, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, "one"), isLastLevel = true)
    }
  }

  "SegmentMerge.merge for Ranges when old Range's rangeValue is Put & Ranges fromKeys are equal and new Range's to key is <= old Range's toKey" should {
    "new Range's range value is Put" in {
      //0   -  10
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 10, None, Value.Put(10)))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Slice(
        Transient.Range[Value, Value](0, 10, None, Value.Put(10), 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Put and new Range's fromValue set to Put" in {
      //0   -  10
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 10, Some(Value.Put(1)), Value.Put(10)))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Slice(
        Transient.Range[Value, Value](0, 10, Some(Value.Put(1)), Value.Put(10), 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(0, 1, 0.1, None), isLastLevel = true)
    }

    "new Range's range value is Put and fromValue is Put and new Range's fromValue is None" in {
      //0   -  10
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 10, None, Value.Put(1)))
      val oldKeyValues = Slice(Memory.Range(1, 20, Some(Value.Put("one")), Value.Put(20)))
      val expected = Slice(
        Transient.Range[Value, Value](0, 1, None, Value.Put(1), 0.1, None),
        Transient.Range[Value, Value](1, 10, Some(Value.Put(1)), Value.Put(1), 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 1, 0.1, None), isLastLevel = true)
    }

    "new Range's range value is Put and new Range's fromValue set to Remove" in {
      //0   -  10
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 10, Some(Value.Remove), Value.Put(10)))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Slice(
        Transient.Range[Value, Value](0, 10, Some(Value.Remove), Value.Put(10), 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)

    }

    "new Range's range value is Remove" in {
      //0   -  10
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 10, None, Value.Remove))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Slice(
        Transient.Range[Value, Value](0, 10, None, Value.Remove, 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove and fromValue of old Range is set to Put" in {
      //0   -  10
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 10, None, Value.Remove))
      val oldKeyValues = Slice(Memory.Range(1, 20, Some(Value.Put(1)), Value.Put(20)))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value, Value](0, 1, None, Value.Remove, 0.1, None),
          Transient.Range[Value, Value](1, 10, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
        ).updateStats
      )

      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value, Value](0, 10, None, Value.Remove, 0.1, None),
          Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove and new Range's fromValue set to Put" in {
      //0   -  10
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 10, Some(Value.Put(1)), Value.Remove))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Slice(
        Transient.Range[Value, Value](0, 10, Some(Value.Put(1)), Value.Remove, 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(0, 1, 0.1, None), isLastLevel = true)
    }

    "new Range's range value is Remove and new Range's fromValue set to Remove" in {
      //0   -  10
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 10, Some(Value.Remove), Value.Remove))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Slice(
        Transient.Range[Value, Value](0, 10, Some(Value.Remove), Value.Remove, 0.1, None),
        Transient.Range[Value, Value](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }
  }

  "SegmentMerge.merge for Ranges when old Range's rangeValue is Put & Ranges fromKeys are equal and new Range's to key is > old Range's toKey" should {
    "new Range's range value is Put" in {
      //0      -      21
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 21, None, Value.Put(10)))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Transient.Range[Value, Value](0, 21, None, Value.Put(10), 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Put and new Range's fromValue set to Put" in {
      //0      -      21
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 21, Some(Value.Put(1)), Value.Put(10)))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Transient.Range[Value, Value](0, 21, Some(Value.Put(1)), Value.Put(10), 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(0, 1, 0.1, None), isLastLevel = true)
    }

    "new Range's range value is Put and old Range's fromValue set to Put" in {
      //0      -      21
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 21, None, Value.Put(10)))
      val oldKeyValues = Slice(Memory.Range(1, 20, Some(Value.Put(1)), Value.Put(20)))
      val expected = Slice(
        Transient.Range[Value, Value](0, 1, None, Value.Put(10), 0.1, None),
        Transient.Range[Value, Value](1, 21, Some(Value.Put(10)), Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 10, 0.1, None), isLastLevel = true)
    }

    "new Range's range value is Put and new Range's fromValue set to Remove" in {
      //0      -      21
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 21, Some(Value.Remove), Value.Put(10)))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Transient.Range[Value, Value](0, 21, Some(Value.Remove), Value.Put(10), 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)

    }

    "new Range's range value is Remove" in {
      //0      -      21
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 21, None, Value.Remove))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Transient.Range[Value, Value](0, 21, None, Value.Remove, 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove and new Range's fromValue set to Put" in {
      //0      -      21
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 21, Some(Value.Put(1)), Value.Remove))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Transient.Range[Value, Value](0, 21, Some(Value.Put(1)), Value.Remove, 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(0, 1, 0.1, None), isLastLevel = true)
    }

    "new Range's range value is Remove and new Range's fromValue set to Remove" in {
      //0      -      21
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 21, Some(Value.Remove), Value.Remove))
      val oldKeyValues = Slice(Memory.Range(1, 20, None, Value.Put(20)))
      val expected = Transient.Range[Value, Value](0, 21, Some(Value.Remove), Value.Remove, 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)

    }

    "new Range's range value is Remove and old Range's fromValue set to Put" in {
      //0      -      21
      //  1    -    20
      val newKeyValues = Slice(Memory.Range(0, 21, None, Value.Remove))
      val oldKeyValues = Slice(Memory.Range(1, 20, Some(Value.Put(1)), Value.Put(20)))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value, Value](0, 1, None, Value.Remove, 0.1, None),
          Transient.Range[Value, Value](1, 21, Some(Value.Remove), Value.Remove, 0.1, None)
        ).updateStats
      )

      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value, Value](0, 21, None, Value.Remove, 0.1, None)
        )
      )
      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)

    }
  }

}
