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
import swaydb.core.data.{Transient, Value}
import swaydb.core.segment.SegmentMerge
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MergeRangeIntoRange_OldRangeIsRemove_FromKeyGreater_Spec extends TestBase {

  implicit val ordering = KeyOrder.default

  import swaydb.core.map.serializer.RangeValueSerializers._

  "SegmentMerge.merge for Ranges when old Range's rangeValue is Remove & Ranges fromKey > and new Range's fromKey & new Range's toKey is <= old Range's toKey" should {
    "old Range's rangeValue is Remove & new Range's range value is Put" in {
      //  2 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Put(1), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None)
        )
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        )
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove, new Range's range value is Put, new Range's fromValue is Put" in {
      //  2 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Put(2)), Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 20, Some(Value.Put(2)), Value.Remove, 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Put(2)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(2, value = 2, 0.1, None), isLastLevel = true)
    }

    "old Range's rangeValue is Remove, new Range's range value is Put, new Range's fromValue is Remove" in {
      //  2 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Remove), Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 20, Some(Value.Remove), Value.Remove, 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove" in {
      //  2 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )
      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Put" in {
      //  2 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Put(10)), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 20, Some(Value.Put(10)), Value.Remove, 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Put(10)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(2, 10, 0.1, None), isLastLevel = true)

    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Remove" in {
      //  2 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Remove), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 20, Some(Value.Remove), Value.Remove, 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }
  }

  "SegmentMerge.merge for Ranges when old Range's rangeValue is Remove & Ranges fromKeys are equal & new Range's toKey is greater" should {
    "old Range's rangeValue is Remove & new Range's range value is Put" in {
      //  2    -     21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 21, None, Value.Put("updated"), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Put("updated"), 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 20, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Put("updated"), 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove, new Range's range value is Put, new Range's fromValue is Put" in {
      //  2    -     21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 21, Some(Value.Put(1)), Value.Put("updated"), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](2, 20, Some(Value.Put(1)), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(2, value = 1, 0.1, None), isLastLevel = true)
    }

    "old Range's rangeValue is Remove, new Range's range value is Put, new Range's fromValue is Remove" in {
      //  2    -     21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 21, Some(Value.Remove), Value.Put("updated"), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](2, 20, Some(Value.Remove), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove" in {
      //  2    -     21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 21, None, Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 21, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Put" in {
      //  2    -     21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 21, Some(Value.Put(1)), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 20, Some(Value.Put(1)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 21, Some(Value.Put(1)), Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(2, 1), isLastLevel = true)

    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Remove" in {
      //  2    -     21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](2, 21, Some(Value.Remove), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 20, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 2, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](2, 21, Some(Value.Remove), Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }
  }

}
