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

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.TestBase
import swaydb.core.data.{Transient, Value}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MergeRangeIntoRange_OldRangeIsRemove_FromKeyEqual_Spec extends TestBase {

  implicit val ordering = KeyOrder.default

  import swaydb.core.map.serializer.RangeValueSerializers._

  "SegmentMerge.merge for Ranges when old Range's rangeValue is Remove & Ranges fromKeys are equal and new Range's to key is <= old Range's toKey" should {
    "old Range's rangeValue is Remove & new Range's range value is Put" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Put(1), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      )
      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove, new Range's range value is Put, new Range's fromValue is Put" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Remove, 0.1, None))
      )

      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, value = 1, 0.1, None), isLastLevel = true)
    }

    "old Range's rangeValue is Remove, new Range's range value is Put, new Range's fromValue is Remove" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Remove), Value.Remove, 0.1, None))
      )
      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      )
      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Put" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(10)), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(10)), Value.Remove, 0.1, None))
      )

      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(10)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 10, 0.1, None), isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Remove" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Remove), Value.Remove, 0.1, None))
      )

      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)

    }

    "1 range two slices" in {
      //1 - 10, 10 - 20
      //1      -     20
      val newKeyValues = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put("ten"), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Remove), Value.Put("twenty"), 0.1, None)
      )
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))

      val expected =
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Remove), Value.Remove, 0.1, None)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 1), isLastLevel = true)
    }

    "input Range overlaps existing 2 Ranges" in {
      //1  -     5
      //1 - 4, 4   -   15
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 5, Some(Value.Put(1)), Value.Put(100), 0.1, None))

      val oldKeyValues = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 4, Some(Value.Put("override")), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](4, 15, Some(Value.Put("override")), Value.Remove, 0.1, None)
      )

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 4, Some(Value.Put(1)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](4, 15, Some(Value.Put(100)), Value.Remove, 0.1, None)
        ).updateStats
      )

      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 4, Some(Value.Put(1)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](4, 5, Some(Value.Put(100)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](5, 15, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice(Transient.Put(1, 1, 0.1, None), Transient.Put(4, 100, 0.1, None)).updateStats, isLastLevel = true)
    }

    "mid range split" in {
      //      10   -   20
      //4   -   15,  19   -   30
      val newKeyValues =
      Slice(
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Remove), Value.Put(200), 0.1, None)
      )

      val oldKeyValues = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](4, 15, Some(Value.Put(4)), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](19, 30, None, Value.Remove, 0.1, None)
      )

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](4, 10, Some(Value.Put(4)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 15, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](15, 19, None, Value.Put(200), 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](19, 30, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](4, 10, Some(Value.Put(4)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 15, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](15, 19, None, Value.Put(200), 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](19, 20, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](20, 30, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(4, 4, 0.1, None), isLastLevel = true)

    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Remove for multiple Ranges" in {
      //1  -     5, 10   -   20
      //1 - 4, 4   -   15,  19 - 30
      val newKeyValues =
      Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 5, Some(Value.Put(1)), Value.Put(100), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Remove), Value.Put(200), 0.1, None)
      )
      val oldKeyValues = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 4, Some(Value.Put("override")), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](4, 15, Some(Value.Put("override")), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](19, 30, None, Value.Remove, 0.1, None)
      )

      assertMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 4, Some(Value.Put(1)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](4, 10, Some(Value.Put(100)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 15, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](15, 19, None, Value.Put(200), 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](19, 30, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      assertSkipListMerge(
        newKeyValues,
        oldKeyValues,
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 4, Some(Value.Put(1)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](4, 5, Some(Value.Put(100)), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](5, 10, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 15, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](15, 19, None, Value.Put(200), 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](19, 20, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](20, 30, None, Value.Remove, 0.1, None)
        ).updateStats
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice(Transient.Put(1, 1, 0.1, None), Transient.Put(4, 100, 0.1, None)).updateStats, isLastLevel = true)
    }
  }

  "SegmentMerge.merge for Ranges when old Range's rangeValue is Remove & Ranges fromKeys are equal & new Range's toKey is greater" should {
    "old Range's rangeValue is Remove & new Range's range value is Put" in {
      //1    -       21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, None, Value.Put("updated"), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove, new Range's range value is Put, new Range's fromValue is Put" in {
      //1    -       21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Put(1)), Value.Put("updated"), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, value = 1, 0.1, None), isLastLevel = true)
    }

    "old Range's rangeValue is Remove, new Range's range value is Put, new Range's fromValue is Remove" in {
      //1    -       21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Put("updated"), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Remove), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Put("updated"), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove" in {
      //1    -       21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, None, Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Remove, 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, Transient.Range[Value.Fixed, Value.Fixed](1, 21, None, Value.Remove, 0.1, None))

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Put" in {
      //1    -       21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Put(1)), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Remove, 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Put(1)), Value.Remove, 0.1, None))

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, value = 1, 0.1, None), isLastLevel = true)
    }

    "old Range's rangeValue is Remove & new Range's range value is Remove and old Ranges from Value is Remove" in {
      //1    -       21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Remove, 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Remove), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](20, 21, None, Value.Remove, 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Remove, 0.1, None))

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }
  }

}
