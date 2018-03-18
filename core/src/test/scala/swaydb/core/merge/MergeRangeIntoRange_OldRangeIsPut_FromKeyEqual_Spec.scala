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
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MergeRangeIntoRange_OldRangeIsPut_FromKeyEqual_Spec extends TestBase {

  implicit val ordering = KeyOrder.default

  import swaydb.core.map.serializer.RangeValueSerializers._

  "SegmentMerge.merge for Ranges when ranges do not overlap" should {
    "add new Range to the left if it's smaller then the old's range fromKey and new Range's toKey overlaps old Range's fromKey and new Ranges's fromValue is Remove" in {
      //1 - 10
      //    10 - 20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Put(1), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(10), 0.1, None))

      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Put(1), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "add new Range to the left if it's smaller then the old's range fromKey and new Range's toKey overlaps old Range's fromKey and new Ranges's fromValue is Put" in {
      //1 - 10
      //    10 - 20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put(1), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(10), 0.1, None))

      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put(1), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 1), isLastLevel = true)
    }

    "add new Range to the left if it's smaller then the old's range's fromKey and new Range's toKey does not overlaps old Range's fromKey and new Range's fromValue is Remove" in {
      //1 - 10
      //      11 - 20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Put(1), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](11, 20, None, Value.Put(10), 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Put(1), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](11, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "add new Range to the left if it's smaller then the old's range's fromKey and new Range's toKey does not overlaps old Range's fromKey and new Range's fromValue is Put" in {
      //1 - 10
      //      11 - 20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put("one")), Value.Put(1), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](11, 20, None, Value.Put(10), 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put("one")), Value.Put(1), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](11, 20, None, Value.Put(10), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, "one"), isLastLevel = true)
    }
  }

  "SegmentMerge.merge for Ranges when old Range's rangeValue is Put & Ranges fromKeys are equal and new Range's to key is <= old Range's toKey" should {
    "new Range's range value is Put" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))

      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Put(10), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Put and new Range's fromValue set to Put" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))

      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put(10), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice(Transient.Put(1, 1, 0.1, None)), isLastLevel = true)
    }

    "new Range's range value is Put and fromValue is Put and new Range's fromValue is None" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Put(1), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put("one")), Value.Put(20), 0.1, None))

      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put(1), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice(Transient.Put(1, 1, 0.1, None)), isLastLevel = true)
    }

    "new Range's range value is Put and new Range's fromValue set to Remove" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Put(10), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))

      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove and fromValue of old Range is set to Put" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(20), 0.1, None))

      assertMerge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
        ).updateStats
      )

      assertRangeSplitter(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove, 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
        ).updateStats
      )
      //      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove and new Range's fromValue set to Put" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice(Transient.Put(1, 1, 0.1, None)), isLastLevel = true)
    }

    "new Range's range value is Remove and new Range's fromValue set to Remove" in {
      //1 - 10
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))
      val expected = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Remove, 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, None, Value.Put(20), 0.1, None)
      ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }
  }

  "SegmentMerge.merge for Ranges when both old and new Range's from and to key match" should {
    "new Range's range value is Remove and old Range's fromValue set is Put" in {
      //1    -    20
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Remove), Value.Put(100), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(20), 0.1, None))
      val expected = Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Remove), Value.Put(100), 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Put and old Range's fromValue set is Remove" in {
      //1    -    20
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(100), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Remove), Value.Put(20), 0.1, None))
      val expected = Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(100), 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 1), isLastLevel = true)
    }

    "new Range's range value is None and old Range's fromValue set is Put" in {
      //1    -    20
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(100), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(20), 0.1, None))
      val expected = Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(100)), Value.Put(100), 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 100), isLastLevel = true)
    }

    "new Range's range value is Put and old Range's fromValue set is None" in {
      //1    -    20
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(100), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))
      val expected = Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(100), 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 1), isLastLevel = true)
    }

    "1 range two slices" in {
      //1 - 10, 10 - 20
      //1      -     20
      val newKeyValues = Slice(
        Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put("ten"), 0.1, None),
        Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Remove), Value.Put("twenty"), 0.1, None)
      )
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Remove), Value.Put(20), 0.1, None))

      val expected =
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put("ten"), 0.1, None),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Remove), Value.Put("twenty"), 0.1, None)
        ).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Transient.Put(1, 1), isLastLevel = true)
    }

  }

  "SegmentMerge.merge for Ranges when old Range's rangeValue is Put & Ranges fromKeys are equal and new Range's to key is > old Range's toKey" should {
    "new Range's range value is Put" in {
      //1     -      21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, None, Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))

      val expected = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, None, Value.Put(10), 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Put and new Range's fromValue set to Put" in {
      //1     -      21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Put(1)), Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))
      val expected = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Put(1)), Value.Put(10), 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice(Transient.Put(1, 1, 0.1, None)), isLastLevel = true)
    }

    "new Range's range value is Put and old Range's fromValue set to Put" in {
      //1     -      21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, None, Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(20), 0.1, None))
      val expected = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Put(10)), Value.Put(10), 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice(Transient.Put(1, 10, 0.1, None)), isLastLevel = true)
    }

    "new Range's range value is Put and new Range's fromValue set to Remove" in {
      //1     -      21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Put(10), 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))

      val expected = Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Put(10), 0.1, None)

      assertMerge(newKeyValues, oldKeyValues, Slice(expected))
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove" in {
      //1     -      21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, None, Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))
      val expected = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, None, Value.Remove, 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove and new Range's fromValue set to Put" in {
      //1     -      21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Put(1)), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))
      val expected = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Put(1)), Value.Remove, 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice(Transient.Put(1, 1, 0.1, None)), isLastLevel = true)
    }

    "new Range's range value is Remove and new Range's fromValue set to Remove" in {
      //1     -      21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, None, Value.Put(20), 0.1, None))
      val expected = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Remove, 0.1, None))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }

    "new Range's range value is Remove and old Range's fromValue set to Put" in {
      //1     -      21
      //1    -    20
      val newKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Remove, 0.1, None))
      val oldKeyValues = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 20, Some(Value.Put(1)), Value.Put(20), 0.1, None))

      assertMerge(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Remove, 0.1, None))
      )

      assertRangeSplitter(
        newKeyValues = newKeyValues,
        oldKeyValues = oldKeyValues,
        expected = Transient.Range[Value.Fixed, Value.Fixed](1, 21, Some(Value.Remove), Value.Remove, 0.1, None)
      )

      //is last Level
      assertMerge(newKeyValues, oldKeyValues, expected = Slice.empty, isLastLevel = true)
    }
  }

}
