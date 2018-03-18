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
import swaydb.core.data.Transient.Remove
import swaydb.core.data.{KeyValue, Transient}
import swaydb.core.segment.SegmentMerge
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MergeFixedIntoFixedSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  import ordering._

  "SegmentMerge.merge when merging Fixed key-values" should {

    "merge non-overlapping Put key-values in-order" in {
      val newKeyValues = Slice(Transient.Put(1, "1 value"))
      val oldKeyValues = Slice(Transient.Put(2, "2 value"))
      val expected = Slice(Transient.Put(1, "1 value"), Transient.Put(2, "2 value")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //reversed should return the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertRangeSplitter(oldKeyValues, newKeyValues, expected)

      //last Level check
      assertMerge(newKeyValues, oldKeyValues, expected, isLastLevel = true)
      assertMerge(oldKeyValues, newKeyValues, expected, isLastLevel = true)
    }

    "merge non-overlapping Remove key-values in-order" in {
      val newKeyValues = Slice(Transient.Remove(1))
      val oldKeyValues = Slice(Transient.Remove(2))
      val expected = Slice(Transient.Remove(1), Transient.Remove(2)).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //reversed should return the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertRangeSplitter(oldKeyValues, newKeyValues, expected)

      //last Level check
      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
      assertMerge(oldKeyValues, newKeyValues, Slice.empty, isLastLevel = true)
    }

    "update value for Put" in {
      val newKeyValues = Slice(Transient.Put(1, "new value"))
      val oldKeyValues = Slice(Transient.Put(1, "old value"))
      val expected = Slice(Transient.Put(1, "new value"))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //last Level check
      assertMerge(newKeyValues, oldKeyValues, expected, isLastLevel = true)
    }

    "update value for Put when there are multiple key-values" in {
      val newKeyValues = Slice(Transient.Put(3, "new value"))
      val oldKeyValues = Slice(Transient.Put(1, "old value 1"), Transient.Remove(2), Transient.Put(3, "old value 3")).updateStats
      val expected = Slice(Transient.Put(1, "old value 1"), Transient.Remove(2), Transient.Put(3, "new value")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      //last Level check does not contain removes.
      assertMerge(newKeyValues, oldKeyValues, expected.filterNot(_.isInstanceOf[Transient.Remove]).updateStats, isLastLevel = true)
    }

    "replace Put with Remove" in {
      val newKeyValues = Slice(Transient.Remove(2))
      val oldKeyValues = Slice(Transient.Put(1, "old value 1"), Transient.Put(2, "old value 2"), Transient.Put(3, "old value 3")).updateStats

      val expected = Slice(Transient.Put(1, "old value 1"), Transient.Remove(2), Transient.Put(3, "old value 3")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)

      assertMerge(newKeyValues, oldKeyValues, expected.filterNot(_.isInstanceOf[Transient.Remove]).updateStats, isLastLevel = true)
    }

    "replace Remove with Put" in {
      val newKeyValues = Slice(Transient.Put(2, "old value 2"))
      val oldKeyValues = Slice(Transient.Put(1, "old value 1"), Transient.Remove(2), Transient.Put(3, "old value 3")).updateStats

      val expected = Slice(Transient.Put(1, "old value 1"), Transient.Put(2, "old value 2"), Transient.Put(3, "old value 3")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertRangeSplitter(newKeyValues, oldKeyValues, expected)
    }

    "discard Remove if the level is the last Level" in {
      val newKeyValues = Slice(Transient.Remove(2), Transient.Remove(4), Transient.Remove(5)).updateStats
      val oldKeyValues = Slice(Transient.Put(1, "old value 1"), Transient.Put(2, "old value 2"), Transient.Put(3, "old value 3")).updateStats

      val expected = Slice(Transient.Put(1, "old value 1"), Transient.Put(3, "old value 3")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected, isLastLevel = true)
    }

    "returns empty if all the key-values were removed and it's the last level" in {
      val newKeyValues = Slice(Transient.Remove(1), Transient.Remove(2), Transient.Remove(3)).updateStats
      val oldKeyValues = Slice(Transient.Put(1, "old value 1"), Transient.Put(2, "old value 2"), Transient.Put(3, "old value 3")).updateStats

      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
      assertRangeSplitter(newKeyValues, oldKeyValues, newKeyValues)
    }

    "split KeyValues to equal chunks" in {
      val oldKeyValues: Slice[KeyValue.WriteOnly] = Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3), Transient.Put(4, 4)).updateStats
      val newKeyValues: Slice[KeyValue.WriteOnly] = Slice(Transient.Put(1, 22), Transient.Put(2, 22), Transient.Put(3, 22), Transient.Put(4, 22)).updateStats

      def assert(segments: Array[Iterable[KeyValue.WriteOnly]]) = {
        segments.length shouldBe 4

        segments(0).size shouldBe 1
        segments(0).head shouldBe newKeyValues(0)

        segments(1).size shouldBe 1
        segments(1).head.key equiv newKeyValues(1).key

        segments(2).size shouldBe 1
        segments(2).head.key equiv newKeyValues(2).key

        segments(3).size shouldBe 1
        segments(3).head.key equiv newKeyValues(3).key
      }

      assert(SegmentMerge.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.byte, isLastLevel = false, forInMemory = false, bloomFilterFalsePositiveRate = 0.1).assertGet.toArray)
      assert(SegmentMerge.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.byte, isLastLevel = false, forInMemory = true, bloomFilterFalsePositiveRate = 0.1).assertGet.toArray)
    }

    "remove Deleted key-values if it's the last Level" in {

      val oldKeyValues: Slice[KeyValue.WriteOnly] = Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3), Transient.Put(4, 4)).updateStats
      val newKeyValues: Slice[KeyValue.WriteOnly] = Slice(Remove(0), Transient.Put(1, 11), Remove(2), Transient.Put(3, 33), Remove(4), Remove(5)).updateStats

      def assert(segments: Iterable[Array[KeyValue.WriteOnly]]) = {
        segments should have size 1

        val mergedKeyValues = segments.head

        mergedKeyValues should have size 2

        mergedKeyValues.head.key shouldBe (1: Slice[Byte])
        mergedKeyValues.head.getOrFetchValue.assertGet shouldBe (11: Slice[Byte])

        mergedKeyValues(1).key shouldBe (3: Slice[Byte])
        mergedKeyValues(1).getOrFetchValue.assertGet shouldBe (33: Slice[Byte])
      }

      assert(SegmentMerge.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.mb, isLastLevel = true, forInMemory = false, bloomFilterFalsePositiveRate = 0.1).assertGet.map(_.toArray))
      assert(SegmentMerge.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.mb, isLastLevel = true, forInMemory = true, bloomFilterFalsePositiveRate = 0.1).assertGet.map(_.toArray))
    }
  }
}
