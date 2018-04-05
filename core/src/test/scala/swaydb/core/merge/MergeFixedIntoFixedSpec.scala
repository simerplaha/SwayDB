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
import swaydb.core.data.{KeyValue, Memory, Transient}
import swaydb.core.segment.SegmentMerge
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MergeFixedIntoFixedSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  "SegmentMerge.merge when merging Fixed key-values" should {

    "merge non-overlapping Put key-values in-order" in {
      val newKeyValues = Slice(Memory.Put(1, "1 value"))
      val oldKeyValues = Slice(Memory.Put(2, "2 value"))

      val expected = Slice(Transient.Put(1, "1 value"), Transient.Put(2, "2 value")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed should return the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last Level check
      assertMerge(newKeyValues, oldKeyValues, expected, isLastLevel = true)
      assertMerge(oldKeyValues, newKeyValues, expected, isLastLevel = true)
    }

    "merge non-overlapping Remove key-values in-order" in {
      val newKeyValues = Slice(Memory.Remove(1))
      val oldKeyValues = Slice(Memory.Remove(2))

      val expected = Slice(Transient.Remove(1), Transient.Remove(2)).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed should return the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last Level check
      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
      assertMerge(oldKeyValues, newKeyValues, Slice.empty, isLastLevel = true)
    }

    "merge non-overlapping Put & Remove key-values in-order" in {
      val newKeyValues = Slice(Memory.Put(1, "1 value"))
      val oldKeyValues = Slice(Memory.Remove(2))

      val expected = Slice(Transient.Put(1, "1 value"), Transient.Remove(2)).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //reversed should return the same result
      assertMerge(oldKeyValues, newKeyValues, expected)
      assertSkipListMerge(oldKeyValues, newKeyValues, expected)

      //last Level check
      assertMerge(newKeyValues, oldKeyValues, Transient.Put(1, "1 value"), isLastLevel = true)
      assertMerge(oldKeyValues, newKeyValues, Transient.Put(1, "1 value"), isLastLevel = true)
    }

    "update value for Put" in {
      val newKeyValues = Slice(Memory.Put(1, "new value"))
      val oldKeyValues = Slice(Memory.Put(1, "old value"))
      val expected = Slice(Transient.Put(1, "new value"))

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last Level check
      assertMerge(newKeyValues, oldKeyValues, expected, isLastLevel = true)

      //reversed
      assertMerge(oldKeyValues, newKeyValues, Transient.Put(1, "old value"), isLastLevel = false)
      assertSkipListMerge(oldKeyValues, newKeyValues, Transient.Put(1, "old value"))
      //last Level check
      assertMerge(oldKeyValues, newKeyValues, Transient.Put(1, "old value"), isLastLevel = true)
    }

    "update value for Put when there are multiple key-values" in {
      val newKeyValues = Slice(Memory.Put(3, "new value"))
      val oldKeyValues = Slice(Memory.Put(1, "old value 1"), Memory.Remove(2), Memory.Put(3, "old value 3"))
      val expected = Slice(Transient.Put(1, "old value 1"), Transient.Remove(2), Transient.Put(3, "new value")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      //last Level check does not contain removes.
      assertMerge(newKeyValues, oldKeyValues, expected.filterNot(_.isInstanceOf[Transient.Remove]).updateStats, isLastLevel = true)
    }

    "replace Put with Remove" in {
      val newKeyValues = Slice(Memory.Remove(2))
      val oldKeyValues = Slice(Memory.Put(1, "old value 1"), Memory.Put(2, "old value 2"), Memory.Put(3, "old value 3"))

      val expected = Slice(Transient.Put(1, "old value 1"), Transient.Remove(2), Transient.Put(3, "old value 3")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)

      assertMerge(newKeyValues, oldKeyValues, expected.filterNot(_.isInstanceOf[Transient.Remove]).updateStats, isLastLevel = true)
    }

    "replace Remove with Put" in {
      val newKeyValues = Slice(Memory.Put(2, "old value 2"))
      val oldKeyValues = Slice(Memory.Put(1, "old value 1"), Memory.Remove(2), Memory.Put(3, "old value 3"))

      val expected = Slice(Transient.Put(1, "old value 1"), Transient.Put(2, "old value 2"), Transient.Put(3, "old value 3")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected)
      assertSkipListMerge(newKeyValues, oldKeyValues, expected)
    }

    "discard Remove if the level is the last Level" in {
      val newKeyValues = Slice(Memory.Remove(2), Memory.Remove(4), Memory.Remove(5))
      val oldKeyValues = Slice(Memory.Put(1, "old value 1"), Memory.Put(2, "old value 2"), Memory.Put(3, "old value 3"))

      val expected = Slice(Transient.Put(1, "old value 1"), Transient.Put(3, "old value 3")).updateStats

      assertMerge(newKeyValues, oldKeyValues, expected, isLastLevel = true)
    }

    "return empty if all the key-values were removed and it's the last level" in {
      val newKeyValues = Slice(Memory.Remove(1), Memory.Remove(2), Memory.Remove(3))
      val oldKeyValues = Slice(Memory.Put(1, "old value 1"), Memory.Put(2, "old value 2"), Memory.Put(3, "old value 3"))

      assertMerge(newKeyValues, oldKeyValues, Slice.empty, isLastLevel = true)
      assertSkipListMerge(newKeyValues, oldKeyValues, newKeyValues)
    }


    "remove Deleted key-values if it's the last Level" in {
      val oldKeyValues: Slice[Memory] = Slice(Memory.Put(1, 1), Memory.Put(2, 2), Memory.Put(3, 3), Memory.Put(4, 4))
      val newKeyValues: Slice[Memory] = Slice(Memory.Remove(0), Memory.Put(1, 11), Memory.Remove(2), Memory.Put(3, 33), Memory.Remove(4), Memory.Remove(5))

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
