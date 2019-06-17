/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.data

import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.{TestBase, TestData, TestTimer}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class TransientSpec extends TestBase {

  val keyValueCount = 100

  implicit def testTimer: TestTimer = TestTimer.random

  "Transient" should {
    "be iterable" in {
      val one = Transient.remove(1)
      val two = Transient.remove(2, TestData.falsePositiveRate, Some(one))
      val three = Transient.put(key = 3, value = Some(3), falsePositiveRate = TestData.falsePositiveRate, previousMayBe = Some(two))
      val four = Transient.remove(4, TestData.falsePositiveRate, Some(three))
      val five = Transient.put(key = 5, value = Some(5), falsePositiveRate = TestData.falsePositiveRate, previousMayBe = Some(four))

      five.reverseIterator.toList should contain inOrderOnly(five, four, three, two, one)
    }
  }

  "assert stats" in {
    runThis(100.times) {
      randomTransientKeyValue(
        key = 1,
        toKey = Some(2),
        value = Option.empty[Slice[Byte]],
        previous = None
      ) match {
        case keyValue: Transient.Remove =>
          keyValue.stats.valueLength shouldBe 0
          keyValue.stats.hasRemoveRange shouldBe false
          keyValue.stats.hasPut shouldBe false
          keyValue.stats.position shouldBe 1
          keyValue.stats.totalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.rangeCommonPrefixesCount shouldBe empty
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset shouldBe 0
          keyValue.stats.totalBloomFiltersItemsCount shouldBe 1

        case keyValue: Transient.Put =>
          keyValue.stats.valueLength shouldBe 0
          keyValue.stats.hasRemoveRange shouldBe false
          keyValue.stats.hasPut shouldBe true
          keyValue.stats.position shouldBe 1
          keyValue.stats.totalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.rangeCommonPrefixesCount shouldBe empty
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset shouldBe 0
          keyValue.stats.totalBloomFiltersItemsCount shouldBe 1

        case keyValue: Transient.Update =>
          keyValue.stats.valueLength shouldBe 0
          keyValue.stats.hasRemoveRange shouldBe false
          keyValue.stats.hasPut shouldBe false
          keyValue.stats.position shouldBe 1
          keyValue.stats.totalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.rangeCommonPrefixesCount shouldBe empty
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset shouldBe 0
          keyValue.stats.totalBloomFiltersItemsCount shouldBe 1

        case keyValue: Transient.Function =>
          keyValue.stats.valueLength should be > 0
          keyValue.stats.hasRemoveRange shouldBe false
          keyValue.stats.hasPut shouldBe false
          keyValue.stats.position shouldBe 1
          keyValue.stats.totalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.rangeCommonPrefixesCount shouldBe empty
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset shouldBe 0
          keyValue.stats.totalBloomFiltersItemsCount shouldBe 1

        case keyValue: Transient.PendingApply =>
          keyValue.stats.valueLength should be > 0
          keyValue.stats.hasRemoveRange shouldBe false
          keyValue.stats.hasPut shouldBe false
          keyValue.stats.position shouldBe 1
          keyValue.stats.totalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.rangeCommonPrefixesCount shouldBe empty
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset shouldBe 0
          keyValue.stats.totalBloomFiltersItemsCount shouldBe 1

        case keyValue: Transient.Range =>
          keyValue.stats.valueLength should be > 0
          keyValue.stats.hasRemoveRange shouldBe keyValue.rangeValue.hasRemoveMayBe
          keyValue.stats.hasPut shouldBe keyValue.fromValue.exists(_.isInstanceOf[Value.Put])
          keyValue.stats.position shouldBe 1
          keyValue.stats.totalNumberOfRanges shouldBe 1
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.rangeCommonPrefixesCount shouldBe Stats.createRangeCommonPrefixesCount(3)
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset shouldBe 0
          keyValue.stats.totalBloomFiltersItemsCount shouldBe 2

        case keyValue: Transient.Group =>
          keyValue.stats.valueLength should be > 0
          keyValue.stats.hasRemoveRange shouldBe keyValue.keyValues.exists(_.stats.hasRemoveRange)
          keyValue.stats.hasPut shouldBe keyValue.keyValues.exists(_.stats.hasPut)
          keyValue.stats.position shouldBe 1
          keyValue.stats.totalNumberOfRanges shouldBe countRangesManually(keyValue.keyValues)
          keyValue.stats.groupsCount shouldBe 1
          keyValue.stats.rangeCommonPrefixesCount shouldBe keyValue.keyValues.last.stats.rangeCommonPrefixesCount
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset shouldBe 0
          keyValue.stats.totalBloomFiltersItemsCount shouldBe keyValue.keyValues.last.stats.totalBloomFiltersItemsCount
      }
    }
  }
}
