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
      val three = Transient.put(key = 3, value = Some(3), falsePositiveRate = TestData.falsePositiveRate, previous = Some(two))
      val four = Transient.remove(4, TestData.falsePositiveRate, Some(three))
      val five = Transient.put(key = 5, value = Some(5), falsePositiveRate = TestData.falsePositiveRate, previous = Some(four))

      five.reverseIterator.toList should contain inOrderOnly(five, four, three, two, one)
    }
  }

  "assert stats" in {
    runThis(100.times) {
      randomTransientKeyValue(
        key = 1,
        toKey = Some(2),
        value = Option.empty[Slice[Byte]],
        previous = None,
        falsePositiveRate = 0.01,
        enableBinarySearchIndex = true,
        buildFullBinarySearchIndex = true,
        resetPrefixCompressionEvery = 0,
        minimumNumberOfKeysForHashIndex = 5,
        hashIndexCompensation = _ => 0
      ) match {
        case keyValue: Transient.Remove =>
          keyValue.stats.valueSize shouldBe 0
          keyValue.stats.segmentHasRemoveRange shouldBe false
          keyValue.stats.segmentHasPut shouldBe false
          keyValue.stats.chainPosition shouldBe 1
          keyValue.stats.segmentTotalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesAccessIndexOffset shouldBe 0
          keyValue.stats.segmentUniqueKeysCount shouldBe 1

        case keyValue: Transient.Put =>
          keyValue.stats.valueSize shouldBe 0
          keyValue.stats.segmentHasRemoveRange shouldBe false
          keyValue.stats.segmentHasPut shouldBe true
          keyValue.stats.chainPosition shouldBe 1
          keyValue.stats.segmentTotalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesAccessIndexOffset shouldBe 0
          keyValue.stats.segmentUniqueKeysCount shouldBe 1

        case keyValue: Transient.Update =>
          keyValue.stats.valueSize shouldBe 0
          keyValue.stats.segmentHasRemoveRange shouldBe false
          keyValue.stats.segmentHasPut shouldBe false
          keyValue.stats.chainPosition shouldBe 1
          keyValue.stats.segmentTotalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesAccessIndexOffset shouldBe 0
          keyValue.stats.segmentUniqueKeysCount shouldBe 1

        case keyValue: Transient.Function =>
          keyValue.stats.valueSize should be > 0
          keyValue.stats.segmentHasRemoveRange shouldBe false
          keyValue.stats.segmentHasPut shouldBe false
          keyValue.stats.chainPosition shouldBe 1
          keyValue.stats.segmentTotalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesAccessIndexOffset shouldBe 0
          keyValue.stats.segmentUniqueKeysCount shouldBe 1

        case keyValue: Transient.PendingApply =>
          keyValue.stats.valueSize should be > 0
          keyValue.stats.segmentHasRemoveRange shouldBe false
          keyValue.stats.segmentHasPut shouldBe false
          keyValue.stats.chainPosition shouldBe 1
          keyValue.stats.segmentTotalNumberOfRanges shouldBe 0
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesAccessIndexOffset shouldBe 0
          keyValue.stats.segmentUniqueKeysCount shouldBe 1

        case keyValue: Transient.Range =>
          keyValue.stats.valueSize should be > 0
          keyValue.stats.segmentHasRemoveRange shouldBe keyValue.rangeValue.hasRemoveMayBe
          keyValue.stats.segmentHasPut shouldBe keyValue.fromValue.exists(_.isInstanceOf[Value.Put])
          keyValue.stats.chainPosition shouldBe 1
          keyValue.stats.segmentTotalNumberOfRanges shouldBe 1
          keyValue.stats.groupsCount shouldBe 0
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesAccessIndexOffset shouldBe 0
          keyValue.stats.segmentUniqueKeysCount shouldBe 1

        case keyValue: Transient.Group =>
          keyValue.stats.valueSize should be > 0
          keyValue.stats.segmentHasRemoveRange shouldBe keyValue.keyValues.exists(_.stats.segmentHasRemoveRange)
          keyValue.stats.segmentHasPut shouldBe keyValue.keyValues.exists(_.stats.segmentHasPut)
          keyValue.stats.chainPosition shouldBe 1
          keyValue.stats.segmentTotalNumberOfRanges shouldBe countRangesManually(keyValue.keyValues)
          keyValue.stats.groupsCount shouldBe 1
          keyValue.stats.thisKeyValueIndexOffset shouldBe 0
          keyValue.stats.thisKeyValuesAccessIndexOffset shouldBe 0
          keyValue.stats.segmentUniqueKeysCount shouldBe keyValue.keyValues.last.stats.segmentUniqueKeysCount
      }
    }
  }

  "set hashIndex size to 0 if minimum number of keys is not met" in {
    val keyValues =
      randomKeyValues(
        count = 10,
        startId = Some(0),
        addRandomRemoves = true,
        addRandomFunctions = true,
        addRandomRemoveDeadlines = true,
        addRandomUpdates = true,
        addRandomPendingApply = true,
        resetPrefixCompressionEvery = randomIntMax(20),
        minimumNumberOfKeysForHashIndex = 11
      )

    keyValues.last.stats.segmentHashIndexSize shouldBe 0
  }

  "calculate hashIndex size if minimum number of keys is met" in {
    runThis(10.times) {
      val keyValues =
        randomKeyValues(
          count = 10,
          startId = Some(0),
          addRandomRemoves = true,
          addRandomFunctions = true,
          addRandomRemoveDeadlines = true,
          addRandomUpdates = true,
          addRandomPendingApply = true,
          resetPrefixCompressionEvery = randomIntMax(20),
          minimumNumberOfKeysForHashIndex = randomIntMax(10)
        )

      keyValues.last.stats.segmentHashIndexSize should be > 0
    }
  }
}
