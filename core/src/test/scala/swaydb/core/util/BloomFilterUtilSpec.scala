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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import bloomfilter.mutable.BloomFilter
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data.{Transient, Value}
import swaydb.core.util.BloomFilterUtil._
import swaydb.core.{TestBase, TestData, TestTimer}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class BloomFilterUtilSpec extends TestBase {

  "toBytes & toSlice" should {
    "write bloom filter to bytes" in {

      val bloomFilter = BloomFilter[Slice[Byte]](10, 0.01)
      (1 to 10) foreach (bloomFilter.add(_))
      (1 to 10) foreach (key => bloomFilter.mightContain(key) shouldBe true)
      (11 to 20) foreach (key => bloomFilter.mightContain(key) shouldBe false)

      val readBloomFilter = bloomFilter.toSlice.toBloomFilter
      (1 to 10) foreach (key => readBloomFilter.mightContain(key) shouldBe true)
      (11 to 20) foreach (key => readBloomFilter.mightContain(key) shouldBe false)
    }
  }

  "byteSize" should {
    "return the number of bytes required to store the Bloom filter" in {
      (1 to 1000) foreach {
        i =>
          val numberOfItems = i * 10
          val falsePositiveRate = 0.0 + (0 + "." + i.toString).toDouble
          val bloomFilter = BloomFilter[Slice[Byte]](numberOfItems, falsePositiveRate)
          bloomFilter.toBytes.length shouldBe BloomFilterUtil.byteSize(numberOfItems, falsePositiveRate)
      }
    }
  }

  "init" should {
    "not initialise if keyValues are empty" in {
      BloomFilterUtil.init(
        keyValues = Slice.empty,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate
      ) shouldBe empty
    }

    "not initialise bloomFilter if it contain removeRange" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        BloomFilterUtil.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(None, time.next))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty

        BloomFilterUtil.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(Some(randomDeadline()), time.next))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty
      }
    }

    "not initialise bloomFilter if it contain function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //range functions can also contain Remove so BloomFilter should not be created
        BloomFilterUtil.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Function(Slice.emptyBytes, time.next))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty
      }
    }

    "not initialise bloomFilter if it contain pendingApply with remove or function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //range functions can also contain Remove so BloomFilter should not be created
        BloomFilterUtil.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Remove(randomDeadlineOption(), time.next))))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty

        BloomFilterUtil.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Function(randomFunctionId(), time.next))))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty
      }
    }

    "initialise bloomFilter if it does not contain pendingApply with remove or function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //pending apply should allow to create bloomFilter if it does not have remove or function.
        BloomFilterUtil.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Update(randomStringOption, randomDeadlineOption(), time.next))))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe defined
      }
    }

    "initialise bloomFilter when from value is remove but range value is not" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random
        //fromValue is remove but it's not a remove range.
        BloomFilterUtil.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, Some(Value.Remove(None, time.next)), Value.update(100))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe defined
      }
    }
  }
}
