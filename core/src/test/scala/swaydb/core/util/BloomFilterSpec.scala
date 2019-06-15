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

package swaydb.core.util

import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data.{Transient, Value}
import swaydb.core.{TestBase, TestData, TestTimer}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class BloomFilterSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  "toBytes & toSlice" should {
    "write bloom filter to bytes" in {

      val bloomFilter =
        BloomFilter(
          numberOfKeys = 10,
          falsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = TestData.enableRangeFilter
        )

      (1 to 10) foreach (bloomFilter.add(_))
      (1 to 10) foreach (key => bloomFilter.mightContain(key) shouldBe true)
      (11 to 20) foreach (key => bloomFilter.mightContain(key) shouldBe false)

      val readBloomFilter = BloomFilter(bloomFilter.toBloomFilterSlice, bloomFilter.toRangeFilterSlice).get
      (1 to 10) foreach (key => readBloomFilter.mightContain(key) shouldBe true)
      (11 to 20) foreach (key => readBloomFilter.mightContain(key) shouldBe false)

      println(bloomFilter.numberOfBits)
      println(bloomFilter.endOffset)
    }
  }

  "optimalSegmentBloomFilterByteSize" should {
    "return empty if false positive rate is 0.0 or number of keys is 0" in {
      BloomFilter.optimalSegmentBloomFilterByteSize(1000, 0.0) shouldBe BloomFilter.minimumSize
      BloomFilter.optimalSegmentBloomFilterByteSize(0, 0.001) shouldBe BloomFilter.minimumSize
    }

    "return the number of bytes required to store the Bloom filter" in {
      (1 to 1000) foreach {
        i =>
          val numberOfItems = i * 10
          val falsePositiveRate = 0.0 + (0 + "." + i.toString).toDouble
          val bloomFilter =
            BloomFilter(
              numberOfKeys = numberOfItems,
              falsePositiveRate = falsePositiveRate,
              enableRangeFilter = TestData.enableRangeFilter
            )
          bloomFilter.toBloomFilterSlice.size should be <= BloomFilter.optimalSegmentBloomFilterByteSize(numberOfItems, falsePositiveRate)
      }
    }
  }

  "init" should {
    "not initialise if keyValues are empty" in {
      BloomFilter.init(
        keyValues = Slice.empty,
        falsePositiveRate = TestData.falsePositiveRate,
        enableRangeFilter = TestData.enableRangeFilter
      ) shouldBe empty
    }

    "not initialise if false positive range is 0.0 are empty" in {
      BloomFilter.init(
        keyValues = randomizedKeyValues(),
        falsePositiveRate = 0.0,
        enableRangeFilter = true
      ) shouldBe empty
    }

    "not initialise bloomFilter if it contain removeRange" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(None, time.next))),
          falsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = false
        ) shouldBe empty

        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(Some(randomDeadline()), time.next))),
          falsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = false
        ) shouldBe empty
      }
    }

    "not initialise bloomFilter if it contains a range function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //range functions can also contain Remove so BloomFilter should not be created
        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Function(Slice.emptyBytes, time.next))),
          falsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = false
        ) shouldBe empty
      }
    }

    "not initialise bloomFilter if it contain pendingApply with remove or function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //range functions can also contain Remove so BloomFilter should not be created
        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Remove(randomDeadlineOption(), time.next))))),
          falsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = false
        ) shouldBe empty

        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Function(randomFunctionId(), time.next))))),
          falsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = false
        ) shouldBe empty
      }
    }

    "initialise bloomFilter if it does not contain pendingApply with remove or function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //pending apply should allow to create bloomFilter if it does not have remove or function.
        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Update(randomStringOption, randomDeadlineOption(), time.next))))),
          falsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = TestData.enableRangeFilter
        ) shouldBe defined
      }
    }

    "initialise bloomFilter when from value is remove but range value is not" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random
        //fromValue is remove but it's not a remove range.
        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, Some(Value.Remove(None, time.next)), Value.update(100))),
          falsePositiveRate = TestData.falsePositiveRate,
          enableRangeFilter = TestData.enableRangeFilter
        ) shouldBe defined
      }
    }
  }

  "bloomFilter error check" in {
    def assert(data: Seq[String],
               filter: BloomFilter,
               previousFilter: Option[BloomFilter]) = {
      val positives =
        data.par collect {
          case data if !filter.mightContain(data) =>
            data
        }

      val falsePositives =
        data.par collect {
          case data if filter.mightContain(Random.alphanumeric.take(2000).mkString.getBytes()) =>
            data
        }

      println(s"missed out of ${data.size}: " + positives.size)
      println(s"errors out of ${data.size}: " + falsePositives.size)
      println(s"Optimal byte size: " + filter.numberOfBits)
      println(s"Actual byte size: " + filter.toBloomFilterSlice.size)
      println

      positives.size shouldBe 0
      falsePositives.size should be < 200

      if (previousFilter.isEmpty)
        filter.toBloomFilterSlice.underlyingArraySize shouldBe (filter.numberOfBits + filter.startOffset)

      previousFilter map {
        previousFilter =>
          filter.endOffset shouldBe previousFilter.endOffset
          filter.numberOfBits shouldBe previousFilter.numberOfBits
          filter.numberOfHashes shouldBe previousFilter.numberOfHashes
          filter.startOffset shouldBe previousFilter.startOffset
          filter.toBloomFilterSlice.size shouldBe previousFilter.toBloomFilterSlice.size
          filter.toBloomFilterSlice.underlyingArraySize shouldBe filter.toBloomFilterSlice.size
      }
    }

    val filter =
      BloomFilter(
        numberOfKeys = 10000,
        falsePositiveRate = TestData.falsePositiveRate,
        enableRangeFilter = TestData.enableRangeFilter
      )
    val data: Seq[String] =
      (1 to 10000) map {
        _ =>
          val string = Random.alphanumeric.take(2000).mkString
          filter.add(string.getBytes())
          string
      }

    assert(data, filter, None)
    //re-create bloomFilter and read again.
    val filter2 = BloomFilter(filter.toBloomFilterSlice.unslice(), filter.toRangeFilterSlice.unslice()).get
    assert(data, filter2, Some(filter))
    //re-create bloomFilter from created filter.
    val filter3 = BloomFilter(filter2.toBloomFilterSlice.unslice(), filter2.toRangeFilterSlice.unslice()).get
    assert(data, filter3, Some(filter2))
  }

  "range filter" in {
    Seq(Slice.writeIntUnsigned(_), Slice.writeInt(_)) foreach {
      serialiser =>
        implicit def toBytes(int: Int): Slice[Byte] = serialiser(int)

        val bloom1 =
          BloomFilter(
            numberOfKeys = 7,
            falsePositiveRate = TestData.falsePositiveRate,
            enableRangeFilter = true
          )

        bloom1.add(1)
        bloom1.add(2)
        bloom1.add(10, 20)
        bloom1.add(20, 30)
        bloom1.add(31)
        bloom1.add(32, 40)
        bloom1.add(40, 50)

        def assert(bloom: BloomFilter) = {
          bloom.mightContain(1) shouldBe true
          bloom.mightContain(2) shouldBe true
          (10 to 29) foreach {
            i =>
              bloom.mightContain(i) shouldBe true
          }
          bloom.mightContain(30) shouldBe false
          bloom.mightContain(31) shouldBe true
          (32 to 49) foreach {
            i =>
              bloom.mightContain(i) shouldBe true
          }
          bloom.mightContain(50) shouldBe false

          bloom.endOffset shouldBe bloom1.endOffset
          bloom.hasRanges shouldBe bloom1.hasRanges
          bloom.numberOfHashes shouldBe bloom1.numberOfHashes
          bloom.numberOfBits shouldBe bloom1.numberOfBits
          bloom.startOffset shouldBe bloom1.startOffset
        }

        assert(bloom1)

        val bloom2 = BloomFilter(bloom1.toBloomFilterSlice.unslice(), bloom1.toRangeFilterSlice.unslice()).get
        assert(bloom2)

        val bloom3 = BloomFilter(bloom2.toBloomFilterSlice.unslice(), bloom2.toRangeFilterSlice.unslice()).get
        assert(bloom3)
    }
  }
}
