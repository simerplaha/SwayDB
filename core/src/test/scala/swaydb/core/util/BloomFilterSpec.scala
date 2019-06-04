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
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class BloomFilterSpec extends TestBase {

  "toBytes & toSlice" should {
    "write bloom filter to bytes" in {

      val bloomFilter = BloomFilter(10, 0.01)
      (1 to 10) foreach (bloomFilter.add(_))
      (1 to 10) foreach (key => bloomFilter.mightContain(key) shouldBe true)
      (11 to 20) foreach (key => bloomFilter.mightContain(key) shouldBe false)

      val readBloomFilter = BloomFilter(bloomFilter.toSlice).get
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
          val bloomFilter = BloomFilter(numberOfItems, falsePositiveRate)
          bloomFilter.toSlice.size shouldBe BloomFilter.byteSize(numberOfItems, falsePositiveRate)
      }
    }
  }

  "init" should {
    "not initialise if keyValues are empty" in {
      BloomFilter.init(
        keyValues = Slice.empty,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate
      ) shouldBe empty
    }

    "not initialise bloomFilter if it contain removeRange" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(None, time.next))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty

        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(Some(randomDeadline()), time.next))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty
      }
    }

    "not initialise bloomFilter if it contain function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //range functions can also contain Remove so BloomFilter should not be created
        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Function(Slice.emptyBytes, time.next))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty
      }
    }

    "not initialise bloomFilter if it contain pendingApply with remove or function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //range functions can also contain Remove so BloomFilter should not be created
        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Remove(randomDeadlineOption(), time.next))))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty

        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Function(randomFunctionId(), time.next))))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe empty
      }
    }

    "initialise bloomFilter if it does not contain pendingApply with remove or function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //pending apply should allow to create bloomFilter if it does not have remove or function.
        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Update(randomStringOption, randomDeadlineOption(), time.next))))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe defined
      }
    }

    "initialise bloomFilter when from value is remove but range value is not" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random
        //fromValue is remove but it's not a remove range.
        BloomFilter.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, Some(Value.Remove(None, time.next)), Value.update(100))),
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ) shouldBe defined
      }
    }
  }

  "bloomFilter error check" in {
    def assert(data: Seq[String], filter: BloomFilter) = {
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

      positives.size shouldBe 0
      falsePositives.size should be < 200
    }

    val filter = BloomFilter(10000, 0.01)
    val data: Seq[String] =
      (1 to 10000) map {
        _ =>
          val string = Random.alphanumeric.take(2000).mkString
          filter.add(string.getBytes())
          string
      }

    assert(data, filter)
    //re-create bloomFilter and read again.
    assert(data, BloomFilter(filter.toSlice).get)
  }
}
