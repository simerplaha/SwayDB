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

package swaydb.core.segment.format.a.index

import swaydb.core.io.reader.Reader
import swaydb.core.{TestBase, TestData}
import swaydb.data.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class BloomFilterSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  "toBytes & toSlice" should {
    "write bloom filter to bytes" in {
      val filter =
        BloomFilter(
          numberOfKeys = 10,
          falsePositiveRate = TestData.falsePositiveRate
        )

      (1 to 10) foreach (BloomFilter.add(_, filter))
      val header = BloomFilter(0, Reader(filter.bytes)).get
      (1 to 10) foreach (key => BloomFilter.mightContain(key, header) shouldBe true)
      (11 to 20) foreach (key => BloomFilter.mightContain(key, header) shouldBe false)

      val readBloomFilter = BloomFilter(0, Reader(filter.bytes)).get
      (1 to 10) foreach (key => BloomFilter.mightContain(key, readBloomFilter) shouldBe true)
      (11 to 20) foreach (key => BloomFilter.mightContain(key, readBloomFilter) shouldBe false)

      println("numberOfBits: " + filter.numberOfBits)
      println("written: " + filter.written)
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
              falsePositiveRate = falsePositiveRate
            )
          bloomFilter.bytes.written should be <= BloomFilter.optimalSegmentBloomFilterByteSize(numberOfItems, falsePositiveRate)
      }
    }
  }

  "init" should {
    "not initialise if keyValues are empty" in {
      BloomFilter.init(
        numberOfKeys = 0,
        falsePositiveRate = TestData.falsePositiveRate
      ) shouldBe empty
    }

    "not initialise if false positive range is 0.0 are empty" in {
      BloomFilter.init(
        numberOfKeys = 100,
        falsePositiveRate = 0.0
      ) shouldBe empty
    }

    //todo - move these test to Segment

    //    "not initialise bloomFilter if it contain removeRange" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //
    //        BloomFilter.init(
    //          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(None, time.next))),
    //          falsePositiveRate = TestData.falsePositiveRate,
    //          enablePositionIndex = false
    //        ) shouldBe empty
    //
    //        BloomFilter.init(
    //          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(Some(randomDeadline()), time.next))),
    //          falsePositiveRate = TestData.falsePositiveRate,
    //          enablePositionIndex = false
    //        ) shouldBe empty
    //      }
    //    }
    //
    //    "not initialise bloomFilter if it contains a range function" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //
    //        //range functions can also contain Remove so BloomFilter should not be created
    //        BloomFilter.init(
    //          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Function(Slice.emptyBytes, time.next))),
    //          falsePositiveRate = TestData.falsePositiveRate,
    //          enablePositionIndex = false
    //        ) shouldBe empty
    //      }
    //    }
    //
    //    "not initialise bloomFilter if it contain pendingApply with remove or function" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //
    //        //range functions can also contain Remove so BloomFilter should not be created
    //        BloomFilter.init(
    //          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Remove(randomDeadlineOption(), time.next))))),
    //          falsePositiveRate = TestData.falsePositiveRate,
    //          enablePositionIndex = false
    //        ) shouldBe empty
    //
    //        BloomFilter.init(
    //          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Function(randomFunctionId(), time.next))))),
    //          falsePositiveRate = TestData.falsePositiveRate,
    //          enablePositionIndex = false
    //        ) shouldBe empty
    //      }
    //    }
    //
    //    "initialise bloomFilter if it does not contain pendingApply with remove or function" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //
    //        //pending apply should allow to create bloomFilter if it does not have remove or function.
    //        BloomFilter.init(
    //          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Update(randomStringOption, randomDeadlineOption(), time.next))))),
    //          falsePositiveRate = TestData.falsePositiveRate,
    //          enablePositionIndex
    //        ) shouldBe defined
    //      }
    //    }
    //
    //    "initialise bloomFilter when from value is remove but range value is not" in {
    //      runThisParallel(10.times) {
    //        implicit val time = TestTimer.random
    //        //fromValue is remove but it's not a remove range.
    //        BloomFilter.init(
    //          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, Some(Value.Remove(None, time.next)), Value.update(100))),
    //          falsePositiveRate = TestData.falsePositiveRate,
    //          enablePositionIndex
    //        ) shouldBe defined
    //      }
    //    }
  }

  "bloomFilter error check" in {
    def assert(data: Seq[String],
               filter: BloomFilter.Header,
               previousFilter: Option[BloomFilter.Header]) = {
      val positives =
        data.par collect {
          case data if !BloomFilter.mightContain(data, filter) =>
            data
        }

      val falsePositives =
        data.par collect {
          case data if BloomFilter.mightContain(Random.alphanumeric.take(2000).mkString.getBytes(), filter) =>
            data
        }

      println(s"errors out of ${data.size}: " + positives.size)
      println(s"falsePositives out of ${data.size}: " + falsePositives.size)
      println(s"Optimal byte size: " + filter.numberOfBits)
      println(s"Actual byte size: " + filter.reader.size.get)
      println

      positives.size shouldBe 0
      falsePositives.size should be < 200

      previousFilter map {
        previousFilter =>
          filter.numberOfBits shouldBe previousFilter.numberOfBits
          filter.probe shouldBe previousFilter.probe
          filter.startOffset shouldBe previousFilter.startOffset
      }
    }

    val filter =
      BloomFilter(
        numberOfKeys = 10000,
        falsePositiveRate = TestData.falsePositiveRate
      )

    val data: Seq[String] =
      (1 to 10000) map {
        _ =>
          val string = Random.alphanumeric.take(2000).mkString
          BloomFilter.add(string.getBytes(), filter)
          string
      }

    val header1 = BloomFilter(0, Reader(filter.bytes)).get
    assert(data, header1, None)

    //re-create bloomFilter and read again.
    val header2 = BloomFilter(0, Reader(filter.bytes)).get
    assert(data, header2, Some(header1))

    //re-create bloomFilter from created filter.
    val filter3 = BloomFilter(0, Reader(header2.reader.copy().readRemaining().get)).get
    assert(data, filter3, Some(header2))
  }
}
