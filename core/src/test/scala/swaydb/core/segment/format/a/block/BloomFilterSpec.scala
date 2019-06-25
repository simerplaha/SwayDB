///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.segment.format.a.block
//
//import swaydb.core.CommonAssertions.{assertBloom, eitherOne}
//import swaydb.core.RunThis._
//import swaydb.core.TestData._
//import swaydb.core.data.Value.{FromValue, RangeValue}
//import swaydb.core.data.{Transient, Value}
//import swaydb.core.io.reader.Reader
//import swaydb.core.segment.format.a.SegmentWriter
//import swaydb.core.{TestBase, TestData, TestTimer}
//import swaydb.data.order.KeyOrder
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.util.Random
//
//class BloomFilterSpec extends TestBase {
//
//  implicit val keyOrder = KeyOrder.default
//
//  val keyValueCount = 1000
//
//  "toBytes & toSlice" should {
//    "write bloom filter to bytes" in {
//      val filter =
//        BloomFilter(
//          numberOfKeys = 10,
//          falsePositiveRate = TestData.falsePositiveRate
//        )
//
//      (1 to 10) foreach (BloomFilter.add(_, filter))
//      val bloom = BloomFilter.read(BloomFilter.Offset(0, filter.bytes.written), Reader(filter.bytes)).get
//      (1 to 10) foreach (key => BloomFilter.mightContain(key, Reader(filter.bytes), bloom).get shouldBe true)
//      (11 to 20) foreach (key => BloomFilter.mightContain(key, Reader(filter.bytes), bloom).get shouldBe false)
//
//      val readBloom = BloomFilter.read(BloomFilter.Offset(0, filter.bytes.written), Reader(filter.bytes)).get
//      (1 to 10) foreach (key => BloomFilter.mightContain(key, Reader(filter.bytes), readBloom).get shouldBe true)
//      (11 to 20) foreach (key => BloomFilter.mightContain(key, Reader(filter.bytes), readBloom).get shouldBe false)
//
//      println("numberOfBits: " + filter.numberOfBits)
//      println("written: " + filter.written)
//    }
//  }
//
//  "optimalSegmentBloomFilterByteSize" should {
//    "return empty if false positive rate is 0.0 or number of keys is 0" in {
//      BloomFilter.optimalSegmentBloomFilterByteSize(1000, 0.0) shouldBe BloomFilter.minimumSize
//      BloomFilter.optimalSegmentBloomFilterByteSize(0, 0.001) shouldBe BloomFilter.minimumSize
//    }
//
//    "return the number of bytes required to store the Bloom filter" in {
//      (1 to 1000) foreach {
//        i =>
//          val numberOfItems = i * 10
//          val falsePositiveRate = 0.0 + (0 + "." + i.toString).toDouble
//          val bloomFilter =
//            BloomFilter(
//              numberOfKeys = numberOfItems,
//              falsePositiveRate = falsePositiveRate
//            )
//          bloomFilter.bytes.written should be <= BloomFilter.optimalSegmentBloomFilterByteSize(numberOfItems, falsePositiveRate)
//      }
//    }
//  }
//
//  "init" should {
//    "not initialise if keyValues are empty" in {
//      BloomFilter.init(
//        numberOfKeys = 0,
//        falsePositiveRate = TestData.falsePositiveRate
//      ) shouldBe empty
//    }
//
//    "not initialise if false positive range is 0.0 are empty" in {
//      BloomFilter.init(
//        numberOfKeys = 100,
//        falsePositiveRate = 0.0
//      ) shouldBe empty
//    }
//
//    "not initialise bloomFilter if it contain removeRange" in {
//      runThisParallel(10.times) {
//        implicit val time = TestTimer.random
//
//        BloomFilter.init(
//          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(None, time.next))),
//        ) shouldBe empty
//
//        BloomFilter.init(
//          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(Some(randomDeadline()), time.next))),
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
//        ) shouldBe empty
//
//        BloomFilter.init(
//          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Function(randomFunctionId(), time.next))))),
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
//        ) shouldBe defined
//      }
//    }
//  }
//
//  "bloomFilter error check" in {
//    def runAssert(data: Seq[String],
//                  bloom: BloomFilter,
//                  bytes: Slice[Byte]) = {
//
//      val positives =
//        data.par collect {
//          case data if !BloomFilter.mightContain(data, Reader(bytes), bloom).get =>
//            data
//        }
//
//      val falsePositives =
//        data.par collect {
//          case data if BloomFilter.mightContain(Random.alphanumeric.take(2000).mkString.getBytes(), Reader(bytes), bloom).get =>
//            data
//        }
//
//      println(s"errors out of ${data.size}: " + positives.size)
//      println(s"falsePositives out of ${data.size}: " + falsePositives.size)
//      println(s"Optimal byte size: " + bloom.numberOfBits)
//      println(s"Actual byte size: " + bytes.written)
//      println
//
//      positives.size shouldBe 0
//      falsePositives.size should be < 200
//    }
//
//    val state =
//      BloomFilter(
//        numberOfKeys = 10000,
//        falsePositiveRate = TestData.falsePositiveRate
//      )
//
//    val data: Seq[String] =
//      (1 to 10000) map {
//        _ =>
//          val string = Random.alphanumeric.take(2000).mkString
//          BloomFilter.add(string.getBytes(), state)
//          string
//      }
//
//    val bloom: BloomFilter = BloomFilter.read(BloomFilter.Offset(0, state.bytes.written), Reader(state.bytes)).get
//    val bytes = state.bytes
//
//    runAssert(data, bloom, bytes)
//
//    runThis(10.times) {
//      val randomBytes = randomBytesSlice(1)
//
//      val (adjustedOffset, alteredBytes) =
//        eitherOne(
//          (bloom, bytes),
//          (bloom, bytes ++ randomBytesSlice(randomIntMax(100))),
//          (bloom.copy(offset = bloom.offset.copy(start = bloom.offset.start + randomBytes.size)), randomBytes ++ bytes.close()),
//          (bloom.copy(offset = bloom.offset.copy(start = bloom.offset.start + randomBytes.size)), randomBytes ++ bytes ++ randomBytesSlice(randomIntMax(100)))
//        )
//
//      runAssert(data, adjustedOffset, alteredBytes)
//    }
//  }
//
//  "write indexes and get the nearest deadline" in {
//    runThis(100.times) {
//      val keyValues = randomizedKeyValues(keyValueCount)
//      val group = randomGroup(keyValues)
//      val bloom: Option[BloomFilter.State] = BloomFilter.init(keyValues = keyValues)
//      if (BloomFilter.shouldCreateBloomFilter(keyValues)) {
//        bloom shouldBe defined
//
//        val deadline =
//          SegmentWriter.writeIndexesAndGetDeadline(
//            keyValue = group,
//            hashIndex = None,
//            bloomFilter = bloom,
//            binarySearchIndex = None,
//            currentNearestDeadline = None
//          ).get
//
//        assertBloom(keyValues, bloom.get)
//        deadline shouldBe nearestDeadline(keyValues)
//      } else {
//        bloom shouldBe empty
//      }
//    }
//  }
//}
