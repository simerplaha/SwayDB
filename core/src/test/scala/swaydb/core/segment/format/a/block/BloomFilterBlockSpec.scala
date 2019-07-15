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

package swaydb.core.segment.format.a.block

import swaydb.core.CommonAssertions.{eitherOne, _}
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data.{Transient, Value}
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class BloomFilterBlockSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  val keyValueCount = 1000

  "toBytes & toSlice" should {
    "write bloom filter to bytes" in {
      runThis(10.times) {
        val compressions = eitherOne(Seq.empty, randomCompressions())

        val filter =
          BloomFilterBlock.init(
            numberOfKeys = 10,
            falsePositiveRate = 0.01,
            compressions = _ => compressions
          ).get

        (1 to 10) foreach (BloomFilterBlock.add(_, filter))

        BloomFilterBlock.close(filter).get

        val segmentBlock = SegmentBlock.decompressed(filter.bytes)
        val bloom = BloomFilterBlock.read(BloomFilterBlock.Offset(0, filter.bytes.size), segmentBlock).get
        (1 to 10) foreach (key => BloomFilterBlock.mightContain(key, bloom.decompress(segmentBlock)).get shouldBe true)
        (11 to 20) foreach (key => BloomFilterBlock.mightContain(key, bloom.decompress(segmentBlock)).get shouldBe false)

        println("numberOfBits: " + filter.numberOfBits)
        println("written: " + filter.written)
      }
    }
  }

  "optimalSegmentBloomFilterByteSize" should {
    "return empty if false positive rate is 0.0 or number of keys is 0" in {
      BloomFilterBlock.optimalSize(1000, 0.0, randomBoolean(), minimumNumberOfKeys = 0) shouldBe 0
      BloomFilterBlock.optimalSize(0, 0.001, randomBoolean(), minimumNumberOfKeys = 0) shouldBe 0
    }

    "return the number of bytes required to store the Bloom filter" in {
      (1 to 1000) foreach {
        i =>
          val compressions = eitherOne(Seq.empty, randomCompressions())

          val numberOfItems = i * 10
          val falsePositiveRate = 0.0 + (0 + "." + i.toString).toDouble
          val compression = compressions

          val bloomFilter =
            BloomFilterBlock.init(
              numberOfKeys = numberOfItems,
              falsePositiveRate = falsePositiveRate,
              compressions = _ => compression
            ).get

          bloomFilter.bytes.size should be <=
            BloomFilterBlock.optimalSize(
              numberOfKeys = numberOfItems,
              falsePositiveRate = falsePositiveRate,
              hasCompression = compression.nonEmpty,
              minimumNumberOfKeys = 0
            )
      }
    }
  }

  "init" should {
    "not initialise if keyValues are empty" in {
      BloomFilterBlock.init(
        numberOfKeys = 0,
        falsePositiveRate = randomFalsePositiveRate(),
        compressions = _ => randomCompressionsOrEmpty()
      ) shouldBe empty
    }

    "not initialise if false positive range is 0.0 are empty" in {
      BloomFilterBlock.init(
        numberOfKeys = 100,
        falsePositiveRate = 0.0,
        compressions = _ => randomCompressionsOrEmpty()
      ) shouldBe empty
    }

    "not initialise bloomFilter if it contain removeRange" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        BloomFilterBlock.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(None, time.next)))
        ) shouldBe empty

        BloomFilterBlock.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Remove(Some(randomDeadline()), time.next)))
        ) shouldBe empty
      }
    }

    "not initialise bloomFilter if it contains a range function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //range functions can also contain Remove so BloomFilter should not be created
        BloomFilterBlock.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.Function(Slice.emptyBytes, time.next)))
        ) shouldBe empty
      }
    }

    "not initialise bloomFilter if it contain pendingApply with remove or function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //range functions can also contain Remove so BloomFilter should not be created
        BloomFilterBlock.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Remove(randomDeadlineOption(), time.next)))))
        ) shouldBe empty

        BloomFilterBlock.init(
          keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 2, None, Value.PendingApply(Slice(Value.Function(randomFunctionId(), time.next)))))
        ) shouldBe empty
      }
    }

    "initialise bloomFilter if it does not contain pendingApply with remove or function" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random

        //pending apply should allow to create bloomFilter if it does not have remove or function.
        BloomFilterBlock.init(
          keyValues =
            Slice(
              Transient.Range.create[FromValue, RangeValue](
                fromKey = 1,
                toKey = 2,
                fromValue = None,
                rangeValue = Value.PendingApply(Slice(Value.Update(randomStringOption, randomDeadlineOption(), time.next))),
                bloomFilterConfig = BloomFilterBlock.Config.random.copy(falsePositiveRate = 0.001, minimumNumberOfKeys = 0)
              )
            )
        ) shouldBe defined
      }
    }

    "initialise bloomFilter when from value is remove but range value is not" in {
      runThisParallel(10.times) {
        implicit val time = TestTimer.random
        //fromValue is remove but it's not a remove range.
        BloomFilterBlock.init(
          keyValues =
            Slice(
              Transient.Range.create[FromValue, RangeValue](
                fromKey = 1,
                toKey = 2,
                fromValue = Some(Value.Remove(None, time.next)),
                rangeValue = Value.update(100),
                bloomFilterConfig = BloomFilterBlock.Config.random.copy(falsePositiveRate = 0.001, minimumNumberOfKeys = 0)
              )
            )
        ) shouldBe defined
      }
    }
  }

  "bloomFilter error check" in {
    def runAssert(data: Seq[String],
                  bloom: BloomFilterBlock,
                  bytes: Slice[Byte]) = {

      val segmentBlock = SegmentBlock.decompressed(bytes)

      val positives =
        data collect {
          case data if !BloomFilterBlock.mightContain(data, bloom.decompress(segmentBlock)).get =>
            data
        }

      val falsePositives =
        data collect {
          case data if BloomFilterBlock.mightContain(Random.alphanumeric.take(2000).mkString.getBytes(), bloom.decompress(segmentBlock)).get =>
            data
        }

      println(s"errors out of ${data.size}: " + positives.size)
      println(s"falsePositives out of ${data.size}: " + falsePositives.size)
      println(s"Optimal byte size: " + bloom.numberOfBits)
      println(s"Actual byte size: " + bytes.size)
      println

      positives.size shouldBe 0
      falsePositives.size should be < 200
    }

    runThis(5.times) {
      val compressions = eitherOne(Seq.empty, randomCompressions())

      val state =
        BloomFilterBlock.init(
          numberOfKeys = 10000,
          falsePositiveRate = 0.001,
          compressions = _ => compressions
        ).get

      val data: Seq[String] =
        (1 to 10000) map {
          _ =>
            val string = Random.alphanumeric.take(2000).mkString
            BloomFilterBlock.add(string.getBytes(), state)
            string
        }

      BloomFilterBlock.close(state).get

      val segmentBlock = SegmentBlock.decompressed(state.bytes)

      val bloom: BloomFilterBlock = BloomFilterBlock.read(BloomFilterBlock.Offset(0, state.bytes.size), segmentBlock).get
      val bytes = state.bytes

      runAssert(data, bloom, bytes)

      val randomBytes = randomBytesSlice(randomIntMax(9) + 1)

      val (adjustedOffset, alteredBytes) =
        eitherOne(
//          (bloom, bytes),
//          (bloom, bytes ++ randomBytesSlice(randomIntMax(100))),
//          (bloom, bytes ++ randomBytesSlice(randomIntMax(100))),
          (bloom.copy(offset = bloom.offset.copy(start = bloom.offset.start + randomBytes.size)), randomBytes ++ bytes.close()),
          (bloom.copy(offset = bloom.offset.copy(start = bloom.offset.start + randomBytes.size)), randomBytes ++ bytes.close()),
//          (bloom.copy(offset = bloom.offset.copy(start = bloom.offset.start + randomBytes.size)), randomBytes ++ bytes ++ randomBytesSlice(randomIntMax(100)))
        )

      runAssert(data, adjustedOffset, alteredBytes)
    }
  }

  //  "write indexes and value the nearest deadline" in {
  //    runThis(100.times) {
  //      val keyValues = randomizedKeyValues(keyValueCount)
  //      val group = randomGroup(keyValues)
  //      val bloom: Option[BloomFilter.State] =
  //        BloomFilter.init(
  //          keyValues = keyValues,
  //          compressions = eitherOne(Seq.empty, Seq(randomCompression()))
  //        )
  //
  //      if (BloomFilter.shouldCreateBloomFilter(keyValues)) {
  //        BloomFilter.close(bloom.get).get
  //
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
}
