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

package swaydb.core.util

import bloomfilter.mutable.BloomFilter
import swaydb.core.TestBase
import swaydb.core.data.{Transient, Value}
import swaydb.core.util.BloomFilterUtil._
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class BloomFilterUtilSpec extends TestBase {

  "BloomFilterUtil.toBytes" should {
    "write bloom filter to bytes" in {

      val bloomFilter = BloomFilter[Slice[Byte]](10, 0.01)
      (1 to 10) foreach (bloomFilter.add(_))
      (1 to 10) foreach (key => bloomFilter.mightContain(key) shouldBe true)
      (11 to 20) foreach (key => bloomFilter.mightContain(key) shouldBe false)

      val slice = Slice(bloomFilter.toBytes)

      val readBloomFilter = slice.toBloomFilter
      (1 to 10) foreach (key => readBloomFilter.mightContain(key) shouldBe true)
      (11 to 20) foreach (key => readBloomFilter.mightContain(key) shouldBe false)
    }
  }

  "BloomFilterUtil.byteSize" should {
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

  "BloomFilterUtil.initBloomFilter" should {
    "not initialise bloomFilter if it contain removeRange" in {
      import swaydb.core.map.serializer.RangeValueSerializers._
      BloomFilterUtil.initBloomFilter(
        keyValues = Slice(Transient.Range[Value.FromValue, Value.RangeValue](1, 2, None, Value.Remove(None))),
        bloomFilterFalsePositiveRate = 0.1
      ) shouldBe empty

      BloomFilterUtil.initBloomFilter(
        keyValues = Slice(Transient.Range[Value.FromValue, Value.RangeValue](1, 2, None, Value.Remove(Some(randomDeadline())))),
        bloomFilterFalsePositiveRate = 0.1
      ) shouldBe empty

      //fromValue is remove but it's not a remove ange
      BloomFilterUtil.initBloomFilter(
        keyValues = Slice(Transient.Range[Value.FromValue, Value.RangeValue](1, 2, Some(Value.Remove(None)), Value.Update(100))),
        bloomFilterFalsePositiveRate = 0.1
      ) shouldBe defined
    }
  }
}