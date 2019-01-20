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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import bloomfilter.hashing.MurmurHash3Generic
import bloomfilter.mutable.BloomFilter
import bloomfilter.{CanGenerateHashFrom, CanGetDataFrom}
import swaydb.core.data.KeyValue
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

private[core] object BloomFilterUtil {

  implicit case object CanGetDataFromByteSlice extends CanGetDataFrom[Slice[Byte]] {
    override def getLong(bytes: Slice[Byte], offset: Int): Long =
      (bytes(offset + 7).toLong << 56) |
        ((bytes(offset + 6) & 0xffL) << 48) |
        ((bytes(offset + 5) & 0xffL) << 40) |
        ((bytes(offset + 4) & 0xffL) << 32) |
        ((bytes(offset + 3) & 0xffL) << 24) |
        ((bytes(offset + 2) & 0xffL) << 16) |
        ((bytes(offset + 1) & 0xffL) << 8) |
        bytes(offset) & 0xffL

    override def getByte(from: Slice[Byte], offset: Int): Byte =
      from.get(offset)
  }

  implicit case object CanGenerateHashFromByteSlice extends CanGenerateHashFrom[Slice[Byte]] {
    override def generateHash(bytes: Slice[Byte]): Long =
      MurmurHash3Generic.murmurhash3_x64_64(bytes, 0, bytes.size, 0)
  }

  implicit class BloomFilterImplicit[T](bloomFilter: BloomFilter[T]) {
    def toBytes: Array[Byte] = {
      val boomFilterOut = new ByteArrayOutputStream()
      bloomFilter.writeTo(boomFilterOut)
      boomFilterOut.toByteArray
    }

    def toSlice: Slice[Byte] =
      Slice(toBytes)
  }

  implicit class BloomFilterReadImplicit[T](slice: Slice[Byte]) {
    def toBloomFilter: BloomFilter[Slice[Byte]] =
      BloomFilter.readFrom(new ByteArrayInputStream(slice.toArray))
  }

  val emptyBloomFilter = BloomFilter[Slice[Byte]](1, 1)

  def byteSize(numberOfItems: Long, falsePositiveRate: Double): Int = {
    val numberOfBits = BloomFilter.optimalNumberOfBits(numberOfItems, falsePositiveRate)
    ((Math.ceil(numberOfBits / 64.0) * ByteSizeOf.long) + ByteSizeOf.long + ByteSizeOf.long + ByteSizeOf.int).toInt
  }

  def initBloomFilter(keyValues: Iterable[KeyValue.WriteOnly],
                      bloomFilterFalsePositiveRate: Double): Option[BloomFilter[Slice[Byte]]] =
    if (keyValues.last.stats.hasRemoveRange)
      None
    else
      Some(
        BloomFilter[Slice[Byte]](
          numberOfItems = keyValues.last.stats.bloomFilterItemsCount,
          falsePositiveRate = bloomFilterFalsePositiveRate
        )
      )
}
