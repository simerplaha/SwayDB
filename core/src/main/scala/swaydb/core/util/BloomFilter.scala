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

import swaydb.core.data.KeyValue
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.ValueSerializer.IntMapListBufferSerializer
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object BloomFilter {

  val empty =
    new BloomFilter(
      startOffset = 0,
      _maxStartOffset = 0,
      numberOfBits = 0,
      numberOfHashes = 0,
      hasRanges = false,
      mutable.Map.empty,
      bytes = Slice.emptyBytes
    )(KeyOrder.default)

  val byteBufferStartOffset =
    ByteSizeOf.int + //numberOfBits
      ByteSizeOf.int //numberOfHashes

  def apply(numberOfKeys: Int,
            falsePositiveRate: Double)(implicit keyOrder: KeyOrder[Slice[Byte]]): BloomFilter = {
    val numberOfBits = optimalNumberOfBloomFilterBits(numberOfKeys, falsePositiveRate)
    val numberOfHashes = optimalNumberOfBloomFilterHashes(numberOfKeys, numberOfBits)
    val buffer = Slice.create[Byte](byteBufferStartOffset + numberOfBits)
    buffer.addInt(numberOfBits)
    buffer.addInt(numberOfHashes)
    new BloomFilter(
      _maxStartOffset = 0,
      startOffset = byteBufferStartOffset,
      numberOfBits = numberOfBits,
      numberOfHashes = numberOfHashes,
      hasRanges = false,
      rangeFilter = mutable.Map.empty,
      bytes = buffer
    )
  }

  def apply(numberOfBits: Int,
            numberOfHashes: Int,
            bytes: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): BloomFilter = {
    bytes.addInt(numberOfBits)
    bytes.addInt(numberOfHashes)

    new BloomFilter(
      _maxStartOffset = 0,
      startOffset = byteBufferStartOffset,
      numberOfBits = numberOfBits,
      numberOfHashes = numberOfHashes,
      hasRanges = false,
      rangeFilter = mutable.Map.empty,
      bytes = bytes
    )
  }

  def optimalRangeFilterByteSize(numberOfRanges: Int): Int =
    IntMapListBufferSerializer optimalBytesRequired numberOfRanges

  def optimalNumberOfBloomFilterBits(numberOfKeys: Int, falsePositiveRate: Double): Int =
    math.ceil(-1 * numberOfKeys * math.log(falsePositiveRate) / math.log(2) / math.log(2)).toInt

  def optimalNumberOfBloomFilterHashes(numberOfKeys: Int, numberOfInt: Long): Int =
    math.ceil(numberOfInt / numberOfKeys * math.log(2)).toInt

  def optimalSegmentBloomFilterByteSize(numberOfKeys: Int, falsePositiveRate: Double): Int =
    BloomFilter.optimalNumberOfBloomFilterBits(
      numberOfKeys = numberOfKeys,
      falsePositiveRate = falsePositiveRate
    ) + byteBufferStartOffset

  def apply(bloomFilterBytes: Slice[Byte], rangeFilterBytes: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[BloomFilter] = {
    val reader = Reader(bloomFilterBytes)
    for {
      numberOfBits <- reader.readInt()
      numberOfHashes <- reader.readInt()
      rangeEntries <- if (rangeFilterBytes.isEmpty) IO.Success(mutable.Map.empty[Int, Iterable[(Byte, Byte)]]) else IntMapListBufferSerializer.read(rangeFilterBytes)
    } yield {
      new BloomFilter(
        startOffset = byteBufferStartOffset + bloomFilterBytes.fromOffset,
        _maxStartOffset = bloomFilterBytes.size - 8,
        numberOfBits = numberOfBits,
        numberOfHashes = numberOfHashes,
        hasRanges = rangeEntries.nonEmpty,
        rangeFilter = rangeEntries,
        bytes = bloomFilterBytes
      )
    }
  }

  /**
    * Initialise bloomFilter if key-values do no contain remove range.
    */
  def init(keyValues: Iterable[KeyValue.WriteOnly],
           bloomFilterFalsePositiveRate: Double)(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[BloomFilter] =
    keyValues.lastOption map {
      last =>
        BloomFilter(
          numberOfKeys = last.stats.bloomFilterKeysCount,
          falsePositiveRate = bloomFilterFalsePositiveRate
        )
    }
}

class BloomFilter(val startOffset: Int,
                  private var _maxStartOffset: Int,
                  val numberOfBits: Int,
                  val numberOfHashes: Int,
                  var hasRanges: Boolean,
                  rangeFilter: mutable.Map[Int, Iterable[(Byte, Byte)]],
                  bytes: Slice[Byte])(implicit ordering: KeyOrder[Slice[Byte]]) {

  import ordering._

  def maxStartOffset: Int =
    _maxStartOffset

  private def get(index: Long): Long =
    bytes.take(startOffset + ((index >>> 6) * 8L).toInt, ByteSizeOf.long).readLong() & (1L << index)

  private def set(index: Long): Unit = {
    val offset = startOffset + (index >>> 6) * 8L
    _maxStartOffset = _maxStartOffset max offset.toInt
    val long = bytes.take(offset.toInt, ByteSizeOf.long).readLong()
    if ((long & (1L << index)) == 0) {
      bytes.moveTo(offset.toInt)
      bytes.addLong(long | (1L << index))
    }
  }

  def add(key: Slice[Byte]): Unit = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(key, 0, key.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var i = 0
    while (i < numberOfHashes) {
      val computedHash = hash1 + i * hash2
      set((computedHash & Long.MaxValue) % numberOfBits)
      i += 1
    }
  }

  def add(from: Slice[Byte], to: Slice[Byte]): Unit = {
    val common = Bytes.commonPrefix(from, to)
    rangeFilter.get(common) map {
      ranges =>
        ranges.asInstanceOf[ListBuffer[(Byte, Byte)]] += ((from(common), to(common)))
    } getOrElse {
      rangeFilter.put(common, ListBuffer((from(common), to(common))))
    }

    hasRanges = true
    add(from)
    add(from.take(common))
  }

  def mightContain(key: Slice[Byte]): Boolean = {
    var contains = mightContainHashed(key)
    if (!contains && hasRanges) {
      rangeFilter exists {
        case (commonLowerBytes, rangeBytes) =>
          val lowerItemBytes = key.take(commonLowerBytes)
          if (mightContainHashed(lowerItemBytes)) {
            val lowerItemBytesWithOneBit = key.take(commonLowerBytes + 1)
            rangeBytes exists {
              case (leftBloomFilterRangeByte, rightBloomFilterRangeByte) =>
                val leftBytes = lowerItemBytes ++ Slice(leftBloomFilterRangeByte)
                val rightBytes = lowerItemBytes ++ Slice(rightBloomFilterRangeByte)
                contains = leftBytes <= lowerItemBytesWithOneBit && lowerItemBytesWithOneBit < rightBytes
                contains
            }
          } else {
            contains
          }
      }
    }
    contains
  }

  private def mightContainHashed(key: Slice[Byte]): Boolean = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(key, 0, key.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var i = 0
    while (i < numberOfHashes) {
      val computedHash = hash1 + i * hash2
      if (get((computedHash & Long.MaxValue) % numberOfBits) == 0)
        return false
      i += 1
    }
    true
  }

  def toBloomFilterSlice: Slice[Byte] =
    bytes.slice(0, maxStartOffset + 7)

  def toRangeFilterSlice: Slice[Byte] =
    if (rangeFilter.isEmpty)
      Slice.emptyBytes
    else {
      val rangeFilterBytes = Slice.create[Byte](IntMapListBufferSerializer.bytesRequired(rangeFilter))
      IntMapListBufferSerializer.write(value = rangeFilter, bytes = rangeFilterBytes)
      rangeFilterBytes
    }

  override def hashCode(): Int =
    bytes.hashCode()
}
