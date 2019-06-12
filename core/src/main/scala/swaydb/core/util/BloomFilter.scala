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

  val bloomFilterFormatID = 0.toByte

  val empty =
    new BloomFilter(
      startOffset = 0,
      _endOffset = 0,
      numberOfBits = 0,
      numberOfHashes = 0,
      hasRanges = false,
      mutable.Map.empty,
      bytes = Slice.emptyBytes
    )(KeyOrder.default)

  def apply(numberOfKeys: Int,
            falsePositiveRate: Double)(implicit keyOrder: KeyOrder[Slice[Byte]]): BloomFilter = {
    val numberOfBits = optimalNumberOfBloomFilterBits(numberOfKeys, falsePositiveRate)
    val numberOfHashes = optimalNumberOfBloomFilterHashes(numberOfKeys, numberOfBits)

    val numberOfBitsSize = Bytes.sizeOf(numberOfBits)
    val numberOfHashesSize = Bytes.sizeOf(numberOfHashes)

    val bytes = Slice.create[Byte](ByteSizeOf.byte + numberOfBitsSize + numberOfHashesSize + numberOfBits)

    bytes add bloomFilterFormatID
    bytes addIntUnsigned numberOfBits
    bytes addIntUnsigned numberOfHashes

    val startOffset = ByteSizeOf.byte + numberOfBitsSize + numberOfHashesSize

    new BloomFilter(
      startOffset = startOffset,
      _endOffset = startOffset,
      numberOfBits = numberOfBits,
      numberOfHashes = numberOfHashes,
      hasRanges = false,
      rangeFilter = mutable.Map.empty,
      bytes = bytes
    )
  }

  //when the byte size is already pre-computed.
  def apply(numberOfKeys: Int,
            falsePositiveRate: Double,
            bytes: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): BloomFilter = {
    val numberOfBits = optimalNumberOfBloomFilterBits(numberOfKeys, falsePositiveRate)
    val numberOfHashes = optimalNumberOfBloomFilterHashes(numberOfKeys, numberOfBits)

    val numberOfBitsSize = Bytes.sizeOf(numberOfBits)
    val numberOfHashesSize = Bytes.sizeOf(numberOfHashes)

    bytes add bloomFilterFormatID
    bytes addIntUnsigned numberOfBits
    bytes addIntUnsigned numberOfHashes

    val startOffset = ByteSizeOf.byte + numberOfBitsSize + numberOfHashesSize

    new BloomFilter(
      startOffset = startOffset,
      _endOffset = startOffset,
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
    ) + ByteSizeOf.byte + ByteSizeOf.int + ByteSizeOf.int

  def apply(bloomFilterBytes: Slice[Byte],
            rangeFilterBytes: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[BloomFilter] = {
    val reader = Reader(bloomFilterBytes)
    reader.get() flatMap {
      formatID =>
        if (formatID == bloomFilterFormatID)
          for {
            numberOfBits <- reader.readIntUnsigned()
            numberOfHashes <- reader.readIntUnsigned()
            rangeEntries <- if (rangeFilterBytes.isEmpty) IO.Success(mutable.Map.empty[Int, Iterable[(Byte, Byte)]]) else IntMapListBufferSerializer.read(rangeFilterBytes)
          } yield
            new BloomFilter(
              startOffset = ByteSizeOf.byte + Bytes.sizeOf(numberOfBits) + Bytes.sizeOf(numberOfHashes) + bloomFilterBytes.fromOffset,
              _endOffset = bloomFilterBytes.size - 1,
              numberOfBits = numberOfBits,
              numberOfHashes = numberOfHashes,
              hasRanges = rangeEntries.nonEmpty,
              rangeFilter = rangeEntries,
              bytes = bloomFilterBytes
            )
        else
          IO.Failure(IO.Error.Fatal(new Exception(s"Invalid bloomFilter formatID: $formatID. Expected: $bloomFilterFormatID")))
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
                  private var _endOffset: Int,
                  val numberOfBits: Int,
                  val numberOfHashes: Int,
                  var hasRanges: Boolean,
                  rangeFilter: mutable.Map[Int, Iterable[(Byte, Byte)]],
                  bytes: Slice[Byte])(implicit ordering: KeyOrder[Slice[Byte]]) {

  import ordering._

  def endOffset: Int =
    _endOffset

  private def get(index: Long): Long =
    bytes.take(startOffset + ((index >>> 6) * 8L).toInt, ByteSizeOf.long).readLong() & (1L << index)

  private def set(index: Long): Unit = {
    val offset = (startOffset + (index >>> 6) * 8L).toInt
    val long = bytes.take(offset, ByteSizeOf.long).readLong()
    if ((long & (1L << index)) == 0) {
      bytes moveWritePositionUnsafe offset
      bytes addLong (long | (1L << index))
      _endOffset = _endOffset max (offset + ByteSizeOf.long)
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

  def writeRangeFilter(slice: Slice[Byte]): Unit =
    IntMapListBufferSerializer.write(
      value = rangeFilter,
      bytes = slice
    )

  def toBloomFilterSlice: Slice[Byte] =
    bytes.slice(0, _endOffset)

  def exportSize =
    _endOffset + 1

  def currentRangeFilterBytesRequired =
    IntMapListBufferSerializer.bytesRequired(rangeFilter)

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
