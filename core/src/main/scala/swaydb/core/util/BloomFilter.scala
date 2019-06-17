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

  val minimumSize =
    ByteSizeOf.byte + //format
      ByteSizeOf.int + //number of bits
      ByteSizeOf.int //number of hashes

  /**
    * Used to create an empty rangeFilter instead of creating an empty one every time
    * this val is used to save memory.
    */
  val emptyRangeFilter: mutable.Map[Int, Iterable[(Byte, Byte)]] =
    new mutable.Map[Int, Iterable[(Byte, Byte)]] {
      override def +=(kv: (Int, Iterable[(Byte, Byte)])): this.type = throw new NotImplementedError("emptyRangeFilter")
      override def -=(key: Int): this.type = throw new NotImplementedError("emptyRangeFilter")
      override def get(key: Int): Option[Iterable[(Byte, Byte)]] = throw new NotImplementedError("emptyRangeFilter")
      override def iterator: Iterator[(Int, Iterable[(Byte, Byte)])] = throw new NotImplementedError("emptyRangeFilter")
    }

  val empty =
    new BloomFilter(
      startOffset = 0,
      _endOffset = 0,
      numberOfBits = 0,
      numberOfHashes = 0,
      hasRanges = false,
      rangeFilter = None,
      bloomBytes = Slice.emptyBytes
    )(KeyOrder.default)

  def apply(numberOfKeys: Int,
            falsePositiveRate: Double,
            enableRangeFilter: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): BloomFilter = {
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
      rangeFilter = if (enableRangeFilter) Some(mutable.Map.empty) else None,
      bloomBytes = bytes
    )
  }

  //when the byte size is already pre-computed.
  def apply(numberOfKeys: Int,
            falsePositiveRate: Double,
            enableRangeFilter: Boolean,
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
      rangeFilter = if (enableRangeFilter) Some(mutable.Map.empty) else None,
      bloomBytes = bytes
    )
  }

  def optimalRangeFilterByteSize(numberOfRanges: Int, rangeFilterCommonPrefixes: Iterable[Int]): Int =
    IntMapListBufferSerializer.optimalBytesRequired(numberOfRanges, rangeFilterCommonPrefixes)

  def optimalNumberOfBloomFilterBits(numberOfKeys: Int, falsePositiveRate: Double): Int =
    if (numberOfKeys <= 0 || falsePositiveRate <= 0.0)
      0
    else
      math.ceil(-1 * numberOfKeys * math.log(falsePositiveRate) / math.log(2) / math.log(2)).toInt

  def optimalNumberOfBloomFilterHashes(numberOfKeys: Int, numberOfInt: Long): Int =
    math.ceil(numberOfInt / numberOfKeys * math.log(2)).toInt

  def optimalSegmentBloomFilterByteSize(numberOfKeys: Int, falsePositiveRate: Double): Int =
    BloomFilter.optimalNumberOfBloomFilterBits(
      numberOfKeys = numberOfKeys,
      falsePositiveRate = falsePositiveRate
    ) + minimumSize

  def apply(bloomFilterBytes: Slice[Byte],
            rangeFilterBytes: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[BloomFilter] = {
    val reader = Reader(bloomFilterBytes)
    reader.get() flatMap {
      formatID =>
        if (formatID == bloomFilterFormatID)
          for {
            numberOfBits <- reader.readIntUnsigned()
            numberOfHashes <- reader.readIntUnsigned()
            rangeEntries <- if (rangeFilterBytes.isEmpty) IO.none else IntMapListBufferSerializer.read(rangeFilterBytes).map(Some(_))
          } yield
            new BloomFilter(
              startOffset = ByteSizeOf.byte + Bytes.sizeOf(numberOfBits) + Bytes.sizeOf(numberOfHashes) + bloomFilterBytes.fromOffset,
              _endOffset = bloomFilterBytes.size - 1,
              numberOfBits = numberOfBits,
              numberOfHashes = numberOfHashes,
              hasRanges = rangeEntries.exists(_.nonEmpty),
              rangeFilter = rangeEntries,
              bloomBytes = bloomFilterBytes
            )
        else
          IO.Failure(IO.Error.Fatal(new Exception(s"Invalid bloomFilter formatID: $formatID. Expected: $bloomFilterFormatID")))
    }
  }

  /**
    * Initialise bloomFilter if key-values do no contain remove range.
    */
  def init(keyValues: Iterable[KeyValue.WriteOnly],
           falsePositiveRate: Double,
           enableRangeFilter: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[BloomFilter] =
    if (falsePositiveRate <= 0.0)
      None
    else
      keyValues.lastOption flatMap {
        last =>
          //if rangeFilter is disabled then disable bloomFilter if if it has remove range.
          if (!enableRangeFilter && last.stats.hasRemoveRange)
            None
          else
            Some(
              BloomFilter(
                numberOfKeys = last.stats.bloomFilterKeysCount,
                falsePositiveRate = falsePositiveRate,
                enableRangeFilter = enableRangeFilter
              )
            )
      }

  /**
    * Initialise bloomFilter if key-values do no contain remove range.
    */
  def init(numberOfKeys: Int,
           hasRemoveRange: Boolean,
           falsePositiveRate: Double,
           enableRangeFilter: Boolean,
           bytes: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[BloomFilter] =
    if (falsePositiveRate <= 0.0 || (!enableRangeFilter && hasRemoveRange))
      None
    else
      Some(
        BloomFilter(
          numberOfKeys = numberOfKeys,
          falsePositiveRate = falsePositiveRate,
          enableRangeFilter = enableRangeFilter,
          bytes = bytes
        )
      )
}

class BloomFilter(val startOffset: Int,
                  private var _endOffset: Int,
                  val numberOfBits: Int,
                  val numberOfHashes: Int,
                  var hasRanges: Boolean,
                  rangeFilter: Option[mutable.Map[Int, Iterable[(Byte, Byte)]]],
                  bloomBytes: Slice[Byte])(implicit ordering: KeyOrder[Slice[Byte]]) {

  import ordering._

  def endOffset: Int =
    _endOffset

  private def get(index: Long): Long =
    bloomBytes.take(startOffset + ((index >>> 6) * 8L).toInt, ByteSizeOf.long).readLong() & (1L << index)

  private def set(index: Long): Unit = {
    val offset = (startOffset + (index >>> 6) * 8L).toInt
    val long = bloomBytes.take(offset, ByteSizeOf.long).readLong()
    if ((long & (1L << index)) == 0) {
      bloomBytes moveWritePositionUnsafe offset
      bloomBytes addLong (long | (1L << index))
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
    rangeFilter foreach {
      rangeFilter =>
        rangeFilter.get(common) map {
          ranges =>
            ranges.asInstanceOf[ListBuffer[(Byte, Byte)]] += ((from(common), to(common)))
        } getOrElse {
          rangeFilter.put(common, ListBuffer((from(common), to(common))))
        }
    }
    hasRanges = true
    add(from)
    add(from.take(common))
  }

  def mightContain(key: Slice[Byte]): Boolean = {
    var contains = mightContainHashed(key)
    if (!contains && hasRanges) {
      rangeFilter exists {
        rangeFilter =>
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
      value = rangeFilter getOrElse BloomFilter.emptyRangeFilter,
      bytes = slice
    )

  def toBloomFilterSlice: Slice[Byte] =
    bloomBytes.slice(0, _endOffset)

  def exportSize =
    _endOffset + 1

  def currentRangeFilterBytesRequired =
    rangeFilter
      .map(IntMapListBufferSerializer.bytesRequired)
      .getOrElse(0)

  def toRangeFilterSlice: Slice[Byte] = {
    val bytesRequired = IntMapListBufferSerializer.bytesRequired(rangeFilter.getOrElse(BloomFilter.emptyRangeFilter))
    val bytes = Slice.create[Byte](bytesRequired)
    writeRangeFilter(bytes)
    bytes
  }

  override def hashCode(): Int =
    bloomBytes.hashCode()
}
