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

import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.OffsetBase
import swaydb.core.util.{Bytes, MurmurHash3Generic}
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

object BloomFilter {

  val formatId: Byte = 1.toByte

  case class Offset(start: Int, size: Int) extends OffsetBase

  case class State(startOffset: Int,
                   numberOfBits: Int,
                   probe: Int,
                   bytes: Slice[Byte]) {

    def written =
      bytes.written

    override def hashCode(): Int =
      bytes.hashCode()
  }

  case class Header(startOffset: Int,
                    probe: Int,
                    numberOfBits: Int,
                    reader: Reader)

  val minimumSize =
    ByteSizeOf.byte + //format
      ByteSizeOf.int + //number of bits
      ByteSizeOf.int //max probe

  val empty =
    BloomFilter.State(
      startOffset = 0,
      numberOfBits = 0,
      probe = 0,
      bytes = Slice.emptyBytes
    )

  def apply(numberOfKeys: Int,
            falsePositiveRate: Double): BloomFilter.State = {
    val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
    val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits)

    val numberOfBitsSize = Bytes.sizeOf(numberOfBits)
    val maxProbeSize = Bytes.sizeOf(maxProbe)

    val bytes = Slice.create[Byte](ByteSizeOf.byte + numberOfBitsSize + maxProbeSize + numberOfBits)

    bytes add formatId
    bytes addIntUnsigned numberOfBits
    bytes addIntUnsigned maxProbe

    val startOffset = ByteSizeOf.byte + numberOfBitsSize + maxProbeSize

    BloomFilter.State(
      startOffset = startOffset,
      numberOfBits = numberOfBits,
      probe = maxProbe,
      bytes = bytes
    )
  }

  //when the byte size is already pre-computed.
  def apply(numberOfKeys: Int,
            falsePositiveRate: Double,
            bytes: Slice[Byte]): BloomFilter.State = {
    val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
    val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits)

    val numberOfBitsSize = Bytes.sizeOf(numberOfBits)
    val maxProbeSize = Bytes.sizeOf(maxProbe)

    bytes add formatId
    bytes addIntUnsigned numberOfBits
    bytes addIntUnsigned maxProbe

    val startOffset = ByteSizeOf.byte + numberOfBitsSize + maxProbeSize

    BloomFilter.State(
      startOffset = startOffset,
      numberOfBits = numberOfBits,
      probe = maxProbe,
      bytes = bytes
    )
  }

  def optimalNumberOfBits(numberOfKeys: Int, falsePositiveRate: Double): Int =
    if (numberOfKeys <= 0 || falsePositiveRate <= 0.0)
      0
    else
      math.ceil(-1 * numberOfKeys * math.log(falsePositiveRate) / math.log(2) / math.log(2)).toInt

  def optimalNumberOfProbes(numberOfKeys: Int, numberOfBits: Long): Int =
    if (numberOfKeys <= 0 || numberOfBits <= 0)
      0
    else
      math.ceil(numberOfBits / numberOfKeys * math.log(2)).toInt

  def optimalSegmentBloomFilterByteSize(numberOfKeys: Int, falsePositiveRate: Double): Int =
    BloomFilter.optimalNumberOfBits(
      numberOfKeys = numberOfKeys,
      falsePositiveRate = falsePositiveRate
    ) + minimumSize

  def readHeader(offset: Offset, reader: Reader): IO[BloomFilter.Header] =
    reader
      .moveTo(offset.start)
      .get()
      .flatMap {
        formatID =>
          if (formatID == formatId)
            for {
              numberOfBits <- reader.readIntUnsigned()
              maxProbe <- reader.readIntUnsigned()
            } yield
              BloomFilter.Header(
                startOffset = ByteSizeOf.byte + Bytes.sizeOf(numberOfBits) + Bytes.sizeOf(maxProbe) + offset.start,
                probe = maxProbe,
                numberOfBits = numberOfBits,
                reader = reader
              )
          else
            IO.Failure(IO.Error.Fatal(new Exception(s"Invalid bloomFilter formatID: $formatID. Expected: $formatId")))
      }

  def init(keyValues: Iterable[KeyValue.WriteOnly]): Option[BloomFilter.State] =
    if (keyValues.isEmpty || keyValues.last.stats.segmentHasRemoveRange || keyValues.last.stats.segmentBloomFilterSize <= 1 || keyValues.last.falsePositiveRate <= 0.0)
      None
    else
      init(
        numberOfKeys = keyValues.last.stats.segmentUniqueKeysCount,
        falsePositiveRate = keyValues.last.falsePositiveRate
      )
  /**
    * Initialise bloomFilter if key-values do no contain remove range.
    */
  def init(numberOfKeys: Int,
           falsePositiveRate: Double): Option[BloomFilter.State] =
    if (numberOfKeys <= 0 || falsePositiveRate <= 0.0)
      None
    else
      Some(
        BloomFilter(
          numberOfKeys = numberOfKeys,
          falsePositiveRate = falsePositiveRate,
          bytes = Slice.create[Byte](optimalSegmentBloomFilterByteSize(numberOfKeys, falsePositiveRate))
        )
      )

  def add(key: Slice[Byte], state: BloomFilter.State): Unit = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(key, 0, key.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var probe = 0
    while (probe < state.probe) {
      val computedHash = hash1 + probe * hash2
      val hashIndex = (computedHash & Long.MaxValue) % state.numberOfBits
      val offset = (state.startOffset + (hashIndex >>> 6) * 8L).toInt
      val long = state.bytes.take(offset, ByteSizeOf.long).readLong()
      if ((long & (1L << hashIndex)) == 0) {
        state.bytes moveWritePosition offset
        state.bytes addLong (long | (1L << hashIndex))
      }
      probe += 1
    }
  }

  def mightContain(key: Slice[Byte],
                   header: Header): Boolean = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(key, 0, key.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32
    var probe = 0
    val reader = header.reader.copy()

    while (probe < header.probe) {
      val computedHash = hash1 + probe * hash2
      val hashIndex = (computedHash & Long.MaxValue) % header.numberOfBits

      val index =
        reader
          .moveTo(header.startOffset + ((hashIndex >>> 6) * 8L).toInt)
          .readLong()
          .get & (1L << hashIndex)

      if (index == 0) return false
      probe += 1
    }
    true
  }
}
