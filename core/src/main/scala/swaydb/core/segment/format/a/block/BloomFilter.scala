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

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.CompressionInternal
import swaydb.core.data.KeyValue
import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.format.a.OffsetBase
import swaydb.core.util.{Bytes, MurmurHash3Generic, Options}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

object BloomFilter extends LazyLogging {

  object Config {
    val disabled =
      Config(
        falsePositiveRate = 0.0,
        minimumNumberOfKeys = Int.MaxValue,
        cacheOnAccess = false,
        compressions = Seq.empty
      )

    def apply(config: swaydb.data.config.MightContainKeyIndex): Config =
      config match {
        case swaydb.data.config.MightContainKeyIndex.Disable =>
          Config(
            falsePositiveRate = 0.0,
            minimumNumberOfKeys = Int.MaxValue,
            cacheOnAccess = false,
            compressions = Seq.empty
          )
        case enable: swaydb.data.config.MightContainKeyIndex.Enable =>
          Config(
            falsePositiveRate = enable.falsePositiveRate,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            cacheOnAccess = enable.cacheOnAccess,
            compressions = enable.compression map CompressionInternal.apply
          )
      }
  }

  case class Config(falsePositiveRate: Double,
                    minimumNumberOfKeys: Int,
                    cacheOnAccess: Boolean,
                    compressions: Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends OffsetBase

  case class State(startOffset: Int,
                   numberOfBits: Int,
                   maxProbe: Int,
                   headerSize: Int,
                   var _bytes: Slice[Byte],
                   compressions: Seq[CompressionInternal]) {

    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes

    def written =
      bytes.size

    override def hashCode(): Int =
      bytes.hashCode()
  }

  def optimalSize(numberOfKeys: Int,
                  falsePositiveRate: Double,
                  hasCompression: Boolean): Int = {
    if (falsePositiveRate <= 0.0 || numberOfKeys <= 0) {
      0
    } else {
      val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
      val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits)

      val numberOfBitsSize = Bytes.sizeOf(numberOfBits)
      val maxProbeSize = Bytes.sizeOf(maxProbe)

      val headerByteSize =
        Block.headerSize(hasCompression) +
          numberOfBitsSize +
          maxProbeSize +
          numberOfBits

      Bytes.sizeOf(headerByteSize) +
        headerByteSize
    }
  }

  private def apply(numberOfKeys: Int,
                    falsePositiveRate: Double,
                    compressions: Seq[CompressionInternal]): BloomFilter.State = {
    val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
    val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits)

    val numberOfBitsSize = Bytes.sizeOf(numberOfBits)
    val maxProbeSize = Bytes.sizeOf(maxProbe)

    val headerBytesSize =
      Block.headerSize(compressions.nonEmpty) +
        numberOfBitsSize +
        maxProbeSize

    val headerSize =
      Bytes.sizeOf(headerBytesSize) +
        headerBytesSize

    val bytes = Slice.create[Byte](headerSize + numberOfBits)

    BloomFilter.State(
      startOffset = headerSize,
      numberOfBits = numberOfBits,
      headerSize = headerSize,
      maxProbe = maxProbe,
      _bytes = bytes,
      compressions = compressions
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

  def close(state: State): IO[State] =
    Block.create(
      headerSize = state.headerSize,
      bytes = state.bytes,
      compressions = state.compressions
    ) flatMap {
      compressedOrUncompressedBytes =>
        IO {
          state.bytes = compressedOrUncompressedBytes
          state.bytes addIntUnsigned state.numberOfBits
          state.bytes addIntUnsigned state.maxProbe
          if (state.bytes.currentWritePosition > state.headerSize)
            throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
          state
        }
    }

  def read(offset: Offset,
           segmentReader: Reader): IO[BloomFilter] =
    for {
      blockHeader <- Block.readHeader(offset = offset, segmentReader = segmentReader)
      numberOfBits <- blockHeader.headerReader.readIntUnsigned()
      maxProbe <- blockHeader.headerReader.readIntUnsigned()
    } yield
      BloomFilter(
        offset = offset,
        headerSize = blockHeader.headerSize,
        maxProbe = maxProbe,
        numberOfBits = numberOfBits,
        compressionInfo = blockHeader.compressionInfo
      )

  def shouldNotCreateBloomFilter(keyValues: Iterable[KeyValue.WriteOnly]): Boolean =
    keyValues.isEmpty ||
      keyValues.last.stats.segmentHasRemoveRange ||
      keyValues.last.stats.segmentBloomFilterSize <= 0 ||
      keyValues.last.bloomFilterConfig.falsePositiveRate <= 0.0

  def shouldCreateBloomFilter(keyValues: Iterable[KeyValue.WriteOnly]): Boolean =
    !shouldNotCreateBloomFilter(keyValues)

  def init(keyValues: Iterable[KeyValue.WriteOnly]): Option[BloomFilter.State] =
    if (shouldCreateBloomFilter(keyValues))
      init(
        numberOfKeys = keyValues.last.stats.segmentUniqueKeysCount,
        falsePositiveRate = keyValues.last.bloomFilterConfig.falsePositiveRate,
        compressions = keyValues.last.bloomFilterConfig.compressions
      )
    else
      None

  /**
    * Initialise bloomFilter if key-values do no contain remove range.
    */
  def init(numberOfKeys: Int,
           falsePositiveRate: Double,
           compressions: Seq[CompressionInternal]): Option[BloomFilter.State] =
    if (numberOfKeys <= 0 || falsePositiveRate <= 0.0)
      None
    else
      Some(
        BloomFilter(
          numberOfKeys = numberOfKeys,
          falsePositiveRate = falsePositiveRate,
          compressions = compressions
        )
      )

  def add(key: Slice[Byte],
          state: BloomFilter.State): Unit = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(key, 0, key.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var probe = 0
    while (probe < state.maxProbe) {
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
                   reader: BlockReader[BloomFilter]): IO[Boolean] = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(key, 0, key.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    (0 until reader.block.maxProbe)
      .untilSomeResult {
        probe =>
          val computedHash = hash1 + probe * hash2
          val hashIndex = (computedHash & Long.MaxValue) % reader.block.numberOfBits

          reader
            .moveTo(((hashIndex >>> 6) * 8L).toInt)
            .readLong()
            .map {
              index =>
                if ((index & (1L << hashIndex)) == 0)
                  Options.`false`
                else
                  None
            }
      }
      .map(_.getOrElse(true))
  }
}

case class BloomFilter(offset: BloomFilter.Offset,
                       maxProbe: Int,
                       numberOfBits: Int,
                       headerSize: Int,
                       compressionInfo: Option[Block.CompressionInfo]) extends Block {

  override def createBlockReader(bytes: Slice[Byte]): BlockReader[BloomFilter] =
    createBlockReader(Reader(bytes))

  override def createBlockReader(segmentReader: Reader): BlockReader[BloomFilter] =
    new BlockReader(
      reader = segmentReader,
      block = this
    )
  override def updateOffset(start: Int, size: Int): Block =
    copy(offset = BloomFilter.Offset(start = start, size = size))
}
