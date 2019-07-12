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
import swaydb.core.io.reader.BlockReader
import swaydb.core.segment.format.a.block.BinarySearchIndex.Config.defaultBlockIO
import swaydb.core.util.{Bytes, FunctionUtil, MurmurHash3Generic, Options}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.config.{BlockIO, BlockInfo, UncompressedBlockInfo}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

private[core] object BloomFilter extends LazyLogging {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {
    val disabled =
      Config(
        falsePositiveRate = 0.0,
        minimumNumberOfKeys = Int.MaxValue,
        blockIO = blockInfo => BlockIO.SynchronisedIO(cacheOnAccess = blockInfo.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.MightContainIndex): Config =
      config match {
        case swaydb.data.config.MightContainIndex.Disable =>
          Config(
            falsePositiveRate = 0.0,
            minimumNumberOfKeys = Int.MaxValue,
            blockIO = blockInfo => BlockIO.SynchronisedIO(cacheOnAccess = blockInfo.isCompressed),
            compressions = _ => Seq.empty
          )
        case enable: swaydb.data.config.MightContainIndex.Enable =>
          Config(
            falsePositiveRate = enable.falsePositiveRate,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            blockIO = FunctionUtil.safe(defaultBlockIO, enable.blockIO),
            compressions =
              FunctionUtil.safe(
                default = _ => Seq.empty[CompressionInternal],
                f = enable.compression(_) map CompressionInternal.apply
              )
          )
      }
  }

  case class Config(falsePositiveRate: Double,
                    minimumNumberOfKeys: Int,
                    blockIO: BlockInfo => BlockIO,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  case class MemoryBlock(bloomFilter: BloomFilter,
                         bytes: Slice[Byte])

  case class Offset(start: Int, size: Int) extends BlockOffset

  case class State(startOffset: Int,
                   numberOfBits: Int,
                   maxProbe: Int,
                   headerSize: Int,
                   var _bytes: Slice[Byte],
                   compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {

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
                  hasCompression: Boolean,
                  minimumNumberOfKeys: Int): Int = {
    if (falsePositiveRate <= 0.0 || numberOfKeys < minimumNumberOfKeys || numberOfKeys <= 0) {
      0
    } else {
      val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
      val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits)

      val numberOfBitsSize = Bytes.sizeOf(numberOfBits)
      val maxProbeSize = Bytes.sizeOf(maxProbe)

      val headerByteSize =
        Block.headerSize(hasCompression) +
          numberOfBitsSize +
          maxProbeSize

      Bytes.sizeOf(headerByteSize) +
        headerByteSize +
        numberOfBits
    }
  }

  private def apply(numberOfKeys: Int,
                    falsePositiveRate: Double,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal]): BloomFilter.State = {
    val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
    val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits)

    val numberOfBitsSize = Bytes.sizeOf(numberOfBits)
    val maxProbeSize = Bytes.sizeOf(maxProbe)

    val headerBytesSize =
      Block.headerSize(compressions(UncompressedBlockInfo(numberOfBits)).nonEmpty) +
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
      math.ceil(-1 * numberOfKeys * math.log(falsePositiveRate) / math.log(2) / math.log(2)).toInt max ByteSizeOf.long

  def optimalNumberOfProbes(numberOfKeys: Int, numberOfBits: Long): Int =
    if (numberOfKeys <= 0 || numberOfBits <= 0)
      0
    else
      math.ceil(numberOfBits / numberOfKeys * math.log(2)).toInt

  def closeForMemory(state: BloomFilter.State): IO[Option[BloomFilter.MemoryBlock]] =
    BloomFilter.close(state) flatMap {
      closedBloomFilter =>
        closedBloomFilter map {
          closedBloomFilter =>
            SegmentBlock.createUnblockedReader(closedBloomFilter.bytes) flatMap {
              segmentBlock =>
                BloomFilter.read(
                  BloomFilter.Offset(0, closedBloomFilter.bytes.size),
                  segmentReader = segmentBlock
                ) map {
                  bloomFilterBlock =>
                    Some(
                      MemoryBlock(
                        bloomFilter = bloomFilterBlock,
                        bytes = closedBloomFilter.bytes.unslice())
                    )
                }
            }
        } getOrElse IO.none
    }

  def close(state: State): IO[Option[BloomFilter.State]] =
    if (state.bytes.isEmpty)
      IO.none
    else
      Block.create(
        headerSize = state.headerSize,
        bytes = state.bytes,
        compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
        blockName = blockName
      ) flatMap {
        compressedOrUncompressedBytes =>
          IO {
            state.bytes = compressedOrUncompressedBytes
            state.bytes addIntUnsigned state.numberOfBits
            state.bytes addIntUnsigned state.maxProbe
            if (state.bytes.currentWritePosition > state.headerSize)
              throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
            Some(state)
          }
      }

  def read(offset: Offset,
           segmentReader: BlockReader[SegmentBlock]): IO[BloomFilter] =
    for {
      blockHeader <- Block.readHeader(offset = offset, reader = segmentReader)
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
    keyValues.last.stats.segmentHasRemoveRange ||
      keyValues.last.stats.segmentBloomFilterSize <= 0 ||
      keyValues.last.bloomFilterConfig.falsePositiveRate <= 0.0 ||
      keyValues.size < keyValues.last.bloomFilterConfig.minimumNumberOfKeys

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
           compressions: UncompressedBlockInfo => Seq[CompressionInternal]): Option[BloomFilter.State] =
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

private[core] case class BloomFilter(offset: BloomFilter.Offset,
                                     maxProbe: Int,
                                     numberOfBits: Int,
                                     headerSize: Int,
                                     compressionInfo: Option[Block.CompressionInfo]) extends Block {

  override def updateOffset(start: Int, size: Int): Block =
    copy(offset = BloomFilter.Offset(start = start, size = size))
}
