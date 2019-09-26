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
import swaydb.core.data.Transient
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.{Bytes, MurmurHash3Generic}
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, Functions}

private[core] object BloomFilterBlock extends LazyLogging {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {
    val disabled =
      Config(
        falsePositiveRate = 0.0,
        minimumNumberOfKeys = Int.MaxValue,
        optimalMaxProbe = probe => probe,
        blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.MightContainIndex): Config =
      config match {
        case swaydb.data.config.MightContainIndex.Disable =>
          Config(
            falsePositiveRate = 0.0,
            minimumNumberOfKeys = Int.MaxValue,
            optimalMaxProbe = _ => 0,
            blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.MightContainIndex.Enable =>
          Config(
            falsePositiveRate = enable.falsePositiveRate,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            optimalMaxProbe = Functions.safe(probe => probe, enable.updateMaxProbe),
            blockIO = Functions.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
            compressions =
              Functions.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )
      }
  }

  case class Config(falsePositiveRate: Double,
                    minimumNumberOfKeys: Int,
                    optimalMaxProbe: Int => Int,
                    blockIO: IOAction => IOStrategy,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

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
                  updateMaxProbe: Int => Int,
                  minimumNumberOfKeys: Int): Int = {
    if (falsePositiveRate <= 0.0 || numberOfKeys < minimumNumberOfKeys || numberOfKeys <= 0) {
      0
    } else {
      val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
      val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits, updateMaxProbe)

      val numberOfBitsSize = Bytes.sizeOfUnsignedInt(numberOfBits)
      val maxProbeSize = Bytes.sizeOfUnsignedInt(maxProbe)

      val headerByteSize =
        Block.headerSize(hasCompression) +
          numberOfBitsSize +
          maxProbeSize

      Bytes.sizeOfUnsignedInt(headerByteSize) +
        headerByteSize +
        numberOfBits
    }
  }

  private def apply(numberOfKeys: Int,
                    falsePositiveRate: Double,
                    updateMaxProbe: Int => Int,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal]): BloomFilterBlock.State = {
    val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
    val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits, updateMaxProbe) max 1

    val numberOfBitsSize = Bytes.sizeOfUnsignedInt(numberOfBits)
    val maxProbeSize = Bytes.sizeOfUnsignedInt(maxProbe)

    val hasCompression = compressions(UncompressedBlockInfo(numberOfBits)).nonEmpty

    val headerBytesSize =
      Block.headerSize(hasCompression) +
        numberOfBitsSize +
        maxProbeSize

    val headerSize =
      Bytes.sizeOfUnsignedInt(headerBytesSize) +
        headerBytesSize

    val bytes = Slice.create[Byte](headerSize + numberOfBits)

    BloomFilterBlock.State(
      startOffset = headerSize,
      numberOfBits = numberOfBits,
      headerSize = headerSize,
      maxProbe = maxProbe,
      _bytes = bytes,
      compressions =
        if (hasCompression)
          compressions
        else
          _ => Seq.empty
    )
  }

  def optimalNumberOfBits(numberOfKeys: Int, falsePositiveRate: Double): Int =
    if (numberOfKeys <= 0 || falsePositiveRate <= 0.0)
      0
    else
      math.ceil(-1 * numberOfKeys * math.log(falsePositiveRate) / math.log(2) / math.log(2)).toInt max ByteSizeOf.long

  def optimalNumberOfProbes(numberOfKeys: Int, numberOfBits: Long,
                            update: Int => Int): Int = {
    val optimal =
      if (numberOfKeys <= 0 || numberOfBits <= 0)
        0
      else
        math.ceil(numberOfBits / numberOfKeys * math.log(2)).toInt

    update(optimal)
  }

  def closeForMemory(state: BloomFilterBlock.State): Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]] =
    BloomFilterBlock.close(state) map {
      closedBloomFilter =>
        Block.unblock[BloomFilterBlock.Offset, BloomFilterBlock](closedBloomFilter.bytes.unslice())(BloomFilterBlockOps)
    }

  def close(state: State): Option[BloomFilterBlock.State] =
    if (state.bytes.isEmpty) {
      None
    } else {
      val compressedOrUncompressedBytes =
        Block.block(
          headerSize = state.headerSize,
          bytes = state.bytes,
          compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
          blockName = blockName
        )

      state.bytes = compressedOrUncompressedBytes
      state.bytes addUnsignedInt state.numberOfBits
      state.bytes addUnsignedInt state.maxProbe
      if (state.bytes.currentWritePosition > state.headerSize) {
        throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
      } else {
        logger.trace(s"BloomFilter stats: allocatedSpace: ${state.numberOfBits}. actualSpace: ${state.bytes.size}. maxProbe: ${state.maxProbe}")
        Some(state)
      }
    }

  def read(header: Block.Header[BloomFilterBlock.Offset]): BloomFilterBlock = {
    val numberOfBits = header.headerReader.readUnsignedInt()
    val maxProbe = header.headerReader.readUnsignedInt()
    BloomFilterBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      maxProbe = maxProbe,
      numberOfBits = numberOfBits,
      compressionInfo = header.compressionInfo
    )
  }

  def shouldNotCreateBloomFilter(keyValues: Iterable[Transient]): Boolean =
    keyValues.last.stats.segmentHasRemoveRange ||
      keyValues.last.stats.segmentBloomFilterSize <= 0 ||
      keyValues.last.bloomFilterConfig.falsePositiveRate <= 0.0 ||
      keyValues.last.bloomFilterConfig.falsePositiveRate >= 1 ||
      keyValues.size < keyValues.last.bloomFilterConfig.minimumNumberOfKeys

  def shouldCreateBloomFilter(keyValues: Iterable[Transient]): Boolean =
    !shouldNotCreateBloomFilter(keyValues)

  def init(keyValues: Iterable[Transient]): Option[BloomFilterBlock.State] =
    if (shouldCreateBloomFilter(keyValues))
      init(
        numberOfKeys = keyValues.last.stats.linkedPosition,
        falsePositiveRate = keyValues.last.bloomFilterConfig.falsePositiveRate,
        compressions = keyValues.last.bloomFilterConfig.compressions,
        updateMaxProbe = keyValues.last.bloomFilterConfig.optimalMaxProbe
      )
    else
      None

  /**
   * Initialise bloomFilter if key-values do no contain remove range.
   */
  def init(numberOfKeys: Int,
           falsePositiveRate: Double,
           updateMaxProbe: Int => Int,
           compressions: UncompressedBlockInfo => Seq[CompressionInternal]): Option[BloomFilterBlock.State] =
    if (numberOfKeys <= 0 || falsePositiveRate <= 0.0)
      None
    else
      Some(
        BloomFilterBlock(
          numberOfKeys = numberOfKeys,
          falsePositiveRate = falsePositiveRate,
          updateMaxProbe = updateMaxProbe,
          compressions = compressions
        )
      )

  def add(key: Slice[Byte],
          state: BloomFilterBlock.State): Unit = {
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
                   reader: UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]): Boolean = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(key, 0, key.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var probe = 0
    while (probe < reader.block.maxProbe) {
      val computedHash = hash1 + probe * hash2
      val hashIndex = (computedHash & Long.MaxValue) % reader.block.numberOfBits
      val position = ((hashIndex >>> 6) * 8L).toInt
      //hash for invalid key could result in a position that is outside of the actual index bounds which
      //means that key does not exist.
      if (reader.block.offset.size - position < ByteSizeOf.long) {
        return false
      } else {
        val index =
          reader
            .moveTo(position)
            .readLong()

        if ((index & (1L << hashIndex)) == 0)
          return false
      }
      probe += 1
    }
    true
  }

  implicit object BloomFilterBlockOps extends BlockOps[BloomFilterBlock.Offset, BloomFilterBlock] {
    override def updateBlockOffset(block: BloomFilterBlock, start: Int, size: Int): BloomFilterBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      BloomFilterBlock.Offset(start = start, size = size)

    override def readBlock(header: Block.Header[Offset]): BloomFilterBlock =
      BloomFilterBlock.read(header)
  }
}

private[core] case class BloomFilterBlock(offset: BloomFilterBlock.Offset,
                                          maxProbe: Int,
                                          numberOfBits: Int,
                                          headerSize: Int,
                                          compressionInfo: Option[Block.CompressionInfo]) extends Block[BloomFilterBlock.Offset]

