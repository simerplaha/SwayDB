/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.block.bloomfilter

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.CompressionInternal
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.{Block, BlockOffset, BlockOps}
import swaydb.core.util.MurmurHash3Generic
import swaydb.effect.IOStrategy
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.slice.Slice
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.{ByteSizeOf, FunctionSafe}

private[core] case object BloomFilterBlock extends LazyLogging {

  val blockName = this.productPrefix

  object Config {
    val disabled =
      Config(
        falsePositiveRate = 0.0,
        minimumNumberOfKeys = Int.MaxValue,
        optimalMaxProbe = probe => probe,
        ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.BloomFilter): Config =
      config match {
        case swaydb.data.config.BloomFilter.Off =>
          Config(
            falsePositiveRate = 0.0,
            minimumNumberOfKeys = Int.MaxValue,
            optimalMaxProbe = _ => 0,
            ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.BloomFilter.On =>
          Config(
            falsePositiveRate = enable.falsePositiveRate,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            optimalMaxProbe = FunctionSafe.safe(probe => probe, enable.updateMaxProbe),
            ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
            compressions =
              FunctionSafe.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )
      }
  }

  case class Config(falsePositiveRate: Double,
                    minimumNumberOfKeys: Int,
                    optimalMaxProbe: Int => Int,
                    ioStrategy: IOAction => IOStrategy,
                    compressions: UncompressedBlockInfo => Iterable[CompressionInternal])

  case class Offset(start: Int, size: Int) extends BlockOffset

  class State(val numberOfBits: Int,
              val maxProbe: Int,
              var compressibleBytes: Slice[Byte],
              val cacheableBytes: Slice[Byte],
              var header: Slice[Byte],
              val compressions: UncompressedBlockInfo => Iterable[CompressionInternal]) {

    def blockSize: Int =
      header.size + compressibleBytes.size

    def blockBytes: Slice[Byte] =
      header ++ compressibleBytes

    def written =
      compressibleBytes.size

    override def hashCode(): Int =
      compressibleBytes.hashCode()
  }

  def optimalSize(numberOfKeys: Int,
                  falsePositiveRate: Double,
                  hasCompression: Boolean,
                  updateMaxProbe: Int => Int,
                  minimumNumberOfKeys: Int): Int = {
    if (falsePositiveRate <= 0.0 || numberOfKeys < minimumNumberOfKeys || numberOfKeys <= 0) {
      0
    } else {
      optimalNumberOfBits(numberOfKeys, falsePositiveRate)
      //      val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
      //      val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits, updateMaxProbe)
      //
      //      val numberOfBitsSize = Bytes.sizeOfUnsignedInt(numberOfBits)
      //      val maxProbeSize = Bytes.sizeOfUnsignedInt(maxProbe)
      //
      ////      val headerByteSize =
      ////        Block.headerSize(hasCompression) +
      ////          numberOfBitsSize +
      ////          maxProbeSize
      //
      //      Bytes.sizeOfUnsignedInt(headerByteSize) +
      //        headerByteSize +
      //        numberOfBits
    }
  }

  private def apply(numberOfKeys: Int,
                    falsePositiveRate: Double,
                    updateMaxProbe: Int => Int,
                    compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): BloomFilterBlock.State = {
    val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
    val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits, updateMaxProbe) max 1

    val hasCompression = compressions(UncompressedBlockInfo(numberOfBits)).nonEmpty

    val bytes = Slice.of[Byte](numberOfBits)

    new BloomFilterBlock.State(
      numberOfBits = numberOfBits,
      maxProbe = maxProbe,
      compressibleBytes = bytes,
      cacheableBytes = bytes,
      header = Slice.emptyBytes,
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

  def close(state: State): Option[BloomFilterBlock.State] =
    if (state.compressibleBytes.isEmpty) {
      None
    } else {
      val compressionResult =
        Block.compress(
          bytes = state.compressibleBytes,
          compressions = state.compressions(UncompressedBlockInfo(state.compressibleBytes.size)),
          blockName = blockName
        )

      compressionResult.compressedBytes foreach (state.compressibleBytes = _)

      compressionResult.headerBytes addUnsignedInt state.numberOfBits
      compressionResult.headerBytes addUnsignedInt state.maxProbe

      //      compressionResult.headerBytes moveWritePosition state.headerSize

      //header
      compressionResult.fixHeaderSize()

      state.header = compressionResult.headerBytes
      //      if (state.bytes.currentWritePosition > state.headerSize) {
      //        throw IO.throwable(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
      //      } else {
      //        logger.trace(s"BloomFilter stats: allocatedSpace: ${state.numberOfBits}. actualSpace: ${state.bytes.size}. maxProbe: ${state.maxProbe}")
      //        Some(state)
      //      }
      logger.trace(s"BloomFilter stats: allocatedSpace: ${state.numberOfBits}. actualSpace: ${state.compressibleBytes.size}. maxProbe: ${state.maxProbe}")
      Some(state)
    }

  def unblockedReader(closedState: BloomFilterBlock.State): UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock] = {
    val block =
      BloomFilterBlock(
        offset = BloomFilterBlock.Offset(0, closedState.cacheableBytes.size),
        maxProbe = closedState.maxProbe,
        numberOfBits = closedState.numberOfBits,
        headerSize = 0,
        compressionInfo = None
      )

    UnblockedReader(
      block = block,
      bytes = closedState.cacheableBytes.close()
    )
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

  /**
   * Initialise bloomFilter if key-values do no contain remove range.
   */
  def init(numberOfKeys: Int,
           falsePositiveRate: Double,
           updateMaxProbe: Int => Int,
           compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): Option[BloomFilterBlock.State] =
    if (numberOfKeys <= 0 || falsePositiveRate <= 0.0 || falsePositiveRate >= 1)
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

  def add(comparableKey: Slice[Byte],
          state: BloomFilterBlock.State): Unit = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(comparableKey, 0, comparableKey.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var probe = 0
    while (probe < state.maxProbe) {
      val computedHash = hash1 + probe * hash2
      val hashIndex = (computedHash & Long.MaxValue) % state.numberOfBits
      val offset = ((hashIndex >>> 6) * 8L).toInt
      val long = state.compressibleBytes.take(offset, ByteSizeOf.long).readLong()
      if ((long & (1L << hashIndex)) == 0) {
        state.compressibleBytes moveWritePosition offset
        state.compressibleBytes addLong (long | (1L << hashIndex))
      }
      probe += 1
    }
  }

  def mightContain(comparableKey: Slice[Byte],
                   reader: UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]): Boolean = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(comparableKey, 0, comparableKey.size, 0)
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

