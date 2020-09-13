/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.bloomfilter

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.CompressionInternal
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{Block, BlockOffset, BlockOps}
import swaydb.core.util.MurmurHash3Generic
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
        ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.MightContainIndex): Config =
      config match {
        case swaydb.data.config.MightContainIndex.Disable =>
          Config(
            falsePositiveRate = 0.0,
            minimumNumberOfKeys = Int.MaxValue,
            optimalMaxProbe = _ => 0,
            ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.MightContainIndex.Enable =>
          Config(
            falsePositiveRate = enable.falsePositiveRate,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            optimalMaxProbe = Functions.safe(probe => probe, enable.updateMaxProbe),
            ioStrategy = Functions.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
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

    val bytes = Slice.create[Byte](numberOfBits)

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

