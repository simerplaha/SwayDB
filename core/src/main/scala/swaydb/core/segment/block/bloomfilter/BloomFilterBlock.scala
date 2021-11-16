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
import swaydb.core.segment.block._
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.util.{Bytes, MurmurHash3Generic}
import swaydb.config.UncompressedBlockInfo
import swaydb.slice.Slice
import swaydb.utils.ByteSizeOf

private[core] case object BloomFilterBlock extends LazyLogging {

  val blockName = this.productPrefix

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
                    compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): BloomFilterBlockState = {
    val numberOfBits = optimalNumberOfBits(numberOfKeys, falsePositiveRate)
    val maxProbe = optimalNumberOfProbes(numberOfKeys, numberOfBits, updateMaxProbe) max 1

    val hasCompression = compressions(UncompressedBlockInfo(numberOfBits)).nonEmpty

    val bytes = Slice.of[Byte](numberOfBits)

    new BloomFilterBlockState(
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

  def close(state: BloomFilterBlockState): Option[BloomFilterBlockState] =
    if (state.compressibleBytes.isEmpty) {
      None
    } else {
      val headerSize =
        Bytes.sizeOfUnsignedInt(state.numberOfBits) + // numberOfBits
          Bytes.sizeOfUnsignedInt(state.maxProbe) // maxProbe

      val compressionResult =
        Block.compress(
          bytes = state.compressibleBytes,
          dataBlocksHeaderByteSize = headerSize,
          compressions = state.compressions(UncompressedBlockInfo(state.compressibleBytes.size)),
          blockName = blockName
        )

      compressionResult.compressedBytes foreachC (slice => state.compressibleBytes = slice.asMut())

      compressionResult.headerBytes addUnsignedInt state.numberOfBits
      compressionResult.headerBytes addUnsignedInt state.maxProbe

      //      compressionResult.headerBytes moveWritePosition state.headerSize

      assert(compressionResult.headerBytes.isOriginalFullSlice)
      state.header = compressionResult.headerBytes
      //      if (state.bytes.currentWritePosition > state.headerSize) {
      //        throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
      //      } else {
      //        logger.trace(s"BloomFilter stats: allocatedSpace: ${state.numberOfBits}. actualSpace: ${state.bytes.size}. maxProbe: ${state.maxProbe}")
      //        Some(state)
      //      }
      logger.trace(s"BloomFilter stats: allocatedSpace: ${state.numberOfBits}. actualSpace: ${state.compressibleBytes.size}. maxProbe: ${state.maxProbe}")
      Some(state)
    }

  def unblockedReader(closedState: BloomFilterBlockState): UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock] = {
    val block =
      BloomFilterBlock(
        offset = BloomFilterBlockOffset(0, closedState.cacheableBytes.size),
        maxProbe = closedState.maxProbe,
        numberOfBits = closedState.numberOfBits,
        headerSize = 0,
        compressionInfo = BlockCompressionInfo.Null
      )

    UnblockedReader(
      block = block,
      bytes = closedState.cacheableBytes.close()
    )
  }

  def read(header: BlockHeader[BloomFilterBlockOffset]): BloomFilterBlock = {
    val numberOfBits = header.headerReader.readUnsignedInt()
    val maxProbe = header.headerReader.readUnsignedInt()

    BloomFilterBlock(
      offset = header.offset,
      maxProbe = maxProbe,
      numberOfBits = numberOfBits,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )
  }

  /**
   * Initialise bloomFilter if key-values do no contain remove range.
   */
  def init(numberOfKeys: Int,
           falsePositiveRate: Double,
           updateMaxProbe: Int => Int,
           compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): Option[BloomFilterBlockState] =
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
          state: BloomFilterBlockState): Unit = {
    val hash = MurmurHash3Generic.murmurhash3_x64_64(comparableKey, 0, comparableKey.size, 0)
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    var probe = 0
    while (probe < state.maxProbe) {
      val computedHash = hash1 + probe * hash2
      val hashIndex = (computedHash & Long.MaxValue) % state.numberOfBits
      val offset = ((hashIndex >>> 6) * 8L).toInt

      val existing = state.compressibleBytes.take(offset, ByteSizeOf.long)

      val long =
        if (existing.isEmpty)
          0
        else
          existing.readLong()

      if ((long & (1L << hashIndex)) == 0) {
        state.compressibleBytes moveWritePosition offset
        state.compressibleBytes addLong (long | (1L << hashIndex))
      }
      probe += 1
    }
  }

  def mightContain(comparableKey: Slice[Byte],
                   reader: UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]): Boolean = {
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

}

private[core] case class BloomFilterBlock(offset: BloomFilterBlockOffset,
                                          maxProbe: Int,
                                          numberOfBits: Int,
                                          headerSize: Int,
                                          compressionInfo: BlockCompressionInfoOption) extends Block[BloomFilterBlockOffset]

