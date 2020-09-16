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

package swaydb.core.segment.format.a.block.hashindex

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.compression.CompressionInternal
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.config.{IOAction, IOStrategy, RandomKeyIndex, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, Functions}

import scala.annotation.tailrec
import scala.beans.BeanProperty

/**
 * HashIndex.
 */
private[core] case object HashIndexBlock extends LazyLogging {

  val blockName = this.productPrefix

  object Config {
    val disabled =
      Config(
        maxProbe = -1,
        minimumNumberOfKeys = Int.MaxValue,
        allocateSpace = _ => Int.MinValue,
        minimumNumberOfHits = Int.MaxValue,
        format = HashIndexEntryFormat.Reference,
        ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.RandomKeyIndex): Config =
      config match {
        case swaydb.data.config.RandomKeyIndex.Disable =>
          Config(
            maxProbe = -1,
            minimumNumberOfKeys = Int.MaxValue,
            allocateSpace = _ => Int.MinValue,
            minimumNumberOfHits = Int.MaxValue,
            format = HashIndexEntryFormat.Reference,
            ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.RandomKeyIndex.Enable =>
          Config(
            maxProbe = enable.maxProbe,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            minimumNumberOfHits = enable.minimumNumberOfHits,
            format = HashIndexEntryFormat(enable.indexFormat),
            allocateSpace = Functions.safe(_.requiredSpace, enable.allocateSpace),
            ioStrategy = Functions.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
            compressions =
              Functions.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )
      }
  }

  case class Config(maxProbe: Int,
                    minimumNumberOfKeys: Int,
                    minimumNumberOfHits: Int,
                    format: HashIndexEntryFormat,
                    allocateSpace: RandomKeyIndex.RequiredSpace => Int,
                    ioStrategy: IOAction => IOStrategy,
                    compressions: UncompressedBlockInfo => Iterable[CompressionInternal])

  case class Offset(start: Int, size: Int) extends BlockOffset

  final class State(var hit: Int,
                    var miss: Int,
                    val format: HashIndexEntryFormat,
                    val minimumNumberOfKeys: Int,
                    val minimumNumberOfHits: Int,
                    val writeAbleLargestValueSize: Int,
                    @BeanProperty var minimumCRC: Long,
                    val maxProbe: Int,
                    var compressibleBytes: Slice[Byte],
                    val cacheableBytes: Slice[Byte],
                    var header: Slice[Byte],
                    val compressions: UncompressedBlockInfo => Iterable[CompressionInternal]) {

    def blockSize: Int =
      header.size + compressibleBytes.size

    def hasMinimumHits =
      hit >= minimumNumberOfHits

    val hashMaxOffset: Int =
      compressibleBytes.allocatedSize - writeAbleLargestValueSize
  }

  def init(sortedIndexState: SortedIndexBlock.State,
           hashIndexConfig: HashIndexBlock.Config): Option[HashIndexBlock.State] =
    if (sortedIndexState.uncompressedPrefixCount < hashIndexConfig.minimumNumberOfKeys) {
      None
    } else {
      val writeAbleLargestValueSize =
        hashIndexConfig.format.bytesToAllocatePerEntry(
          largestIndexOffset = sortedIndexState.secondaryIndexEntries.last.indexOffset,
          largestMergedKeySize = sortedIndexState.largestUncompressedMergedKeySize
        )

      val optimalBytes =
        optimalBytesRequired(
          keyCounts = sortedIndexState.uncompressedPrefixCount,
          minimumNumberOfKeys = hashIndexConfig.minimumNumberOfKeys,
          writeAbleLargestValueSize = writeAbleLargestValueSize,
          allocateSpace = hashIndexConfig.allocateSpace
        )

      //if the user allocated
      if (optimalBytes < ByteSizeOf.int) {
        None
      } else {
        val bytes = Slice.of[Byte](optimalBytes)
        val state =
          new HashIndexBlock.State(
            hit = 0,
            miss = sortedIndexState.prefixCompressedCount,
            format = hashIndexConfig.format,
            minimumNumberOfKeys = hashIndexConfig.minimumNumberOfKeys,
            minimumNumberOfHits = hashIndexConfig.minimumNumberOfHits,
            writeAbleLargestValueSize = writeAbleLargestValueSize,
            minimumCRC = CRC32.disabledCRC,
            maxProbe = hashIndexConfig.maxProbe,
            compressibleBytes = bytes,
            cacheableBytes = bytes,
            header = null,
            compressions = hashIndexConfig.compressions
          )

        Some(state)
      }
    }

  def optimalBytesRequired(keyCounts: Int,
                           minimumNumberOfKeys: Int,
                           writeAbleLargestValueSize: Int,
                           allocateSpace: RandomKeyIndex.RequiredSpace => Int): Int =
    if (keyCounts < minimumNumberOfKeys) {
      0
    } else {
      val requiredSpace =
      //+1 to skip left & right 0 start-end markers if it's not copiedIndex
      //+1 to for the last 1.byte entry so that next entry does overwrite previous writes tail 0's
      //the +1 does not need to be accounted in writeAbleLargestValueSize because these markers are just an indication of start and end index entry.
        keyCounts * (writeAbleLargestValueSize + 1)

      try
        allocateSpace(
          RandomKeyIndex.RequiredSpace(
            _requiredSpace = requiredSpace,
            _numberOfKeys = keyCounts
          )
        )
      catch {
        case exception: Exception =>
          logger.error(
            """Custom allocate space calculation for HashIndex returned failure.
              |Using the default requiredSpace instead. Please check your implementation to ensure it's not throwing exception.
            """.stripMargin, exception)
          requiredSpace
      }
    }

  def minimumCRCToWrite(state: State): Long =
  //CRC can be -1 when HashIndex is not fully copied.
    if (state.minimumCRC == CRC32.disabledCRC)
      0
    else
      state.minimumCRC

  def close(state: State): Option[State] =
    if (state.compressibleBytes.isEmpty || !state.hasMinimumHits)
      None
    else {
      val compressionResult =
        Block.compress(
          bytes = state.compressibleBytes,
          compressions = state.compressions(UncompressedBlockInfo(state.compressibleBytes.size)),
          blockName = blockName
        )

      val allocatedBytes = state.compressibleBytes.allocatedSize
      compressionResult.compressedBytes foreach (state.compressibleBytes = _)

      compressionResult.headerBytes add state.format.id
      compressionResult.headerBytes addInt allocatedBytes //allocated bytes
      compressionResult.headerBytes addUnsignedInt state.maxProbe
      compressionResult.headerBytes addUnsignedInt state.hit
      compressionResult.headerBytes addUnsignedInt state.miss
      compressionResult.headerBytes addUnsignedLong minimumCRCToWrite(state)
      compressionResult.headerBytes addUnsignedInt state.writeAbleLargestValueSize

      compressionResult.fixHeaderSize()

      state.header = compressionResult.headerBytes

      //      if (state.bytes.currentWritePosition > state.headerSize)
      //        throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition}")
      Some(state)
    }

  def unblockedReader(closedState: HashIndexBlock.State): UnblockedReader[HashIndexBlock.Offset, HashIndexBlock] = {
    val block =
      HashIndexBlock(
        offset = HashIndexBlock.Offset(0, closedState.cacheableBytes.size),
        compressionInfo = None,
        maxProbe = closedState.maxProbe,
        format = closedState.format,
        minimumCRC = minimumCRCToWrite(closedState),
        hit = closedState.hit,
        miss = closedState.miss,
        writeAbleLargestValueSize = closedState.writeAbleLargestValueSize,
        headerSize = 0,
        allocatedBytes = closedState.cacheableBytes.allocatedSize
      )

    UnblockedReader(
      block = block,
      bytes = closedState.cacheableBytes.close()
    )
  }

  def read(header: Block.Header[HashIndexBlock.Offset]): HashIndexBlock = {
    val formatId = header.headerReader.get()
    val format = HashIndexEntryFormat.formats.find(_.id == formatId) getOrElse IO.throws(s"Invalid HashIndex formatId: $formatId")
    val allocatedBytes = header.headerReader.readInt()
    val maxProbe = header.headerReader.readUnsignedInt()
    val hit = header.headerReader.readUnsignedInt()
    val miss = header.headerReader.readUnsignedInt()
    val minimumCRC = header.headerReader.readUnsignedLong()
    val largestValueSize = header.headerReader.readUnsignedInt()

    HashIndexBlock(
      offset = header.offset,
      compressionInfo = header.compressionInfo,
      maxProbe = maxProbe,
      format = format,
      minimumCRC = minimumCRC,
      hit = hit,
      miss = miss,
      writeAbleLargestValueSize = largestValueSize,
      headerSize = header.headerSize,
      allocatedBytes = allocatedBytes
    )
  }

  private def adjustHash(hash: Int,
                         hashMaxOffset: Int) =
    Math.abs(hash) % hashMaxOffset

  def write(entry: SortedIndexBlock.SecondaryIndexEntry,
            state: HashIndexBlock.State): Boolean =
    state.format match {
      case HashIndexEntryFormat.Reference =>
        HashIndexBlock.writeReference(
          indexOffset = entry.indexOffset,
          comparableKey = entry.comparableKey,
          mergedKey = entry.mergedKey,
          keyType = entry.keyType,
          state = state
        )

      case HashIndexEntryFormat.CopyKey =>
        HashIndexBlock.writeCopy(
          indexOffset = entry.indexOffset,
          comparableKey = entry.comparableKey,
          mergedKey = entry.mergedKey,
          keyType = entry.keyType,
          state = state
        )
    }

  /**
   * Mutates the slice and adds writes the indexOffset to it's hash index.
   */
  def writeReference(indexOffset: Int,
                     comparableKey: Slice[Byte],
                     mergedKey: Slice[Byte],
                     keyType: Byte,
                     state: State): Boolean = {

    val requiredSpace =
      HashIndexEntryFormat.Reference.bytesToAllocatePerEntry(
        largestIndexOffset = indexOffset,
        largestMergedKeySize = mergedKey.size
      )

    val hash = comparableKey.hashCode()
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    @tailrec
    def doWrite(probe: Int): Boolean =
      if (probe >= state.maxProbe) {
        //println(s"Key: ${key.readInt()}: write index: miss probe: $probe, requiredSpace: $requiredSpace")
        state.miss += 1
        false
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            hashMaxOffset = state.hashMaxOffset
          )

        val existing = state.compressibleBytes.take(hashIndex, requiredSpace + 2) //+1 to reserve left 0 byte another +1 not overwrite next 0.
        if (existing.forall(_ == 0)) {
          state.compressibleBytes moveWritePosition (hashIndex + 1)

          HashIndexEntryFormat.Reference.write(
            indexOffset = indexOffset,
            mergedKey = mergedKey,
            keyType = keyType,
            bytes = state.compressibleBytes
          )
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, requiredSpace: $requiredSpace, value: ${state.bytes.take(hashIndex, state.bytes.currentWritePosition - hashIndex)} = success")
          state.hit += 1
          true
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, requiredSpace: $requiredSpace = failure")
          doWrite(probe = probe + 1)
        }
      }

    if (state.compressibleBytes.allocatedSize == 0)
      false
    else
      doWrite(0)
  }

  /**
   * Finds a key in the hash index.
   */
  private[block] def searchReference(key: Slice[Byte],
                                     comparableKey: Slice[Byte],
                                     hashIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                                     sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                     valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]]): Persistent.PartialOption = {

    val hash = comparableKey.hashCode()
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val block = hashIndexReader.block

    @tailrec
    def doFind(probe: Int): Persistent.PartialOption =
      if (probe >= block.maxProbe) {
        Persistent.Partial.Null
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            hashMaxOffset = block.hashMaxOffset
          )

        val possibleValueBytes =
          hashIndexReader
            .moveTo(hashIndex)
            .read(block.bytesToReadPerIndex)

        //println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe. sortedIndex bytes: $possibleValueBytes")
        if (possibleValueBytes.isEmpty || possibleValueBytes.head != Bytes.zero) {
          //println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe = failure - invalid start offset.")
          doFind(probe + 1)
        } else {
          val entry = possibleValueBytes.dropHead()
          //println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe, sortedIndex: ${possibleOffset - 1} = reading now!")
          val partialKeyValueOrNull =
            block.format.readOrNull(
              entry = entry,
              hashIndexReader = hashIndexReader,
              sortedIndex = sortedIndexReader,
              valuesOrNull = valuesReaderOrNull
            )

          if (partialKeyValueOrNull == null) {
            //println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe, sortedIndex: ${possibleOffset - 1}, possibleValue: $possibleOffset, containsZero: ${entry.take(bytesRead).exists(_ == 0)} = failed")
            doFind(probe + 1)
          } else if (partialKeyValueOrNull matchForHashIndex key) {
            partialKeyValueOrNull
          } else {
            doFind(probe + 1)
          }
        }
      }

    doFind(probe = 0)
  }

  /**
   * Writes full copy of the index entry within HashIndex.
   */
  def writeCopy(indexOffset: Int,
                comparableKey: Slice[Byte],
                mergedKey: Slice[Byte],
                keyType: Byte,
                state: State): Boolean = {

    val hash = comparableKey.hashCode()
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val requiredSpace =
      state.format.bytesToAllocatePerEntry(
        largestIndexOffset = indexOffset,
        largestMergedKeySize = mergedKey.size
      )

    @tailrec
    def doWrite(probe: Int): Boolean =
      if (probe >= state.maxProbe) {
        //println(s"Key: ${key.readInt()}: write index: miss probe: $probe, requiredSpace: $requiredSpace")
        state.miss += 1
        false
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            hashMaxOffset = state.hashMaxOffset
          )

        //+1 for cases where the last byte is zero.
        val existing = state.compressibleBytes.take(hashIndex, requiredSpace + 1)

        if (existing.forall(_ == 0)) {
          state.compressibleBytes moveWritePosition hashIndex

          val crc =
            HashIndexEntryFormat.CopyKey.write(
              indexOffset = indexOffset,
              mergedKey = mergedKey,
              keyType = keyType,
              bytes = state.compressibleBytes
            )

          if (state.minimumCRC == CRC32.disabledCRC)
            state setMinimumCRC crc
          else
            state setMinimumCRC (crc min state.minimumCRC)

          state.hit += 1
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, requiredSpace: $requiredSpace, value: ${state.bytes.take(hashIndex, state.bytes.currentWritePosition - hashIndex)}, crc = $crc = success")
          true
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, requiredSpace: $requiredSpace = failure")
          doWrite(probe = probe + 1)
        }
      }

    if (state.compressibleBytes.allocatedSize == 0)
      false
    else
      doWrite(0)
  }

  private[block] def searchCopy(key: Slice[Byte],
                                comparableKey: Slice[Byte],
                                hasIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                                sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]]): Persistent.PartialOption = {

    val hash = comparableKey.hashCode()
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val block = hasIndexReader.block

    @tailrec
    def doFind(probe: Int): Persistent.PartialOption =
      if (probe >= block.maxProbe) {
        Persistent.Partial.Null
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            hashMaxOffset = block.hashMaxOffset
          ) //remove headerSize since the blockReader points to the hashIndex's start offset.

        val entry =
          hasIndexReader
            .moveTo(hashIndex)
            .read(block.writeAbleLargestValueSize)

        //println(s"Key: ${key.readInt()}: read hashIndex: ${hashIndex + block.headerSize} probe: $probe. entry: $entry")
        if (entry.isEmpty || entry.size == 1) {
          //println(s"Key: ${key.readInt()}: read hashIndex: ${hashIndex + block.headerSize} probe: $probe = failure - invalid start offset.")
          doFind(probe + 1)
        } else {
          val partialKeyValueOrNull: Persistent.Partial =
            block.format.readOrNull(
              entry = entry,
              hashIndexReader = hasIndexReader,
              sortedIndex = sortedIndexReader,
              valuesOrNull = valuesReaderOrNull
            )

          if (partialKeyValueOrNull == null)
            doFind(probe + 1)
          else if (partialKeyValueOrNull matchForHashIndex key)
            partialKeyValueOrNull
          else
            doFind(probe + 1)
        }
      }

    doFind(probe = 0)
  }

  def search(key: Slice[Byte],
             hashIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]]): Persistent.PartialOption =
    if (hashIndexReader.block.format.isCopy)
      searchCopy(
        key = key,
        comparableKey = keyOrder.comparableKey(key),
        hasIndexReader = hashIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    else
      searchReference(
        key = key,
        comparableKey = keyOrder.comparableKey(key),
        hashIndexReader = hashIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

  implicit object HashIndexBlockOps extends BlockOps[HashIndexBlock.Offset, HashIndexBlock] {
    override def updateBlockOffset(block: HashIndexBlock, start: Int, size: Int): HashIndexBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      HashIndexBlock.Offset(start = start, size = size)

    override def readBlock(header: Block.Header[Offset]): HashIndexBlock =
      HashIndexBlock.read(header)
  }
}

private[core] case class HashIndexBlock(offset: HashIndexBlock.Offset,
                                        compressionInfo: Option[Block.CompressionInfo],
                                        maxProbe: Int,
                                        format: HashIndexEntryFormat,
                                        minimumCRC: Long,
                                        hit: Int,
                                        miss: Int,
                                        writeAbleLargestValueSize: Int,
                                        headerSize: Int,
                                        allocatedBytes: Int) extends Block[HashIndexBlock.Offset] {
  val bytesToReadPerIndex = writeAbleLargestValueSize + 1 //+1 to read header/marker 0 byte.

  val isCompressed = compressionInfo.isDefined

  val hashMaxOffset: Int =
    allocatedBytes - writeAbleLargestValueSize

  def isPerfect =
    miss == 0
}
