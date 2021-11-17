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

package swaydb.core.segment.block.hashindex

import com.typesafe.scalalogging.LazyLogging
import swaydb.config.{HashIndex, UncompressedBlockInfo}
import swaydb.core.segment.block._
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset, SortedIndexBlockSecondaryIndexEntry, SortedIndexBlockState}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}
import swaydb.core.segment.data.Persistent
import swaydb.core.util.{Bytes, CRC32}
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.utils.ByteSizeOf

import scala.annotation.tailrec

/**
 * HashIndex.
 */
private[core] case object HashIndexBlock extends LazyLogging {

  val blockName = this.productPrefix

  def init(sortedIndexState: SortedIndexBlockState,
           hashIndexConfig: HashIndexBlockConfig): Option[HashIndexBlockState] =
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
          new HashIndexBlockState(
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
                           allocateSpace: HashIndex.RequiredSpace => Int): Int =
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
          HashIndex.RequiredSpace(
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

  def close(state: HashIndexBlockState): Option[HashIndexBlockState] =
    if (state.compressibleBytes.isEmpty || !state.hasMinimumHits)
      None
    else {
      val headerSize: Int =
        ByteSizeOf.byte + //formatId
          ByteSizeOf.int + //allocatedBytes
          Bytes.sizeOfUnsignedInt(state.maxProbe) +
          Bytes.sizeOfUnsignedInt(state.hit) +
          Bytes.sizeOfUnsignedInt(state.miss) +
          Bytes.sizeOfUnsignedLong(state.minimumCRCToWrite()) +
          Bytes.sizeOfUnsignedInt(state.writeAbleLargestValueSize)

      val compressionResult =
        Block.compress(
          bytes = state.compressibleBytes,
          dataBlocksHeaderByteSize = headerSize,
          compressions = state.compressions(UncompressedBlockInfo(state.compressibleBytes.size)),
          blockName = blockName
        )

      val allocatedBytes = state.compressibleBytes.allocatedSize
      compressionResult.compressedBytes foreachC (slice => state.compressibleBytes = slice.asMut())

      compressionResult.headerBytes add state.format.id
      compressionResult.headerBytes addInt allocatedBytes //allocated bytes
      compressionResult.headerBytes addUnsignedInt state.maxProbe
      compressionResult.headerBytes addUnsignedInt state.hit
      compressionResult.headerBytes addUnsignedInt state.miss
      compressionResult.headerBytes addUnsignedLong state.minimumCRCToWrite()
      compressionResult.headerBytes addUnsignedInt state.writeAbleLargestValueSize

      assert(compressionResult.headerBytes.isOriginalFullSlice)
      state.header = compressionResult.headerBytes

      //      if (state.bytes.currentWritePosition > state.headerSize)
      //        throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition}")
      Some(state)
    }

  def unblockedReader(closedState: HashIndexBlockState): UnblockedReader[HashIndexBlockOffset, HashIndexBlock] = {
    val block =
      HashIndexBlock(
        offset = HashIndexBlockOffset(0, closedState.cacheableBytes.size),
        compressionInfo = BlockCompressionInfo.Null,
        maxProbe = closedState.maxProbe,
        format = closedState.format,
        minimumCRC = closedState.minimumCRCToWrite(),
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

  def read(header: BlockHeader[HashIndexBlockOffset]): HashIndexBlock = {
    val formatId = header.headerReader.get()
    val format = HashIndexEntryFormat.formats.find(_.id == formatId) getOrElse (throw new Exception(s"Invalid HashIndex formatId: $formatId"))
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

  def write(entry: SortedIndexBlockSecondaryIndexEntry,
            state: HashIndexBlockState): Boolean =
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
                     state: HashIndexBlockState): Boolean = {

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
                                     hashIndexReader: UnblockedReader[HashIndexBlockOffset, HashIndexBlock],
                                     sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                                     valuesReaderOrNull: UnblockedReader[ValuesBlockOffset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]]): Persistent.PartialOption = {

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
                state: HashIndexBlockState): Boolean = {

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
                                hasIndexReader: UnblockedReader[HashIndexBlockOffset, HashIndexBlock],
                                sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                                valuesReaderOrNull: UnblockedReader[ValuesBlockOffset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]]): Persistent.PartialOption = {

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
             hashIndexReader: UnblockedReader[HashIndexBlockOffset, HashIndexBlock],
             sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
             valuesReaderOrNull: UnblockedReader[ValuesBlockOffset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]]): Persistent.PartialOption =
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

}

private[core] case class HashIndexBlock(offset: HashIndexBlockOffset,
                                        compressionInfo: BlockCompressionInfoOption,
                                        maxProbe: Int,
                                        format: HashIndexEntryFormat,
                                        minimumCRC: Long,
                                        hit: Int,
                                        miss: Int,
                                        writeAbleLargestValueSize: Int,
                                        headerSize: Int,
                                        allocatedBytes: Int) extends Block[HashIndexBlockOffset] {
  val bytesToReadPerIndex = writeAbleLargestValueSize + 1 //+1 to read header/marker 0 byte.

  val isCompressed = compressionInfo.isSomeS

  val hashMaxOffset: Int =
    allocatedBytes - writeAbleLargestValueSize

  def isPerfect =
    miss == 0
}
