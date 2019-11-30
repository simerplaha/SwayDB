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

package swaydb.core.segment.format.a.block.hashindex

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.compression.CompressionInternal
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.segment.format.a.block.KeyMatcher.Result
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.config.{IOAction, IOStrategy, RandomKeyIndex, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Maybe._
import swaydb.data.util.{ByteSizeOf, Functions}

import scala.annotation.tailrec
import scala.beans.BeanProperty

/**
 * HashIndex.
 */
private[core] object HashIndexBlock extends LazyLogging {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {
    val disabled =
      Config(
        maxProbe = -1,
        minimumNumberOfKeys = Int.MaxValue,
        allocateSpace = _ => Int.MinValue,
        minimumNumberOfHits = Int.MaxValue,
        format = HashIndexEntryFormat.ReferenceIndex,
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
            format = HashIndexEntryFormat.ReferenceIndex,
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
            ioStrategy = Functions.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
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
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends BlockOffset

  final case class State(var hit: Int,
                         var miss: Int,
                         format: HashIndexEntryFormat,
                         minimumNumberOfKeys: Int,
                         minimumNumberOfHits: Int,
                         writeAbleLargestValueSize: Int,
                         @BeanProperty var minimumCRC: Long,
                         headerSize: Int,
                         maxProbe: Int,
                         var _bytes: Slice[Byte],
                         compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {

    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes

    def hasMinimumHits =
      hit >= minimumNumberOfHits
  }

  def init(keyValues: Iterable[Transient]): Option[HashIndexBlock.State] =
    if (keyValues.size < keyValues.last.hashIndexConfig.minimumNumberOfKeys || keyValues.last.stats.segmentHashIndexSize <= 0) {
      None
    } else {
      val last = keyValues.last

      val writeAbleLargestValueSize =
        last.hashIndexConfig.format.bytesToAllocatePerEntry(
          largestIndexOffset = last.stats.segmentAccessIndexOffset,
          largestKeyOffset = last.stats.segmentMergedKeyOffset,
          //unmerged used here because hashIndexes do not index range key-values.
          largestMergedKeySize = last.stats.segmentLargestUnmergedKeySize
        )

      val hasCompression = last.hashIndexConfig.compressions(UncompressedBlockInfo(last.stats.segmentHashIndexSize)).nonEmpty

      val headSize =
        headerSize(
          keyCounts = last.stats.uncompressedKeyCounts,
          writeAbleLargestValueSize = writeAbleLargestValueSize,
          hasCompression = hasCompression
        )

      val optimalBytes =
        optimalBytesRequired(
          keyCounts = last.stats.uncompressedKeyCounts,
          minimumNumberOfKeys = last.hashIndexConfig.minimumNumberOfKeys,
          writeAbleLargestValueSize = writeAbleLargestValueSize,
          allocateSpace = last.hashIndexConfig.allocateSpace,
          format = last.hashIndexConfig.format,
          hasCompression = hasCompression
        )

      //if the user allocated
      if (optimalBytes < headSize + ByteSizeOf.varInt)
        None
      else
        Some(
          HashIndexBlock.State(
            hit = 0,
            miss = 0,
            format = last.hashIndexConfig.format,
            minimumNumberOfKeys = last.hashIndexConfig.minimumNumberOfKeys,
            minimumNumberOfHits = last.hashIndexConfig.minimumNumberOfHits,
            writeAbleLargestValueSize = writeAbleLargestValueSize,
            minimumCRC = CRC32.disabledCRC,
            headerSize = headSize,
            maxProbe = last.hashIndexConfig.maxProbe,
            _bytes = Slice.create[Byte](optimalBytes),
            compressions =
              //cannot have no compression to begin with a then have compression because that upsets the total bytes required.
              if (hasCompression)
                last.hashIndexConfig.compressions
              else
                _ => Seq.empty
          )
        )
    }

  def headerSize(keyCounts: Int,
                 writeAbleLargestValueSize: Int,
                 hasCompression: Boolean): Int = {
    val headerSize =
      Block.headerSize(hasCompression) +
        ByteSizeOf.byte + //formatId
        ByteSizeOf.int + //allocated bytes
        ByteSizeOf.varInt + //max probe
        (Bytes.sizeOfUnsignedInt(keyCounts) * 2) + //hit & miss rate
        ByteSizeOf.varLong + //minimumCRC
        Bytes.sizeOfUnsignedInt(writeAbleLargestValueSize) //largest value size

    Bytes.sizeOfUnsignedInt(headerSize) +
      headerSize
  }

  def optimalBytesRequired(keyCounts: Int,
                           minimumNumberOfKeys: Int,
                           writeAbleLargestValueSize: Int,
                           hasCompression: Boolean,
                           format: HashIndexEntryFormat,
                           allocateSpace: RandomKeyIndex.RequiredSpace => Int): Int =
    if (keyCounts < minimumNumberOfKeys) {
      0
    } else {
      val sizePerKey =
      //+1 to skip left & right 0 start-end markers if it's not copiedIndex
      //+1 to for the last 1.byte entry so that next entry does overwrite previous writes tail 0's
      //the +1 does not need to be accounted in writeAbleLargestValueSize because these markers are just an indication of start and end index entry.
        keyCounts * (writeAbleLargestValueSize + 1)

      val minimumRequired =
        headerSize(
          keyCounts = keyCounts,
          hasCompression = hasCompression,
          writeAbleLargestValueSize = writeAbleLargestValueSize
        ) + sizePerKey

      try
        allocateSpace(
          RandomKeyIndex.RequiredSpace(
            _requiredSpace = minimumRequired,
            _numberOfKeys = keyCounts
          )
        )
      catch {
        case exception: Exception =>
          logger.error(
            """Custom allocate space calculation for HashIndex returned failure.
              |Using the default requiredSpace instead. Please check your implementation to ensure it's not throwing exception.
            """.stripMargin, exception)
          minimumRequired
      }
    }

  def close(state: State): Option[State] =
    if (state.bytes.isEmpty || !state.hasMinimumHits)
      None
    else {
      val compressedOrUncompressedBytes =
        Block.block(
          headerSize = state.headerSize,
          bytes = state.bytes,
          compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
          blockName = blockName
        )

      val allocatedBytes = state.bytes.allocatedSize
      state.bytes = compressedOrUncompressedBytes
      state.bytes addByte state.format.id
      state.bytes addInt allocatedBytes //allocated bytes
      state.bytes addUnsignedInt state.maxProbe
      state.bytes addUnsignedInt state.hit
      state.bytes addUnsignedInt state.miss
      state.bytes addUnsignedLong {
        //CRC can be -1 when HashIndex is not fully copied.
        if (state.minimumCRC == CRC32.disabledCRC)
          0
        else
          state.minimumCRC
      }
      state.bytes addUnsignedInt state.writeAbleLargestValueSize
      if (state.bytes.currentWritePosition > state.headerSize)
        throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition}")
      Some(state)
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
                         totalBlockSpace: Int,
                         headerSize: Int,
                         writeAbleLargestValueSize: Int) =
    ((hash & Int.MaxValue) % (totalBlockSpace - writeAbleLargestValueSize - headerSize)) + headerSize

  def write(keyValue: Transient,
            state: HashIndexBlock.State): Boolean = {
    if (keyValue.isPrefixCompressed) {
      //fix me - this should be managed by HashIndex itself.
      state.miss += 1
      false
    } else {
      state.format match {
        case format: HashIndexEntryFormat.Reference =>
          HashIndexBlock.writeReference(
            indexOffset = keyValue.stats.segmentAccessIndexOffset,
            keyOffset = keyValue.stats.segmentMergedKeyOffset,
            hashKey = keyValue.key,
            mergedKey = keyValue.mergedKey,
            keyType = keyValue.id,
            format = format,
            state = state
          )

        case format: HashIndexEntryFormat.Copy =>
          HashIndexBlock.writeCopy(
            indexOffset = keyValue.stats.segmentAccessIndexOffset,
            keyOffset = keyValue.stats.segmentMergedKeyOffset,
            hashKey = keyValue.key,
            mergedKey = keyValue.mergedKey,
            keyType = keyValue.id,
            format = format,
            state = state
          )
      }
    }
  }

  /**
   * Mutates the slice and adds writes the indexOffset to it's hash index.
   */
  def writeReference(indexOffset: Int,
                     keyOffset: Int,
                     hashKey: Slice[Byte],
                     mergedKey: Slice[Byte],
                     keyType: Byte,
                     format: HashIndexEntryFormat.Reference,
                     state: State): Boolean = {

    val requiredSpace =
      format.bytesToAllocatePerEntry(
        largestIndexOffset = indexOffset,
        largestKeyOffset = keyOffset,
        largestMergedKeySize = mergedKey.size
      )

    val hash = hashKey.hashCode()
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
            totalBlockSpace = state.bytes.allocatedSize,
            headerSize = state.headerSize,
            writeAbleLargestValueSize = state.writeAbleLargestValueSize
          )

        val existing = state.bytes.take(hashIndex, requiredSpace + 2) //+1 to reserve left 0 byte another +1 not overwrite next 0.
        if (existing.forall(_ == 0)) {
          state.bytes moveWritePosition (hashIndex + 1)

          format.write(
            indexOffset = indexOffset,
            keyOffset = keyOffset,
            mergedKey = mergedKey,
            keyType = keyType,
            bytes = state.bytes
          )
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, requiredSpace: $requiredSpace, value: ${state.bytes.take(hashIndex, state.bytes.currentWritePosition - hashIndex)} = success")
          state.hit += 1
          true
        } else {
          //println(s"Key: ${key.readInt()}: write hashIndex: $hashIndex probe: $probe, requiredSpace: $requiredSpace = failure")
          doWrite(probe = probe + 1)
        }
      }

    if (state.bytes.allocatedSize == 0)
      false
    else
      doWrite(0)
  }

  /**
   * Finds a key in the hash index.
   */
  private[block] def searchReference(key: Slice[Byte],
                                     hashIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                                     sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                     valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[Persistent.Partial] = {

    val hash = key.hashCode()
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val block = hashIndexReader.block

    val matcher = KeyMatcher.Get(key)

    @tailrec
    def doFind(probe: Int): Option[Persistent.Partial] =
      if (probe >= block.maxProbe) {
        None
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            totalBlockSpace = block.allocatedBytes,
            headerSize = block.headerSize,
            writeAbleLargestValueSize = block.writeAbleLargestValueSize
          ) - block.headerSize

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
          val partialKeyValueMaybe =
            block.format.read(
              entry = entry,
              hashIndexReader = hashIndexReader,
              sortedIndex = sortedIndexReader,
              values = valuesReader
            )

          if (partialKeyValueMaybe.isNone) {
            //println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe, sortedIndex: ${possibleOffset - 1}, possibleValue: $possibleOffset, containsZero: ${entry.take(bytesRead).exists(_ == 0)} = failed")
            doFind(probe + 1)
          } else {
            matcher(
              previous = partialKeyValueMaybe,
              next = None,
              hasMore = true
            ) match {
              case matched: Result.Matched =>
                //println(s"Key: ${key.readInt()}: Found: ${partialKeyValueMaybe.key} = success")
                Some(matched.result)

              case _: Result.Behind | _: Result.AheadOrNoneOrEnd =>
                //println(s"Key: ${key.readInt()}: Found: ${partialKeyValueMaybe.key} = Behind || Ahead || None || End")
                doFind(probe + 1)
            }
          }
        }
      }

    doFind(probe = 0)
  }

  /**
   * Writes full copy of the index entry within HashIndex.
   */
  def writeCopy(indexOffset: Int,
                keyOffset: Int,
                hashKey: Slice[Byte],
                mergedKey: Slice[Byte],
                keyType: Byte,
                format: HashIndexEntryFormat.Copy,
                state: State): Boolean = {

    val hash = hashKey.hashCode()
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val requiredSpace =
      state.format.bytesToAllocatePerEntry(
        largestIndexOffset = indexOffset,
        largestKeyOffset = keyOffset,
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
            totalBlockSpace = state.bytes.allocatedSize,
            headerSize = state.headerSize,
            writeAbleLargestValueSize = state.writeAbleLargestValueSize
          )

        //+1 for cases where the last byte is zero.
        val existing = state.bytes.take(hashIndex, requiredSpace + 1)

        if (existing.forall(_ == 0)) {
          state.bytes moveWritePosition hashIndex

          val crc =
            format.write(
              indexOffset = indexOffset,
              keyOffset = keyOffset,
              mergedKey = mergedKey,
              keyType = keyType,
              bytes = state.bytes
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

    if (state.bytes.allocatedSize == 0)
      false
    else
      doWrite(0)
  }

  private[block] def searchCopy(key: Slice[Byte],
                                hasIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                                sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[Persistent.Partial] = {

    val hash = key.hashCode()
    val hash1 = hash >>> 32
    val hash2 = (hash << 32) >> 32

    val block = hasIndexReader.block

    val matcher = KeyMatcher.Get(key)

    @tailrec
    def doFind(probe: Int): Option[Persistent.Partial] =
      if (probe >= block.maxProbe) {
        None
      } else {
        val hashIndex =
          adjustHash(
            hash = hash1 + probe * hash2,
            totalBlockSpace = block.allocatedBytes,
            headerSize = block.headerSize,
            writeAbleLargestValueSize = block.writeAbleLargestValueSize
          ) - block.headerSize //remove headerSize since the blockReader points to the hashIndex's start offset.

        val entry =
          hasIndexReader
            .moveTo(hashIndex)
            .read(block.writeAbleLargestValueSize)

        //println(s"Key: ${key.readInt()}: read hashIndex: ${hashIndex + block.headerSize} probe: $probe. entry: $entry")
        if (entry.isEmpty || entry.size == 1) {
          //println(s"Key: ${key.readInt()}: read hashIndex: ${hashIndex + block.headerSize} probe: $probe = failure - invalid start offset.")
          doFind(probe + 1)
        } else {
          val partialKeyValueMaybe: Maybe[Persistent.Partial] =
            block.format.read(
              entry = entry,
              hashIndexReader = hasIndexReader,
              sortedIndex = sortedIndexReader,
              values = valuesReader
            )

          if (partialKeyValueMaybe.isNone)
            doFind(probe + 1)
          else
            matcher(
              previous = partialKeyValueMaybe,
              next = None,
              hasMore = true
            ) match {
              case matched: Result.Matched =>
                //println(s"Key: ${key.readInt()}: Found: ${partialKeyValueMaybe.key} = success")
                Some(matched.result)

              case _: Result.Behind | _: Result.AheadOrNoneOrEnd =>
                //println(s"Key: ${key.readInt()}: Found: ${partialKeyValueMaybe.key} = Behind || Ahead || None || End")
                doFind(probe + 1)
            }
        }
      }

    doFind(probe = 0)
  }

  def search(key: Slice[Byte],
             hashIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                     partialKeyOrder: KeyOrder[Persistent.Partial]): Option[Persistent.Partial] =
    if (hashIndexReader.block.format.isCopy)
      searchCopy(
        key = key,
        hasIndexReader = hashIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    else
      searchReference(
        key = key,
        hashIndexReader = hashIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReader
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

  def isPerfect =
    miss == 0
}