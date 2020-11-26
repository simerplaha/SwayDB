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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.sortedindex

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.CompressionInternal
import swaydb.core.data._
import swaydb.core.segment.KeyMatcher
import swaydb.core.segment.KeyMatcher.Result
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.format.a.entry.writer._
import swaydb.core.segment.merge.MergeStats
import swaydb.core.util.{Bytes, MinMax}
import swaydb.data.MaxKey
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, FiniteDurations, FunctionSafe}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline

private[core] case object SortedIndexBlock extends LazyLogging {

  val blockName = this.productPrefix

  //use default writers
  implicit val timeWriter: TimeWriter = TimeWriter
  implicit val valueWriter: ValueWriter = ValueWriter
  implicit val deadlineWriter: DeadlineWriter = DeadlineWriter
  implicit val keyWriter: KeyWriter = KeyWriter

  implicit object SortedIndexBlockOps extends BlockOps[SortedIndexBlock.Offset, SortedIndexBlock] {
    override def updateBlockOffset(block: SortedIndexBlock, start: Int, size: Int): SortedIndexBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      SortedIndexBlock.Offset(start = start, size = size)

    override def readBlock(header: Block.Header[Offset]): SortedIndexBlock =
      SortedIndexBlock.read(header)
  }

  object Config {
    val disabled =
      Config(
        ioStrategy = (dataType: IOAction) => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        enablePrefixCompression = false,
        shouldPrefixCompress = _ => false,
        prefixCompressKeysOnly = false,
        enableAccessPositionIndex = false,
        optimiseForReverseIteration = false,
        normaliseIndex = false,
        compressions = (_: UncompressedBlockInfo) => Seq.empty
      )

    def apply(config: swaydb.data.config.SortedKeyIndex): Config =
      config match {
        case config: swaydb.data.config.SortedKeyIndex.On =>
          apply(config)
      }

    def apply(enable: swaydb.data.config.SortedKeyIndex.On): Config =
      Config(
        ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
        shouldPrefixCompress = enable.prefixCompression.shouldCompress,
        prefixCompressKeysOnly = enable.prefixCompression.enabled && enable.prefixCompression.keysOnly,
        enableAccessPositionIndex = enable.enablePositionIndex,
        optimiseForReverseIteration = enable.optimiseForReverseIteration,
        normaliseIndex = enable.prefixCompression.normaliseIndexForBinarySearch,
        enablePrefixCompression = enable.prefixCompression.enabled && !enable.prefixCompression.normaliseIndexForBinarySearch,
        compressions =
          FunctionSafe.safe(
            default = (_: UncompressedBlockInfo) => Iterable.empty[CompressionInternal],
            function = enable.compressions(_: UncompressedBlockInfo) map CompressionInternal.apply
          )
      )

    def apply(ioStrategy: IOAction => IOStrategy,
              enablePrefixCompression: Boolean,
              shouldPrefixCompress: Int => Boolean,
              prefixCompressKeysOnly: Boolean,
              enableAccessPositionIndex: Boolean,
              optimiseForReverseIteration: Boolean,
              normaliseIndex: Boolean,
              compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): Config =
      new Config(
        ioStrategy = ioStrategy,
        shouldPrefixCompress = if (normaliseIndex || !enablePrefixCompression) _ => false else shouldPrefixCompress,
        prefixCompressKeysOnly = if (normaliseIndex || !enablePrefixCompression) false else prefixCompressKeysOnly,
        enableAccessPositionIndex = enableAccessPositionIndex,
        optimiseForReverseIteration = optimiseForReverseIteration,
        enablePrefixCompression = !normaliseIndex && enablePrefixCompression,
        normaliseIndex = normaliseIndex,
        compressions = compressions
      )
  }

  /**
   * Do not create [[Config]] directly. Use one of the apply functions.
   */
  class Config private(val ioStrategy: IOAction => IOStrategy,
                       val shouldPrefixCompress: Int => Boolean,
                       val prefixCompressKeysOnly: Boolean,
                       val enableAccessPositionIndex: Boolean,
                       val enablePrefixCompression: Boolean,
                       val optimiseForReverseIteration: Boolean,
                       val normaliseIndex: Boolean,
                       val compressions: UncompressedBlockInfo => Iterable[CompressionInternal]) {

    def copy(ioStrategy: IOAction => IOStrategy = ioStrategy,
             shouldPrefixCompress: Int => Boolean = shouldPrefixCompress,
             enableAccessPositionIndex: Boolean = enableAccessPositionIndex,
             normaliseIndex: Boolean = normaliseIndex,
             enablePrefixCompression: Boolean = enablePrefixCompression,
             optimiseForReverseIteration: Boolean = optimiseForReverseIteration,
             compressions: UncompressedBlockInfo => Iterable[CompressionInternal] = compressions) =
    //do not use new here. Submit this to the apply function to that rules for creating the config gets applied.
      Config(
        ioStrategy = ioStrategy,
        shouldPrefixCompress = shouldPrefixCompress,
        enableAccessPositionIndex = enableAccessPositionIndex,
        prefixCompressKeysOnly = prefixCompressKeysOnly,
        enablePrefixCompression = enablePrefixCompression,
        optimiseForReverseIteration = optimiseForReverseIteration,
        normaliseIndex = normaliseIndex,
        compressions = compressions
      )
  }

  //IndexEntries that are used to create secondary indexes - binarySearchIndex & hashIndex
  class SecondaryIndexEntry(var indexOffset: Int, //mutable because if the bytes are normalised then this is adjust during close.
                            val mergedKey: Slice[Byte],
                            val comparableKey: Slice[Byte],
                            val keyType: Byte)

  case class Offset(start: Int, size: Int) extends BlockOffset

  /**
   * [[State]] is mostly mutable because these vars calculate runtime stats of
   * a Segment which is used to build other blocks. Immutable version of this
   * resulted in very slow compaction because immutable compaction was creation
   * millions of temporary objects every second causing GC halts.
   */
  class State(var compressibleBytes: Slice[Byte],
              var cacheableBytes: Slice[Byte],
              var header: Slice[Byte],
              var minKey: Slice[Byte],
              var maxKey: MaxKey[Slice[Byte]],
              var lastKeyValue: Memory,
              var smallestIndexEntrySize: Int,
              var largestIndexEntrySize: Int,
              var largestMergedKeySize: Int,
              var largestUncompressedMergedKeySize: Int,
              val enablePrefixCompression: Boolean,
              var entriesCount: Int,
              var prefixCompressedCount: Int,
              val shouldPrefixCompress: Int => Boolean,
              var nearestDeadline: Option[Deadline],
              var rangeCount: Int,
              var hasPut: Boolean,
              var hasRemoveRange: Boolean,
              var minMaxFunctionId: Option[MinMax[Slice[Byte]]],
              val enableAccessPositionIndex: Boolean,
              var optimiseForReverseIteration: Boolean,
              val compressDuplicateRangeValues: Boolean,
              val normaliseIndex: Boolean,
              val compressions: UncompressedBlockInfo => Iterable[CompressionInternal],
              val secondaryIndexEntries: ListBuffer[SecondaryIndexEntry],
              val indexEntries: ListBuffer[Slice[Byte]],
              val builder: EntryWriter.Builder) {

    def blockBytes: Slice[Byte] =
      header ++ compressibleBytes

    def uncompressedPrefixCount: Int =
      entriesCount - prefixCompressedCount

    def hasPrefixCompression: Boolean =
      builder.segmentHasPrefixCompression

    def prefixCompressKeysOnly =
      builder.prefixCompressKeysOnly

    def isPreNormalised: Boolean =
      hasSameIndexSizes()

    def hasSameIndexSizes(): Boolean =
      smallestIndexEntrySize == largestIndexEntrySize

    def blockSize: Int =
      header.size + compressibleBytes.size
  }

  //sortedIndex's byteSize is not know at the time of creation headerSize includes hasCompression
  //  val headerSize = {
  //    val size =
  //      Block.headerSize(true) +
  //        ByteSizeOf.boolean + //enablePositionIndex
  //        ByteSizeOf.boolean + //normalisedForBinarySearch
  //        ByteSizeOf.boolean + //hasPrefixCompression
  //        ByteSizeOf.boolean + //prefixCompressKeysOnly
  //        ByteSizeOf.boolean + //isPreNormalised
  //        ByteSizeOf.varInt + // normalisedEntrySize
  //        ByteSizeOf.varInt // segmentMaxSortedIndexEntrySize
  //
  //    Bytes.sizeOfUnsignedInt(size) + size
  //  }

  def init(keyValues: MergeStats.Persistent.Closed[Iterable],
           valuesConfig: ValuesBlock.Config,
           sortedIndexConfig: SortedIndexBlock.Config): SortedIndexBlock.State =
    init(
      maxSize = keyValues.maxSortedIndexSize,
      compressDuplicateValues = valuesConfig.compressDuplicateValues,
      compressDuplicateRangeValues = valuesConfig.compressDuplicateRangeValues,
      sortedIndexConfig = sortedIndexConfig
    )

  def init(maxSize: Int,
           compressDuplicateValues: Boolean,
           compressDuplicateRangeValues: Boolean,
           sortedIndexConfig: SortedIndexBlock.Config): SortedIndexBlock.State =
    init(
      bytes = Slice.of[Byte](maxSize),
      compressDuplicateValues = compressDuplicateValues,
      compressDuplicateRangeValues = compressDuplicateRangeValues,
      sortedIndexConfig = sortedIndexConfig
    )

  def init(bytes: Slice[Byte],
           compressDuplicateValues: Boolean,
           compressDuplicateRangeValues: Boolean,
           sortedIndexConfig: SortedIndexBlock.Config): SortedIndexBlock.State = {
    val builder =
      EntryWriter.Builder(
        prefixCompressKeysOnly = sortedIndexConfig.prefixCompressKeysOnly,
        compressDuplicateValues = compressDuplicateValues,
        enableAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
        optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration,
        bytes = bytes
      )

    new State(
      compressibleBytes = bytes,
      cacheableBytes = bytes,
      header = null,
      minKey = null,
      maxKey = null,
      lastKeyValue = null,
      smallestIndexEntrySize = Int.MaxValue,
      largestIndexEntrySize = 0,
      largestMergedKeySize = 0,
      largestUncompressedMergedKeySize = 0,
      enablePrefixCompression = sortedIndexConfig.enablePrefixCompression,
      entriesCount = 0,
      prefixCompressedCount = 0,
      shouldPrefixCompress = sortedIndexConfig.shouldPrefixCompress,
      nearestDeadline = None,
      rangeCount = 0,
      hasPut = false,
      hasRemoveRange = false,
      minMaxFunctionId = None,
      enableAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
      optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration,
      compressDuplicateRangeValues = compressDuplicateRangeValues,
      normaliseIndex = sortedIndexConfig.normaliseIndex,
      compressions = sortedIndexConfig.compressions,
      secondaryIndexEntries = ListBuffer.empty,
      indexEntries = ListBuffer.empty,
      builder = builder
    )
  }

  def write(keyValue: Memory,
            state: SortedIndexBlock.State)(implicit keyOrder: KeyOrder[Slice[Byte]]): Unit = {
    //currentWritePositionInThisSlice is used here because state.bytes can be a sub-slice.
    val positionBeforeWrite = state.compressibleBytes.currentWritePositionInThisSlice

    if (state.minKey == null)
      state.minKey = keyValue.key.unslice()

    if (state.enablePrefixCompression && state.shouldPrefixCompress(state.entriesCount))
      state.builder.enablePrefixCompressionForCurrentWrite = true

    keyValue match {
      case keyValue: Memory.Put =>
        state.hasPut = true
        state.nearestDeadline = FiniteDurations.getNearestDeadline(state.nearestDeadline, keyValue.deadline)

        EntryWriter.write(
          current = keyValue,
          builder = state.builder
        )

      case keyValue: Memory.Update =>
        EntryWriter.write(
          current = keyValue,
          builder = state.builder
        )

      case keyValue: Memory.Function =>
        state.minMaxFunctionId = Some(MinMax.minMaxFunction(keyValue, state.minMaxFunctionId))
        EntryWriter.write(
          current = keyValue,
          builder = state.builder
        )

      case keyValue: Memory.PendingApply =>
        state.minMaxFunctionId = MinMax.minMaxFunction(keyValue.applies, state.minMaxFunctionId)
        EntryWriter.write(
          current = keyValue,
          builder = state.builder
        )

      case keyValue: Memory.Remove =>
        EntryWriter.write(
          current = keyValue,
          builder = state.builder
        )

      case keyValue: Memory.Range =>
        keyValue.fromValue foreachS {
          case put: Value.Put =>
            state.nearestDeadline = FiniteDurations.getNearestDeadline(state.nearestDeadline, put.deadline)
          case _: Value.RangeValue =>
          //no need to do anything here. Just put deadline required.
        }
        state.minMaxFunctionId = MinMax.minMaxFunction(keyValue, state.minMaxFunctionId)
        state.hasPut = state.hasPut || keyValue.fromValue.existsS(_.isPut)
        state.hasRemoveRange = state.hasRemoveRange || keyValue.rangeValue.hasRemoveMayBe
        state.rangeCount += 1
        //ranges have a special config "compressDuplicateRangeValues" for duplicate value compression.
        //the following set and resets the compressDuplicateValues the builder accordingly.
        val compressDuplicateValues = state.builder.compressDuplicateValues

        if (compressDuplicateValues && state.compressDuplicateRangeValues)
          state.builder.compressDuplicateValues = true

        EntryWriter.write(
          current = keyValue,
          builder = state.builder
        )

        //reset
        state.builder.compressDuplicateValues = compressDuplicateValues
    }

    if (state.builder.isCurrentPrefixCompressed)
      state.prefixCompressedCount += 1
    else
      state.largestUncompressedMergedKeySize = state.largestUncompressedMergedKeySize max keyValue.mergedKey.size

    //if indexEntry is normalised or prefixCompressed skip create SearchIndexEntry since they are not required.
    if (state.builder.isCurrentPrefixCompressed) {
      //skip creating indexEntry.
      state.builder.isCurrentPrefixCompressed = false
    } else {
      state.builder.previousIndexOffset = positionBeforeWrite

      val entry =
        new SecondaryIndexEntry(
          indexOffset = positionBeforeWrite,
          mergedKey = keyValue.mergedKey,
          comparableKey = keyOrder.comparableKey(keyValue.key),
          keyType = keyValue.id
        )

      state.secondaryIndexEntries += entry
    }

    if (state.normaliseIndex)
      state.indexEntries += state.compressibleBytes.slice(positionBeforeWrite, state.builder.bytes.currentWritePositionInThisSlice - 1)

    val indexEntrySize = state.compressibleBytes.currentWritePositionInThisSlice - positionBeforeWrite

    state.smallestIndexEntrySize = state.smallestIndexEntrySize min indexEntrySize
    state.largestIndexEntrySize = state.largestIndexEntrySize max indexEntrySize

    state.largestMergedKeySize = state.largestMergedKeySize max keyValue.mergedKey.size

    state.entriesCount += 1

    state.builder.enablePrefixCompressionForCurrentWrite = false

    state.lastKeyValue = keyValue
    state.builder.previous = keyValue
  }

  private def normaliseIfRequired(state: State): Slice[Byte] =
    if (state.normaliseIndex) {
      val difference = state.largestIndexEntrySize - state.smallestIndexEntrySize
      if (difference == 0) {
        state.compressibleBytes
      } else {
        //because bytes are normalised searchIndexEntries's indexOffsets is required to be adjusted/padded so the indexOffsets require reset.
        var adjustedIndexEntryOffset = 0
        val secondaryIndexEntries = state.secondaryIndexEntries.iterator
        //temporary check.
        assert(state.indexEntries.size == state.secondaryIndexEntries.size, s"${state.indexEntries.size} != ${state.secondaryIndexEntries.size}")

        val bytesRequired = state.largestIndexEntrySize * state.entriesCount
        val bytes = Slice.of[Byte](bytesRequired)

        state.indexEntries foreach {
          entry =>
            //mutate the index offset.
            secondaryIndexEntries.next().indexOffset = adjustedIndexEntryOffset
            adjustedIndexEntryOffset += state.largestIndexEntrySize

            bytes addAll entry

            //pad any missing bytes.
            val missedBytes = state.largestIndexEntrySize - entry.size
            if (missedBytes > 0)
              bytes moveWritePosition (bytes.currentWritePosition + missedBytes) //fill in the missing bytes to maintain fixed size for each entry.
        }
        state.cacheableBytes = bytes
        bytes
      }
    } else {
      state.compressibleBytes
    }

  def close(state: State): State = {
    val normalisedBytes: Slice[Byte] = normaliseIfRequired(state)

    val compressionResult =
      Block.compress(
        bytes = normalisedBytes,
        compressions = state.compressions(UncompressedBlockInfo(normalisedBytes.size)),
        blockName = blockName
      )

    state.compressibleBytes = compressionResult.compressedBytes getOrElse normalisedBytes

    compressionResult.headerBytes addBoolean state.enableAccessPositionIndex
    compressionResult.headerBytes addBoolean state.optimiseForReverseIteration
    compressionResult.headerBytes addBoolean state.hasPrefixCompression
    compressionResult.headerBytes addBoolean state.prefixCompressKeysOnly
    compressionResult.headerBytes addBoolean state.normaliseIndex
    compressionResult.headerBytes addBoolean state.isPreNormalised
    compressionResult.headerBytes addUnsignedInt state.largestIndexEntrySize

    compressionResult.fixHeaderSize()

    state.maxKey =
      state.lastKeyValue match {
        case range: Memory.Range =>
          MaxKey.Range(range.fromKey.unslice(), range.toKey.unslice())

        case keyValue: Memory.Fixed =>
          MaxKey.Fixed(keyValue.key.unslice())
      }

    state.header = compressionResult.headerBytes

    state
  }

  def unblockedReader(closedState: SortedIndexBlock.State): UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock] = {
    val block =
      SortedIndexBlock(
        offset = SortedIndexBlock.Offset(0, closedState.cacheableBytes.size),
        enableAccessPositionIndex = closedState.enableAccessPositionIndex,
        optimiseForReverseIteration = closedState.optimiseForReverseIteration,
        hasPrefixCompression = closedState.hasPrefixCompression,
        prefixCompressKeysOnly = closedState.prefixCompressKeysOnly,
        normalised = closedState.normaliseIndex,
        isPreNormalised = closedState.isPreNormalised,
        headerSize = 0,
        segmentMaxIndexEntrySize = closedState.largestIndexEntrySize,
        compressionInfo = None
      )

    UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock](
      block = block,
      bytes = closedState.cacheableBytes.close()
    )
  }

  def read(header: Block.Header[SortedIndexBlock.Offset]): SortedIndexBlock = {
    val enableAccessPositionIndex = header.headerReader.readBoolean()
    val optimiseForReverseIteration = header.headerReader.readBoolean()
    val hasPrefixCompression = header.headerReader.readBoolean()
    val prefixCompressKeysOnly = header.headerReader.readBoolean()
    val normaliseForBinarySearch = header.headerReader.readBoolean()
    val isPreNormalised = header.headerReader.readBoolean()
    val segmentMaxIndexEntrySize = header.headerReader.readUnsignedInt()

    SortedIndexBlock(
      offset = header.offset,
      enableAccessPositionIndex = enableAccessPositionIndex,
      optimiseForReverseIteration = optimiseForReverseIteration,
      hasPrefixCompression = hasPrefixCompression,
      prefixCompressKeysOnly = prefixCompressKeysOnly,
      normalised = normaliseForBinarySearch,
      isPreNormalised = isPreNormalised,
      headerSize = header.headerSize,
      segmentMaxIndexEntrySize = segmentMaxIndexEntrySize,
      compressionInfo = header.compressionInfo
    )
  }

  def readPartialKeyValue(fromOffset: Int,
                          sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                          valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial =
    readIndexEntry(
      keySizeOrZero = 0,
      sortedIndexReader = sortedIndexReader moveTo fromOffset,
      previous = Persistent.Null,
      valuesReaderOrNull = valuesReaderOrNull,
      parser = SortedIndexEntryParser.PartialEntry
    )

  private def readKeyValue(previous: Persistent,
                           indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent =

    readIndexEntry(
      keySizeOrZero = previous.nextKeySize,
      sortedIndexReader = indexReader moveTo previous.nextIndexOffset,
      previous = previous,
      valuesReaderOrNull = valuesReaderOrNull,
      parser = SortedIndexEntryParser.PersistentEntry
    )

  private def readKeyValue(fromPosition: Int,
                           indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent =
    readKeyValue(
      fromPosition = fromPosition,
      keySizeOrZero = 0,
      indexReader = indexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  private def readKeyValue(fromPosition: Int,
                           keySizeOrZero: Int,
                           indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent =
    readIndexEntry(
      keySizeOrZero = keySizeOrZero,
      sortedIndexReader = indexReader moveTo fromPosition,
      previous = Persistent.Null,
      valuesReaderOrNull = valuesReaderOrNull,
      parser = SortedIndexEntryParser.PersistentEntry
    )

  /**
   * Pre-requisite: The position of the index on the reader should be set.
   */
  private def readIndexEntry[T](keySizeOrZero: Int,
                                previous: PersistentOption,
                                sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
                                parser: SortedIndexEntryParser[T]): T = {
    val positionBeforeReader = sortedIndexReader.getPosition
    if (keySizeOrZero > 0) { //try reading entry bytes within one seek.
      sortedIndexReader skip Bytes.sizeOfUnsignedInt(keySizeOrZero)

      val indexSize =
        if (sortedIndexReader.block.isBinarySearchable)
          sortedIndexReader.block.segmentMaxIndexEntrySize
        else
          EntryWriter.maxEntrySize(
            keySize = keySizeOrZero,
            hasAccessIndexPosition = sortedIndexReader.block.enableAccessPositionIndex,
            optimiseForReverseIteration = sortedIndexReader.block.optimiseForReverseIteration
          )

      //read all bytes for this index entry plus the next 5 bytes to fetch next index entry's size.
      val indexEntry = sortedIndexReader read (indexSize + ByteSizeOf.varInt)

      parser.parse(
        readPosition = positionBeforeReader,
        headerInteger = keySizeOrZero,
        tailBytes = indexEntry,
        previous = previous,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    } else if (sortedIndexReader.block.isBinarySearchable) { //if the reader has a block cache try fetching the bytes required within one seek.
      val indexSize = sortedIndexReader.block.segmentMaxIndexEntrySize
      val bytes = sortedIndexReader.read(sortedIndexReader.block.segmentMaxIndexEntrySize + ByteSizeOf.varInt)
      val (headerInteger, headerIntegerByteSize) = bytes.readUnsignedIntWithByteSize()

      //read all bytes for this index entry plus the next 5 bytes to fetch next index entry's size.
      val indexEntry = bytes.take(headerIntegerByteSize, indexSize + ByteSizeOf.varInt)

      parser.parse(
        readPosition = positionBeforeReader,
        headerInteger = headerInteger,
        tailBytes = indexEntry,
        previous = previous,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    } else if (sortedIndexReader.hasBlockCache) {
      val positionBeforeRead = sortedIndexReader.getPosition
      //read the minimum number of bytes required for parse this indexEntry.
      val bytes = sortedIndexReader.read(ByteSizeOf.varInt)
      val (headerInteger, headerIntegerByteSize) = bytes.readUnsignedIntWithByteSize()
      //open the slice if it's a subslice,
      val openBytes = bytes.openEnd()

      //check if the read bytes are enough to parse the entry.
      val expectedSize =
        headerIntegerByteSize +
          headerInteger +
          EntryWriter.maxEntrySize(
            hasAccessIndexPosition = sortedIndexReader.block.enableAccessPositionIndex,
            optimiseForReverseIteration = sortedIndexReader.block.optimiseForReverseIteration
          ) + ByteSizeOf.varInt

      //read all bytes for this index entry plus the next 5 bytes to fetch next index entry's size.
      //if openBytes results in enough bytes to then read the open bytes only.

      val indexEntry =
        if (openBytes.size >= expectedSize)
          openBytes.take(headerIntegerByteSize, expectedSize)
        else
          sortedIndexReader.moveTo(positionBeforeRead + headerIntegerByteSize) read expectedSize

      parser.parse(
        readPosition = positionBeforeReader,
        headerInteger = headerInteger,
        tailBytes = indexEntry,
        previous = previous,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    } else {
      val headerInteger = sortedIndexReader.readUnsignedInt()

      val indexSize =
        EntryWriter.maxEntrySize(
          headerInteger,
          sortedIndexReader.block.enableAccessPositionIndex,
          sortedIndexReader.block.optimiseForReverseIteration
        )

      //read all bytes for this index entry plus the next 5 bytes to fetch next index entry's size.
      val indexEntry = sortedIndexReader read (indexSize + ByteSizeOf.varInt)

      parser.parse(
        readPosition = positionBeforeReader,
        headerInteger = headerInteger,
        tailBytes = indexEntry,
        previous = previous,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    }
  }

  def toSlice(keyValueCount: Int,
              sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
              valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Slice[Persistent] = {
    val aggregator = Slice.newAggregator[Persistent](keyValueCount)

    iterator(
      sortedIndexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    ) foreach aggregator.apply

    aggregator.result
  }

  def iterator(sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
               valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Iterator[Persistent] =
    new Iterator[Persistent] {
      sortedIndexReader moveTo 0

      var continue = true
      var previousMayBe: PersistentOption = Persistent.Null

      override def hasNext: Boolean =
        continue

      override def next(): Persistent = {
        val nextKeySize: Integer =
          if (previousMayBe.isNoneS) {
            null
          } else {
            val previous = previousMayBe.getS
            //If previous is known, keep reading same reader
            // and set the next position of the reader to be of the next index's offset.
            sortedIndexReader moveTo previous.nextIndexOffset
            previous.nextKeySize
          }

        val next =
          readIndexEntry(
            keySizeOrZero = nextKeySize,
            previous = previousMayBe,
            sortedIndexReader = sortedIndexReader,
            valuesReaderOrNull = valuesReaderOrNull,
            parser = SortedIndexEntryParser.PersistentEntry
          )

        previousMayBe = next
        continue = next.hasMore
        next
      }
    }

  def seekAndMatch(key: Slice[Byte],
                   startFrom: PersistentOption,
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (startFrom.exists(from => order.gteq(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    if (startFrom.forallS(_.hasMore))
      seekAndMatchToPersistent(
        matcher = KeyMatcher.Get(key),
        startFrom = startFrom,
        indexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    else
      Persistent.Null

  def matchOrSeek(key: Slice[Byte],
                  startFrom: Persistent,
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]]): Persistent.PartialOption =
  //    if (ordering.gteq(startFrom.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    if (startFrom.forallS(_.hasMore))
      SortedIndexBlock.matchOrSeek(
        matcher = KeyMatcher.Get(key),
        previous = startFrom,
        next = Persistent.Null,
        indexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      ) match {
        case matched: KeyMatcher.Result.Matched =>
          matched.result

        case KeyMatcher.Result.AheadOrNoneOrEnd | KeyMatcher.Result.BehindStopped =>
          Persistent.Partial.Null
      }
    else
      Persistent.Partial.Null

  def searchSeekOne(key: Slice[Byte],
                    start: Persistent,
                    indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                    valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (order.gteq(start.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    if (start.hasMore)
      seekAndMatchToPersistent(
        matcher = KeyMatcher.Get.MatchOnly(key),
        startFrom = start,
        indexReader = indexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    else
      Persistent.Null

  def searchSeekOne(key: Slice[Byte],
                    fromPosition: Int,
                    keySizeOrZero: Int,
                    indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                    valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (order.gteq(start.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    if (fromPosition == -1)
      Persistent.Null
    else
      seekAndMatchOrSeekToPersistent(
        matcher = KeyMatcher.Get.MatchOnly(key),
        fromOffset = fromPosition,
        keySizeOrZero = keySizeOrZero,
        indexReader = indexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

  def searchHigher(key: Slice[Byte],
                   startFrom: PersistentOption,
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchToPersistent(
      matcher = KeyMatcher.Higher(key),
      startFrom = startFrom,
      indexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def matchOrSeekHigher(key: Slice[Byte],
                        startFrom: PersistentOption,
                        sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                        valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    startFrom match {
      case startFrom: Persistent =>
        SortedIndexBlock.matchOrSeek(
          matcher = KeyMatcher.Higher(key),
          previous = startFrom,
          next = Persistent.Null,
          indexReader = sortedIndexReader,
          valuesReaderOrNull = valuesReaderOrNull
        ) match {
          case matched: KeyMatcher.Result.Matched =>
            matched.result.toPersistent

          case KeyMatcher.Result.AheadOrNoneOrEnd | KeyMatcher.Result.BehindStopped =>
            Persistent.Null
        }

      case Persistent.Null =>
        seekAndMatchToPersistent(
          matcher = KeyMatcher.Higher(key),
          startFrom = startFrom,
          indexReader = sortedIndexReader,
          valuesReaderOrNull = valuesReaderOrNull
        )
    }

  def searchHigherSeekOne(key: Slice[Byte],
                          startFrom: Persistent,
                          sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                          valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (order.gt(startFrom.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchToPersistent(
      matcher = KeyMatcher.Higher.MatchOnly(key),
      startFrom = startFrom,
      indexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def searchHigherSeekOne(key: Slice[Byte],
                          fromPosition: Int,
                          keySizeOrZero: Int,
                          sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                          valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (order.gt(startFrom.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchOrSeekToPersistent(
      matcher = KeyMatcher.Higher.MatchOnly(key),
      fromOffset = fromPosition,
      keySizeOrZero = keySizeOrZero,
      indexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def seekLowerAndMatch(key: Slice[Byte],
                        startFrom: PersistentOption,
                        sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                        valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (startFrom.exists(from => order.gteq(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchToPersistent(
      matcher = KeyMatcher.Lower(key),
      startFrom = startFrom,
      indexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def matchOrSeekLower(key: Slice[Byte],
                       startFrom: PersistentOption,
                       next: PersistentOption,
                       sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Slice[Byte]]): PersistentOption =
  //    if (startFrom.exists(from => order.gteq(from.key, key)) ||
  //      next.exists(next => next match {
  //        case range: Partial.RangeT => order.lt(range.toKey, key)
  //        case fixed: Partial.Fixed => order.lt(fixed.key, key)
  //      }
  //      )
  //    ) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("invalid startFrom or next."))
  //    else
    startFrom match {
      case startFrom: Persistent =>
        SortedIndexBlock.matchOrSeek(
          matcher = KeyMatcher.Lower(key),
          previous = startFrom,
          next = next,
          indexReader = sortedIndexReader,
          valuesReaderOrNull = valuesReaderOrNull
        ) match {
          case matched: KeyMatcher.Result.Matched =>
            matched.result.toPersistent

          case KeyMatcher.Result.AheadOrNoneOrEnd | KeyMatcher.Result.BehindStopped =>
            Persistent.Null
        }

      case Persistent.Null =>
        next match {
          case next: Persistent =>
            SortedIndexBlock.matchOrSeek(
              previous = next,
              next = Persistent.Null,
              matcher = KeyMatcher.Lower(key),
              indexReader = sortedIndexReader,
              valuesReaderOrNull = valuesReaderOrNull
            ) match {
              case matched: KeyMatcher.Result.Matched =>
                matched.result.toPersistent

              case KeyMatcher.Result.AheadOrNoneOrEnd | KeyMatcher.Result.BehindStopped =>
                Persistent.Null
            }

          case Persistent.Null =>
            seekAndMatchToPersistent(
              matcher = KeyMatcher.Lower(key),
              startFrom = startFrom,
              indexReader = sortedIndexReader,
              valuesReaderOrNull = valuesReaderOrNull
            )
        }
    }

  private def seekAndMatchToPersistent(matcher: KeyMatcher,
                                       startFrom: PersistentOption,
                                       indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                       valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): PersistentOption =
    startFrom match {
      case startFrom: Persistent =>
        seekAndMatchOrSeek(
          previous = startFrom,
          next = Persistent.Null,
          matcher = matcher,
          indexReader = indexReader,
          valuesReaderOrNull = valuesReaderOrNull
        ) match {
          case matched: KeyMatcher.Result.Matched =>
            matched.result.toPersistent

          case KeyMatcher.Result.AheadOrNoneOrEnd | KeyMatcher.Result.BehindStopped =>
            Persistent.Null
        }

      //No start from. Get the first index entry from the File and start from there.
      case Persistent.Null =>
        seekAndMatchOrSeekToPersistent(
          matcher = matcher,
          fromOffset = 0,
          indexReader = indexReader,
          valuesReaderOrNull = valuesReaderOrNull
        )
    }

  def seekAndMatchOrSeek(matcher: KeyMatcher,
                         fromOffset: Int,
                         indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                         valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Result.Complete = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = indexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    //        ////////println("matchOrSeekAndPersistent")
    matchOrSeek(
      previous = persistent,
      next = Persistent.Null,
      matcher = matcher,
      indexReader = indexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )
  }

  def seekAndMatchOrSeekToPersistent(matcher: KeyMatcher,
                                     fromOffset: Int,
                                     indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                     valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): PersistentOption =
    seekAndMatchOrSeekToPersistent(
      matcher = matcher,
      fromOffset = fromOffset,
      keySizeOrZero = 0,
      indexReader = indexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def seekAndMatchOrSeekToPersistent(matcher: KeyMatcher,
                                     fromOffset: Int,
                                     keySizeOrZero: Int,
                                     indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                     valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): PersistentOption = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        keySizeOrZero = keySizeOrZero,
        indexReader = indexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    ////////println("matchOrSeekAndPersistent")
    matchOrSeekToPersistent(
      previous = persistent,
      next = Persistent.Null,
      matcher = matcher,
      indexReader = indexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )
  }

  def readAndMatch(matcher: KeyMatcher,
                   fromOffset: Int,
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): KeyMatcher.Result = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    matcher(
      previous = persistent,
      next = Persistent.Partial.Null,
      hasMore = persistent.hasMore
    )
  }

  def read(fromOffset: Int,
           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent =
    readKeyValue(
      fromPosition = fromOffset,
      indexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def read(fromOffset: Int,
           keySize: Int,
           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent =
    readKeyValue(
      fromPosition = fromOffset,
      keySizeOrZero = keySize,
      indexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def readPreviousAndMatch(matcher: KeyMatcher,
                           next: Persistent,
                           fromOffset: Int,
                           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): KeyMatcher.Result = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    matcher(
      previous = persistent,
      next = next,
      hasMore = next.hasMore
    )
  }

  def readSeekAndMatch(matcher: KeyMatcher,
                       previous: Persistent,
                       fromOffset: Int,
                       sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): KeyMatcher.Result = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    matcher(
      previous = previous,
      next = persistent,
      hasMore = persistent.hasMore
    )
  }

  def readNextKeyValue(previous: Persistent,
                       sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent =
    readKeyValue(
      previous = previous,
      indexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def findAndMatchOrSeekMatch(matcher: KeyMatcher,
                              fromOffset: Int,
                              sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                              valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): KeyMatcher.Result = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = sortedIndex,
        valuesReaderOrNull = valuesReaderOrNull
      )

    matchOrSeek(
      previous = persistent,
      next = Persistent.Null,
      matcher = matcher,
      indexReader = sortedIndex,
      valuesReaderOrNull = valuesReaderOrNull
    )
  }

  @tailrec
  def matchOrSeek(previous: Persistent,
                  next: PersistentOption,
                  matcher: KeyMatcher,
                  indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): KeyMatcher.Result.Complete =
    matcher(
      previous = previous,
      next = next.asPartial,
      hasMore = (next getOrElseS previous).hasMore
    ) match {
      //if it's higher with MatchOnly set to true but the currently read key-value is a key match this means the next higher is only a step forward.
      //make an exception and do another seek.
      case KeyMatcher.Result.BehindStopped if matcher.isHigher && next.getOrElseS(previous).existsS(keyValue => keyValue.hasMore && matcher.keyOrder.equiv(keyValue.key, matcher.key)) =>
        val nextKeyValue =
          readKeyValue(
            previous = next.getOrElseS(previous),
            indexReader = indexReader,
            valuesReaderOrNull = valuesReaderOrNull
          )

        new KeyMatcher.Result.Matched(nextKeyValue)

      case result: KeyMatcher.Result.Complete =>
        result

      case KeyMatcher.Result.BehindFetchNext =>
        //        assert(previous.key.readInt() <= previousKeyValue.key.readInt())
        //        println(s"Walking previous: ${previous.key.readInt()}, next: ${next.mapS(_.key.readInt())}")
        val readFrom: Persistent = next getOrElseS previous.toPersistent

        val nextNextKeyValue =
          readKeyValue(
            previous = readFrom,
            indexReader = indexReader,
            valuesReaderOrNull = valuesReaderOrNull
          )

        matchOrSeek(
          previous = readFrom,
          next = nextNextKeyValue,
          matcher = matcher,
          indexReader = indexReader,
          valuesReaderOrNull = valuesReaderOrNull
        )
    }

  def seekAndMatchOrSeek(previous: Persistent,
                         next: PersistentOption,
                         matcher: KeyMatcher,
                         indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                         valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): KeyMatcher.Result.Complete = {
    val nextNextKeyValue =
      readKeyValue(
        previous = next getOrElseS previous,
        indexReader = indexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    matchOrSeek(
      previous = next getOrElseS previous,
      next = nextNextKeyValue,
      matcher = matcher,
      indexReader = indexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )
  }

  def matchOrSeekToPersistent(previous: Persistent,
                              next: PersistentOption,
                              matcher: KeyMatcher,
                              indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                              valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): PersistentOption =
    matchOrSeek(
      previous = previous,
      next = next,
      matcher = matcher,
      indexReader = indexReader,
      valuesReaderOrNull = valuesReaderOrNull
    ) match {
      case matched: KeyMatcher.Result.Matched =>
        matched.result.toPersistent

      case KeyMatcher.Result.AheadOrNoneOrEnd | KeyMatcher.Result.BehindStopped =>
        Persistent.Null
    }
}

private[core] case class SortedIndexBlock(offset: SortedIndexBlock.Offset,
                                          enableAccessPositionIndex: Boolean,
                                          optimiseForReverseIteration: Boolean,
                                          hasPrefixCompression: Boolean,
                                          prefixCompressKeysOnly: Boolean,
                                          normalised: Boolean,
                                          isPreNormalised: Boolean,
                                          headerSize: Int,
                                          segmentMaxIndexEntrySize: Int,
                                          compressionInfo: Option[Block.CompressionInfo]) extends Block[SortedIndexBlock.Offset] {
  val isBinarySearchable =
    !hasPrefixCompression && (normalised || isPreNormalised)

  val isNotPreNormalised =
    !hasPrefixCompression && normalised && !isPreNormalised

  val normalisedByteSize: Int =
    if (normalised) segmentMaxIndexEntrySize else 0

  val sortedIndexEndOffsetForReads =
    offset.size - 1

  val hasNormalisedBytes =
    !isPreNormalised && normalised
}
