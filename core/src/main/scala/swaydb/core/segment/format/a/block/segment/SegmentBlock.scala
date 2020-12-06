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

package swaydb.core.segment.format.a.block.segment

import com.typesafe.scalalogging.LazyLogging
import swaydb.Compression
import swaydb.compression.CompressionInternal
import swaydb.core.data.Memory
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.data.{ClosedBlocks, ClosedBlocksWithFooter, TransientSegment}
import swaydb.core.segment.format.a.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.merge.MergeStats
import swaydb.core.segment.merge.MergeStats.Persistent
import swaydb.core.segment.{PersistentSegmentMany, PersistentSegmentOne}
import swaydb.core.util.{Bytes, Collections, MinMax}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

private[core] case object SegmentBlock extends LazyLogging {

  val blockName = this.productPrefix

  val formatId: Byte = 1.toByte

  val crcBytes: Int = 13

  object Config {

    def apply(config: SegmentConfig): SegmentBlock.Config =
      apply(
        fileOpenIOStrategy = config.fileOpenIOStrategy,
        blockIOStrategy = config.blockIOStrategy,
        cacheBlocksOnCreate = config.cacheSegmentBlocksOnCreate,
        minSize = config.minSegmentSize,
        enableHashIndexForListSegment = config.segmentFormat.enableRootHashIndex,
        maxCount = config.segmentFormat.count,
        pushForward = config.pushForward,
        mmap = config.mmap,
        deleteDelay = config.deleteDelay,
        compressions = config.compression
      )

    def apply(fileOpenIOStrategy: IOStrategy.ThreadSafe,
              blockIOStrategy: IOAction => IOStrategy,
              cacheBlocksOnCreate: Boolean,
              minSize: Int,
              maxCount: Int,
              enableHashIndexForListSegment: Boolean,
              pushForward: PushForwardStrategy,
              mmap: MMAP.Segment,
              deleteDelay: FiniteDuration,
              compressions: UncompressedBlockInfo => Iterable[Compression]): Config =
      applyInternal(
        fileOpenIOStrategy = fileOpenIOStrategy,
        blockIOStrategy = blockIOStrategy,
        cacheBlocksOnCreate = cacheBlocksOnCreate,
        minSize = minSize,
        enableHashIndexForListSegment = enableHashIndexForListSegment,
        maxCount = maxCount,
        pushForward = pushForward,
        mmap = mmap,
        deleteDelay = deleteDelay,
        compressions =
          uncompressedBlockInfo =>
            Try(compressions(uncompressedBlockInfo))
              .getOrElse(Seq.empty)
              .map(CompressionInternal.apply)
              .toSeq
      )

    private[core] def applyInternal(fileOpenIOStrategy: IOStrategy.ThreadSafe,
                                    blockIOStrategy: IOAction => IOStrategy,
                                    cacheBlocksOnCreate: Boolean,
                                    minSize: Int,
                                    maxCount: Int,
                                    enableHashIndexForListSegment: Boolean,
                                    pushForward: PushForwardStrategy,
                                    mmap: MMAP.Segment,
                                    deleteDelay: FiniteDuration,
                                    compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): Config =
      new Config(
        fileOpenIOStrategy = fileOpenIOStrategy,
        blockIOStrategy = blockIOStrategy,
        cacheBlocksOnCreate = cacheBlocksOnCreate,
        minSize = minSize max 1,
        maxCount = maxCount max 1,
        enableHashIndexForListSegment = enableHashIndexForListSegment,
        pushForward = pushForward,
        mmap = mmap,
        deleteDelay = deleteDelay,
        compressions = compressions
      )
  }

  class Config private(val fileOpenIOStrategy: IOStrategy.ThreadSafe,
                       val blockIOStrategy: IOAction => IOStrategy,
                       val cacheBlocksOnCreate: Boolean,
                       val minSize: Int,
                       val maxCount: Int,
                       val enableHashIndexForListSegment: Boolean,
                       val pushForward: PushForwardStrategy,
                       val mmap: MMAP.Segment,
                       val deleteDelay: FiniteDuration,
                       val compressions: UncompressedBlockInfo => Iterable[CompressionInternal]) {

    val isDeleteEventually: Boolean =
      deleteDelay.fromNow.hasTimeLeft()

    //disables splitting of segments and creates a single segment.
    def singleton: Config =
      this.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)

    def copy(minSize: Int = minSize, maxCount: Int = maxCount, mmap: MMAP.Segment = mmap): SegmentBlock.Config =
      SegmentBlock.Config.applyInternal(
        fileOpenIOStrategy = fileOpenIOStrategy,
        blockIOStrategy = blockIOStrategy,
        cacheBlocksOnCreate = cacheBlocksOnCreate,
        minSize = minSize,
        maxCount = maxCount,
        enableHashIndexForListSegment = enableHashIndexForListSegment,
        pushForward = pushForward,
        mmap = mmap,
        deleteDelay = deleteDelay,
        compressions = compressions
      )
  }

  object Offset {
    def empty =
      SegmentBlock.Offset(0, 0)
  }

  case class Offset(start: Int, size: Int) extends BlockOffset

  def read(header: Block.Header[Offset]): SegmentBlock =
    SegmentBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )

  def writeOneOrMany(mergeStats: MergeStats.Persistent.Closed[Iterable],
                     createdInLevel: Int,
                     bloomFilterConfig: BloomFilterBlock.Config,
                     hashIndexConfig: HashIndexBlock.Config,
                     binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                     sortedIndexConfig: SortedIndexBlock.Config,
                     valuesConfig: ValuesBlock.Config,
                     segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment] =
    if (mergeStats.isEmpty) {
      Slice.empty
    } else {
      val ones: Slice[TransientSegment.One] =
        writeOnes(
          mergeStats = mergeStats,
          createdInLevel = createdInLevel,
          bloomFilterConfig = bloomFilterConfig,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          sortedIndexConfig = sortedIndexConfig,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig
        )

      writeOneOrMany(
        createdInLevel = createdInLevel,
        ones = ones,
        sortedIndexConfig = sortedIndexConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      )
    }

  def writeOneOrMany(createdInLevel: Int,
                     ones: Slice[TransientSegment.Singleton],
                     sortedIndexConfig: SortedIndexBlock.Config,
                     hashIndexConfig: HashIndexBlock.Config,
                     binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                     valuesConfig: ValuesBlock.Config,
                     segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment] =
    if (ones.isEmpty) {
      Slice.empty
    } else {
      val groups: Slice[Slice[TransientSegment.Singleton]] =
        Collections.groupedBySize[TransientSegment.Singleton](
          minGroupSize = segmentConfig.minSize,
          itemSize = _.segmentSize,
          items = ones
        )

      groups map {
        segments =>
          if (segments.size == 1) {
            segments.head.copyWithFileHeader(headerBytes = PersistentSegmentOne.formatIdSlice)
          } else {
            val listKeyValue: Persistent.Builder[Memory, Slice] =
              MergeStats.persistent(Slice.newAggregator(segments.size * 2))

            var minMaxFunctionId = Option.empty[MinMax[Slice[Byte]]]

            segments.foldLeft(0) {
              case (offset, segment) =>
                minMaxFunctionId = MinMax.minMaxFunctionOption(segment.minMaxFunctionId, minMaxFunctionId)

                val segmentSize = segment.segmentSize
                listKeyValue addAll segment.toKeyValue(offset, segmentSize)
                offset + segmentSize
            }

            val closedListKeyValues = listKeyValue.close(hasAccessPositionIndex = true, optimiseForReverseIteration = true)

            val modifiedSortedIndex =
              if (sortedIndexConfig.normaliseIndex)
                sortedIndexConfig.copy(normaliseIndex = false)
              else
                sortedIndexConfig

            val listSegments =
              writeOnes(
                mergeStats = closedListKeyValues,
                createdInLevel = createdInLevel,
                bloomFilterConfig = BloomFilterBlock.Config.disabled,
                hashIndexConfig = if (segmentConfig.enableHashIndexForListSegment) hashIndexConfig else HashIndexBlock.Config.disabled,
                binarySearchIndexConfig = binarySearchIndexConfig,
                sortedIndexConfig = modifiedSortedIndex,
                valuesConfig = valuesConfig,
                segmentConfig = segmentConfig.singleton
              )

            assert(listSegments.size == 1, s"listSegments.size: ${listSegments.size} != 1")

            val listSegment = listSegments.head
            val listSegmentSize = listSegment.segmentSize

            val headerSize =
              ByteSizeOf.byte +
                Bytes.sizeOfUnsignedInt(listSegmentSize)

            val fileHeader = Slice.of[Byte](headerSize)
            fileHeader add PersistentSegmentMany.formatId
            fileHeader addUnsignedInt listSegmentSize

            assert(listSegment.minMaxFunctionId.isEmpty, "minMaxFunctionId was not empty")

            TransientSegment.Many(
              minKey = segments.head.minKey,
              maxKey = segments.last.maxKey,
              minMaxFunctionId = minMaxFunctionId,
              fileHeader = fileHeader,
              nearestPutDeadline = listSegment.nearestPutDeadline,
              listSegment = listSegment,
              segments = segments
            )
          }
      }
    }

  def writeOnes(mergeStats: MergeStats.Persistent.Closed[Iterable],
                createdInLevel: Int,
                bloomFilterConfig: BloomFilterBlock.Config,
                hashIndexConfig: HashIndexBlock.Config,
                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                sortedIndexConfig: SortedIndexBlock.Config,
                valuesConfig: ValuesBlock.Config,
                segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment.One] =
    if (mergeStats.isEmpty)
      Slice.empty
    else
      writeClosed(
        keyValues = mergeStats,
        createdInLevel = createdInLevel,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      ) map {
        segment =>
          Block.block(
            blocks = segment,
            compressions = segmentConfig.compressions(UncompressedBlockInfo(segment.segmentSize)),
            blockName = blockName
          )
      }

  def writeClosed(keyValues: MergeStats.Persistent.Closed[Iterable],
                  createdInLevel: Int,
                  bloomFilterConfig: BloomFilterBlock.Config,
                  hashIndexConfig: HashIndexBlock.Config,
                  binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                  sortedIndexConfig: SortedIndexBlock.Config,
                  valuesConfig: ValuesBlock.Config,
                  segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[ClosedBlocksWithFooter] =
    if (keyValues.isEmpty) {
      Slice.empty
    } else {
      //IMPORTANT! - The following is critical for compaction performance!

      //start sortedIndex for a new Segment.
      var sortedIndex =
        SortedIndexBlock.init(
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig
        )

      //start valuesBlock for a new Segment.
      var values =
        ValuesBlock.init(
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          builder = sortedIndex.builder
        )

      val keyValuesCount = keyValues.keyValuesCount

      val totalAllocatedSize = sortedIndex.compressibleBytes.allocatedSize + values.fold(0)(_.compressibleBytes.allocatedSize)
      val maxSegmentCountBasedOnSize = totalAllocatedSize / segmentConfig.minSize
      val maxSegmentCountBasedOnCount = keyValuesCount / segmentConfig.maxCount
      val maxSegmentsCount = (maxSegmentCountBasedOnSize max maxSegmentCountBasedOnCount) + 2
      val segments = Slice.of[ClosedBlocksWithFooter](maxSegmentsCount)

      def unwrittenTailSegmentBytes() =
        sortedIndex.compressibleBytes.unwrittenTailSize() + {
          if (values.isDefined)
            values.get.compressibleBytes.unwrittenTailSize()
          else
            0
        }

      //keys to write to bloomFilter.
      val bloomFilterIndexableKeys = ListBuffer.empty[Slice[Byte]]

      var totalProcessedCount = 0 //numbers of key-values written
      var processedInThisSegment = 0 //numbers of key-values written
      //start off with true for cases with keyValues are empty.
      //true if the following iteration exited after closing the Segment.
      var closed = true

      //start building the segment.
      keyValues.keyValues foreach {
        keyValue =>
          closed = false
          totalProcessedCount += 1
          processedInThisSegment += 1

          val comparableKey = keyOrder.comparableKey(keyValue.key)
          bloomFilterIndexableKeys += comparableKey

          SortedIndexBlock.write(keyValue = keyValue, state = sortedIndex)
          values foreach (ValuesBlock.write(keyValue, _))

          //Do not include SegmentFooterBlock.optimalBytesRequired here. Screws up the above max segments count estimation.
          var currentSegmentSize = sortedIndex.compressibleBytes.size
          values foreach (currentSegmentSize += _.compressibleBytes.size)

          //check and close segment if segment size limit is reached.
          //to do - maybe check if compression is defined and increase the segmentSize.
          def segmentSizeLimitReached: Boolean =
            currentSegmentSize >= segmentConfig.minSize && unwrittenTailSegmentBytes() > segmentConfig.minSize

          def segmentCountLimitReached: Boolean =
            processedInThisSegment >= segmentConfig.maxCount && (keyValuesCount - totalProcessedCount >= segmentConfig.maxCount)

          if (segmentCountLimitReached || segmentSizeLimitReached) {
            logger.debug(s"Creating segment of size: $currentSegmentSize.bytes. segmentCountLimitReached: $segmentCountLimitReached. segmentSizeLimitReached: $segmentSizeLimitReached")

            val (closedSegment, nextSortedIndex, nextValues) =
              writeSegmentBlock(
                createdInLevel = createdInLevel,
                hasMoreKeyValues = totalProcessedCount < keyValuesCount,
                bloomFilterIndexableKeys = bloomFilterIndexableKeys,
                sortedIndex = sortedIndex,
                values = values,
                bloomFilterConfig = bloomFilterConfig,
                hashIndexConfig = hashIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                sortedIndexConfig = sortedIndexConfig,
                valuesConfig = valuesConfig,
                prepareForCachingSegmentBlocksOnCreate = segmentConfig.cacheBlocksOnCreate
              )

            segments add closedSegment

            //segment's closed. Prepare for next Segment.
            bloomFilterIndexableKeys.clear() //clear bloomFilter keys.

            nextSortedIndex foreach { //set the newSortedIndex if it was created.
              newSortedIndex =>
                sortedIndex = newSortedIndex
            }

            values = nextValues
            processedInThisSegment = 0
            closed = true
          }
      }

      //if the segment was closed and all key-values were written then close Segment.
      if (closed) {
        segments
      } else {
        val (closedSegment, nextSortedIndex, nextValuesBlock) =
          writeSegmentBlock(
            createdInLevel = createdInLevel,
            hasMoreKeyValues = false,
            bloomFilterIndexableKeys = bloomFilterIndexableKeys,
            sortedIndex = sortedIndex,
            values = values,
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig,
            prepareForCachingSegmentBlocksOnCreate = segmentConfig.cacheBlocksOnCreate
          )

        //temporary check.
        assert(nextSortedIndex.isEmpty && nextValuesBlock.isEmpty, s"${nextSortedIndex.isEmpty} && ${nextValuesBlock.isEmpty} is not empty.")

        segments add closedSegment
      }
    }

  private def writeSegmentBlock(createdInLevel: Int,
                                hasMoreKeyValues: Boolean,
                                bloomFilterIndexableKeys: ListBuffer[Slice[Byte]],
                                sortedIndex: SortedIndexBlock.State,
                                values: Option[ValuesBlock.State],
                                bloomFilterConfig: BloomFilterBlock.Config,
                                hashIndexConfig: HashIndexBlock.Config,
                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                sortedIndexConfig: SortedIndexBlock.Config,
                                valuesConfig: ValuesBlock.Config,
                                prepareForCachingSegmentBlocksOnCreate: Boolean): (ClosedBlocksWithFooter, Option[SortedIndexBlock.State], Option[ValuesBlock.State]) = {
    //tail bytes before closing and compression is applied.
    val unwrittenTailSortedIndexBytes = sortedIndex.compressibleBytes.unwrittenTail()
    val unwrittenTailValueBytes = values.map(_.compressibleBytes.unwrittenTail())

    val closedBlocks =
      closeBlocks(
        sortedIndex = sortedIndex,
        values = values,
        bloomFilterIndexableKeys = bloomFilterIndexableKeys,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        prepareForCachingSegmentBlocksOnCreate = prepareForCachingSegmentBlocksOnCreate
      )

    val footer =
      SegmentFooterBlock.init(
        keyValuesCount = closedBlocks.sortedIndex.entriesCount,
        rangesCount = closedBlocks.sortedIndex.rangeCount,
        hasPut = closedBlocks.sortedIndex.hasPut,
        createdInLevel = createdInLevel
      )

    val closedFooter: SegmentFooterBlock.State =
      SegmentFooterBlock.writeAndClose(
        state = footer,
        closedBlocks = closedBlocks
      )

    val ref =
      new ClosedBlocksWithFooter(
        minKey = closedBlocks.sortedIndex.minKey,
        maxKey = closedBlocks.sortedIndex.maxKey,

        functionMinMax = closedBlocks.minMaxFunction,

        nearestDeadline = closedBlocks.nearestDeadline,
        valuesBlockHeader = closedBlocks.values.map(_.header.close()),

        valuesBlock = closedBlocks.values.map(_.compressibleBytes.close()),
        valuesUnblockedReader = closedBlocks.valuesUnblockedReader,

        sortedIndexBlockHeader = closedBlocks.sortedIndex.header.close(),
        sortedIndexBlock = closedBlocks.sortedIndex.compressibleBytes.close(),
        sortedIndexUnblockedReader = closedBlocks.sortedIndexUnblockedReader,
        sortedIndexClosedState = closedBlocks.sortedIndex,

        hashIndexBlockHeader = closedBlocks.hashIndex map (_.header.close()),
        hashIndexBlock = closedBlocks.hashIndex map (_.compressibleBytes.close()),
        hashIndexUnblockedReader = closedBlocks.hashIndexUnblockedReader,

        binarySearchIndexBlockHeader = closedBlocks.binarySearchIndex map (_.header.close()),
        binarySearchIndexBlock = closedBlocks.binarySearchIndex map (_.compressibleBytes.close()),
        binarySearchUnblockedReader = closedBlocks.binarySearchUnblockedReader,

        bloomFilterBlockHeader = closedBlocks.bloomFilter map (_.header.close()),
        bloomFilterBlock = closedBlocks.bloomFilter map (_.compressibleBytes.close()),
        bloomFilterUnblockedReader = closedBlocks.bloomFilterUnblockedReader,

        footerBlock = closedFooter.bytes.close()
      )

    //start new sortedIndex block only if there are more key-values to process
    val newSortedIndex =
      if (hasMoreKeyValues)
        Some(
          SortedIndexBlock.init(
            bytes = unwrittenTailSortedIndexBytes,
            compressDuplicateValues = valuesConfig.compressDuplicateValues,
            compressDuplicateRangeValues = valuesConfig.compressDuplicateRangeValues,
            sortedIndexConfig = sortedIndexConfig
          )
        )
      else
        None

    //start new values block only if there are more key-values to process
    val newValues =
      if (hasMoreKeyValues)
        newSortedIndex flatMap {
          newSortedIndex =>
            unwrittenTailValueBytes map {
              tailValueBytes =>
                ValuesBlock.init(
                  bytes = tailValueBytes,
                  valuesConfig = valuesConfig,
                  builder = newSortedIndex.builder
                )
            }
        }
      else
        None

    (ref, newSortedIndex, newValues)
  }

  private def closeBlocks(sortedIndex: SortedIndexBlock.State,
                          values: Option[ValuesBlock.State],
                          bloomFilterIndexableKeys: ListBuffer[Slice[Byte]],
                          bloomFilterConfig: BloomFilterBlock.Config,
                          hashIndexConfig: HashIndexBlock.Config,
                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                          prepareForCachingSegmentBlocksOnCreate: Boolean): ClosedBlocks = {
    val sortedIndexState = SortedIndexBlock.close(sortedIndex)
    val valuesState = values map ValuesBlock.close

    val bloomFilter =
      if (sortedIndexState.hasRemoveRange || bloomFilterIndexableKeys.size < bloomFilterConfig.minimumNumberOfKeys)
        None
      else
        BloomFilterBlock.init(
          numberOfKeys = bloomFilterIndexableKeys.size,
          falsePositiveRate = bloomFilterConfig.falsePositiveRate,
          updateMaxProbe = bloomFilterConfig.optimalMaxProbe,
          compressions = bloomFilterConfig.compressions
        )

    val hashIndex =
      HashIndexBlock.init(
        sortedIndexState = sortedIndexState,
        hashIndexConfig = hashIndexConfig
      )

    val binarySearchIndex =
      BinarySearchIndexBlock.init(
        sortedIndexState = sortedIndexState,
        binarySearchConfig = binarySearchIndexConfig
      )

    if (hashIndex.isDefined || binarySearchIndex.isDefined)
      sortedIndexState.secondaryIndexEntries foreach {
        indexEntry =>
          val hit =
            if (hashIndex.isDefined)
              HashIndexBlock.write(
                entry = indexEntry,
                state = hashIndex.get
              )
            else
              false

          //if it's a hit and binary search is not configured to be full.
          //no need to check if the value was previously written to binary search here since BinarySearchIndexBlock itself performs this check.
          if (binarySearchIndex.isDefined && (binarySearchIndexConfig.fullIndex || !hit))
            BinarySearchIndexBlock.write(
              entry = indexEntry,
              state = binarySearchIndex.get
            )
      }

    bloomFilter foreach {
      bloomFilter =>
        bloomFilterIndexableKeys foreach {
          key =>
            BloomFilterBlock.add(key, bloomFilter)
        }
    }

    new ClosedBlocks(
      sortedIndex = sortedIndexState,
      values = valuesState,
      hashIndex = hashIndex.flatMap(HashIndexBlock.close),
      binarySearchIndex = binarySearchIndex.flatMap(state => BinarySearchIndexBlock.close(state, sortedIndexState.uncompressedPrefixCount)),
      bloomFilter = bloomFilter.flatMap(BloomFilterBlock.close),
      minMaxFunction = sortedIndexState.minMaxFunctionId,
      prepareForCachingSegmentBlocksOnCreate = prepareForCachingSegmentBlocksOnCreate
    )
  }

  implicit object SegmentBlockOps extends BlockOps[SegmentBlock.Offset, SegmentBlock] {
    override def updateBlockOffset(block: SegmentBlock, start: Int, size: Int): SegmentBlock =
      block.copy(offset = SegmentBlock.Offset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      SegmentBlock.Offset(start, size)

    override def readBlock(header: Block.Header[SegmentBlock.Offset]): SegmentBlock =
      SegmentBlock.read(header)
  }

}

private[core] case class SegmentBlock(offset: SegmentBlock.Offset,
                                      headerSize: Int,
                                      compressionInfo: Option[Block.CompressionInfo]) extends Block[SegmentBlock.Offset]


