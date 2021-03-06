/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.segment.block.segment

import com.typesafe.scalalogging.LazyLogging
import swaydb.Compression
import swaydb.compression.CompressionInternal
import swaydb.core.data.Memory
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.block._
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.data.{ClosedBlocks, TransientSegment, TransientSegmentRef}
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.{PersistentSegmentMany, PersistentSegmentOne}
import swaydb.core.util.{Bytes, Collections, MinMax}
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.Futures._
import swaydb.utils.{ByteSizeOf, Futures}

import scala.collection.compat._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
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
        maxCount = config.segmentFormat.count,
        segmentRefCacheLife = config.segmentFormat.segmentRefCacheLife,
        enableHashIndexForListSegment = config.segmentFormat.enableRootHashIndex,
        initialiseIteratorsInOneSeek = config.initialiseIteratorsInOneSeek,
        mmap = config.mmap,
        deleteDelay = config.deleteDelay,
        compressions = config.compression
      )

    def apply(fileOpenIOStrategy: IOStrategy.ThreadSafe,
              blockIOStrategy: IOAction => IOStrategy,
              cacheBlocksOnCreate: Boolean,
              minSize: Int,
              maxCount: Int,
              segmentRefCacheLife: SegmentRefCacheLife,
              enableHashIndexForListSegment: Boolean,
              initialiseIteratorsInOneSeek: Boolean,
              mmap: MMAP.Segment,
              deleteDelay: FiniteDuration,
              compressions: UncompressedBlockInfo => Iterable[Compression]): Config =
      applyInternal(
        fileOpenIOStrategy = fileOpenIOStrategy,
        blockIOStrategy = blockIOStrategy,
        cacheBlocksOnCreate = cacheBlocksOnCreate,
        minSize = minSize,
        maxCount = maxCount,
        segmentRefCacheLife = segmentRefCacheLife,
        enableHashIndexForListSegment = enableHashIndexForListSegment,
        initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek,
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
                                    segmentRefCacheLife: SegmentRefCacheLife,
                                    enableHashIndexForListSegment: Boolean,
                                    initialiseIteratorsInOneSeek: Boolean,
                                    mmap: MMAP.Segment,
                                    deleteDelay: FiniteDuration,
                                    compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): Config =
      new Config(
        fileOpenIOStrategy = fileOpenIOStrategy,
        blockIOStrategy = blockIOStrategy,
        cacheBlocksOnCreate = cacheBlocksOnCreate,
        minSize = minSize max 1,
        maxCount = maxCount max 1,
        segmentRefCacheLife = segmentRefCacheLife,
        enableHashIndexForListSegment = enableHashIndexForListSegment,
        initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek,
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
                       val segmentRefCacheLife: SegmentRefCacheLife,
                       val enableHashIndexForListSegment: Boolean,
                       val initialiseIteratorsInOneSeek: Boolean,
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
        segmentRefCacheLife = segmentRefCacheLife,
        enableHashIndexForListSegment = enableHashIndexForListSegment,
        initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek,
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

  def writeOneOrMany(mergeStats: MergeStats.Persistent.Closed[IterableOnce],
                     createdInLevel: Int,
                     bloomFilterConfig: BloomFilterBlock.Config,
                     hashIndexConfig: HashIndexBlock.Config,
                     binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                     sortedIndexConfig: SortedIndexBlock.Config,
                     valuesConfig: ValuesBlock.Config,
                     segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         ec: ExecutionContext,
                                                         compactionParallelism: CompactionParallelism): Future[Slice[TransientSegment.OneOrRemoteRefOrMany]] =
    if (mergeStats.isEmpty)
      Future.successful(Slice.empty)
    else
      writeOnes(
        mergeStats = mergeStats,
        createdInLevel = createdInLevel,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      ) flatMap {
        ones =>
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
                     ones: Slice[TransientSegment.OneOrRemoteRef],
                     sortedIndexConfig: SortedIndexBlock.Config,
                     hashIndexConfig: HashIndexBlock.Config,
                     binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                     valuesConfig: ValuesBlock.Config,
                     segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         ec: ExecutionContext,
                                                         compactionParallelism: CompactionParallelism): Future[Slice[TransientSegment.OneOrRemoteRefOrMany]] =
    if (ones.isEmpty)
      Future.successful(Slice.empty)
    else
      Future
        .unit
        .flatMapUnit {
          val groups: Slice[Slice[TransientSegment.OneOrRemoteRef]] =
            Collections.groupedBySize[TransientSegment.OneOrRemoteRef](
              minGroupSize = segmentConfig.minSize,
              itemSize = _.segmentSize,
              items = ones
            )

          //FIXME: DOES NOT WORK WITH 2.12
          //          //Be explicit since we know the maximum size already.
          //          val buildFrom =
          //            new CanBuildFrom[Slice[Slice[TransientSegment.OneOrRemoteRef]], TransientSegment.OneOrRemoteRefOrMany, Slice[TransientSegment.OneOrRemoteRefOrMany]] {
          //              override def fromSpecific(from: Slice[Slice[TransientSegment.OneOrRemoteRef]])(it: IterableOnce[TransientSegment.OneOrRemoteRefOrMany]): Slice[TransientSegment.OneOrRemoteRefOrMany] =
          //                Slice.of[TransientSegment.OneOrRemoteRefOrMany](groups.size)
          //
          //              override def newBuilder(from: Slice[Slice[TransientSegment.OneOrRemoteRef]]): mutable.Builder[TransientSegment.OneOrRemoteRefOrMany, Slice[TransientSegment.OneOrRemoteRefOrMany]] =
          //                Slice.newBuilder(groups.size)
          //            }

          Future.traverse(groups)(
            segments =>
              writeGroupedOneOrMany(
                createdInLevel = createdInLevel,
                segments = segments,
                sortedIndexConfig = sortedIndexConfig,
                hashIndexConfig = hashIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                valuesConfig = valuesConfig,
                segmentConfig = segmentConfig
              )
          )
        }

  /**
   * Segments are already groups and ready to persisted.
   */
  private def writeGroupedOneOrMany(createdInLevel: Int,
                                    segments: Slice[TransientSegment.OneOrRemoteRef],
                                    sortedIndexConfig: SortedIndexBlock.Config,
                                    hashIndexConfig: HashIndexBlock.Config,
                                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                    valuesConfig: ValuesBlock.Config,
                                    segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                        ec: ExecutionContext,
                                                                        compactionParallelism: CompactionParallelism): Future[TransientSegment.OneOrRemoteRefOrMany] =
    if (segments.size == 1)
      Future.successful(segments.head.copyWithFileHeader(headerBytes = PersistentSegmentOne.formatIdSlice))
    else
      Future
        .unit
        .flatMapUnit {
          val listKeyValue: MergeStats.Persistent.Builder[Memory, Slice] =
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

          writeOnes(
            mergeStats = closedListKeyValues,
            createdInLevel = createdInLevel,
            bloomFilterConfig = BloomFilterBlock.Config.disabled,
            hashIndexConfig = if (segmentConfig.enableHashIndexForListSegment) hashIndexConfig else HashIndexBlock.Config.disabled,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = modifiedSortedIndex,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig.singleton
          ) map {
            listSegments =>
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

  def writeOnes(mergeStats: MergeStats.Persistent.Closed[IterableOnce],
                createdInLevel: Int,
                bloomFilterConfig: BloomFilterBlock.Config,
                hashIndexConfig: HashIndexBlock.Config,
                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                sortedIndexConfig: SortedIndexBlock.Config,
                valuesConfig: ValuesBlock.Config,
                segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                    ec: ExecutionContext,
                                                    compactionParallelism: CompactionParallelism): Future[Slice[TransientSegment.One]] =
    if (mergeStats.isEmpty)
      Future.successful(Slice.empty)
    else
      writeSegmentRefs(
        mergeStats = mergeStats,
        createdInLevel = createdInLevel,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      ) flatMap {
        segments =>
          Future.traverse(segments) {
            segment =>
              Future {
                Block.block(
                  blocks = segment,
                  compressions = segmentConfig.compressions(UncompressedBlockInfo(segment.segmentSize)),
                  blockName = blockName
                )
              }
          }
      }

  def writeSegmentRefs(mergeStats: MergeStats.Persistent.Closed[IterableOnce],
                       createdInLevel: Int,
                       bloomFilterConfig: BloomFilterBlock.Config,
                       hashIndexConfig: HashIndexBlock.Config,
                       binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                       sortedIndexConfig: SortedIndexBlock.Config,
                       valuesConfig: ValuesBlock.Config,
                       segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                           ec: ExecutionContext): Future[Slice[TransientSegmentRef]] =
    if (mergeStats.isEmpty)
      Future.successful(Slice.empty)
    else
      Future
        .unit
        .flatMapUnit {
          //IMPORTANT! - The following is critical for compaction performance!

          //start sortedIndex for a new Segment.
          var sortedIndex =
            SortedIndexBlock.init(
              stats = mergeStats,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig
            )

          //start valuesBlock for a new Segment.
          var values =
            ValuesBlock.init(
              stats = mergeStats,
              valuesConfig = valuesConfig,
              builder = sortedIndex.builder
            )

          val keyValuesCount = mergeStats.keyValuesCount

          val segments = ListBuffer.empty[Future[TransientSegmentRef]]

          def unwrittenTailSegmentBytes() =
            sortedIndex.compressibleBytes.unwrittenTailSize() + {
              if (values.isDefined)
                values.get.compressibleBytes.unwrittenTailSize()
              else
                0
            }

          //keys to write to bloomFilter.
          var bloomFilterIndexableKeys = ListBuffer.empty[Slice[Byte]]

          var totalProcessedCount = 0 //numbers of key-values written
          var processedInThisSegment = 0 //numbers of key-values written
          //start off with true for cases with keyValues are empty.
          //true if the following iteration exited after closing the Segment.
          var closed = true

          //start building the segment.
          for (keyValue <- mergeStats.keyValues) {
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
                writeSegmentRef(
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

                segments += closedSegment

              //segment's closed. Prepare for next Segment.
              bloomFilterIndexableKeys = ListBuffer.empty[Slice[Byte]] //clear bloomFilter keys.

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
            Slice.sequence(segments)
          } else {
            val (segmentRef, nextSortedIndex, nextValuesBlock) =
              writeSegmentRef(
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

            segments += segmentRef

            Slice.sequence(segments)
          }
        }

  private def writeSegmentRef(createdInLevel: Int,
                              hasMoreKeyValues: Boolean,
                              bloomFilterIndexableKeys: Iterable[Slice[Byte]],
                              sortedIndex: SortedIndexBlock.State,
                              values: Option[ValuesBlock.State],
                              bloomFilterConfig: BloomFilterBlock.Config,
                              hashIndexConfig: HashIndexBlock.Config,
                              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                              sortedIndexConfig: SortedIndexBlock.Config,
                              valuesConfig: ValuesBlock.Config,
                              prepareForCachingSegmentBlocksOnCreate: Boolean)(implicit ec: ExecutionContext): (Future[TransientSegmentRef], Option[SortedIndexBlock.State], Option[ValuesBlock.State]) = {
    //tail bytes before closing and compression is applied.
    val unwrittenTailSortedIndexBytes = sortedIndex.compressibleBytes.unwrittenTail()
    val unwrittenTailValueBytes = values.map(_.compressibleBytes.unwrittenTail())

    //Run closeBlocks concurrently in another thread while the
    //next slot of sortedIndex and valuesBlock is being created.

    val ref: Future[TransientSegmentRef] =
      closeBlocks(
        sortedIndex = sortedIndex,
        values = values,
        bloomFilterIndexableKeys = bloomFilterIndexableKeys,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        prepareForCachingSegmentBlocksOnCreate = prepareForCachingSegmentBlocksOnCreate
      ) map {
        closedBlocks =>
          val footer =
            SegmentFooterBlock.init(
              keyValuesCount = closedBlocks.sortedIndex.entriesCount,
              rangesCount = closedBlocks.sortedIndex.rangeCount,
              updateCount = closedBlocks.sortedIndex.updateCount,
              putCount = closedBlocks.sortedIndex.putCount,
              putDeadlineCount = closedBlocks.sortedIndex.putDeadlineCount,
              createdInLevel = createdInLevel
            )

          val closedFooter: SegmentFooterBlock.State =
            SegmentFooterBlock.writeAndClose(
              state = footer,
              closedBlocks = closedBlocks
            )

          new TransientSegmentRef(
            minKey = closedBlocks.sortedIndex.minKey,
            maxKey = closedBlocks.sortedIndex.maxKey,

            functionMinMax = closedBlocks.minMaxFunction,

            nearestDeadline = closedBlocks.nearestDeadline,
            updateCount = closedBlocks.sortedIndex.updateCount,
            putCount = closedBlocks.sortedIndex.putCount,
            putDeadlineCount = closedBlocks.sortedIndex.putDeadlineCount,
            rangeCount = closedBlocks.sortedIndex.rangeCount,
            keyValueCount = closedBlocks.sortedIndex.entriesCount,
            createdInLevel = createdInLevel,

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
      }

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

  /**
   * Build closed Blocks and runs each block index concurrently.
   */
  private def closeBlocks(sortedIndex: SortedIndexBlock.State,
                          values: Option[ValuesBlock.State],
                          bloomFilterIndexableKeys: Iterable[Slice[Byte]],
                          bloomFilterConfig: BloomFilterBlock.Config,
                          hashIndexConfig: HashIndexBlock.Config,
                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                          prepareForCachingSegmentBlocksOnCreate: Boolean)(implicit ec: ExecutionContext): Future[ClosedBlocks] =
    Future
      .unit
      .flatMapUnit {
        //close the Values block first since no other block depends on it
        val closedValuesFuture =
          if (values.isDefined)
            Future(Some(ValuesBlock.close(values.get)))
          else
            Futures.none

        //close bloomFilter since no other block depends on it
        val closedBloomFilterFuture =
          if (sortedIndex.mightContainRemoveRange || bloomFilterIndexableKeys.size < bloomFilterConfig.minimumNumberOfKeys)
            Futures.none
          else
            Future { //build bloomFilter concurrently
              val bloomFilterOption =
                BloomFilterBlock.init(
                  numberOfKeys = bloomFilterIndexableKeys.size,
                  falsePositiveRate = bloomFilterConfig.falsePositiveRate,
                  updateMaxProbe = bloomFilterConfig.optimalMaxProbe,
                  compressions = bloomFilterConfig.compressions
                )

              if (bloomFilterOption.isDefined) {
                val bloomFilter = bloomFilterOption.get

                for (comparableKey <- bloomFilterIndexableKeys)
                  BloomFilterBlock.add(
                    comparableKey = comparableKey,
                    state = bloomFilter
                  )

                BloomFilterBlock.close(bloomFilter)
              } else {
                None
              }
            }

        //close sorted index and initialise build hashIndex and binarySearchIndex blocks
        val closedSortedIndex = SortedIndexBlock.close(sortedIndex)

        closeIndexBlocks(
          closedSortedIndex = closedSortedIndex,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig
        ) flatMap {
          case (closedHashIndex, closedBinarySearchIndex) =>
            for {
              closedBloomFilter <- closedBloomFilterFuture
              closedValues <- closedValuesFuture
            } yield {
              new ClosedBlocks(
                sortedIndex = closedSortedIndex,
                values = closedValues,
                hashIndex = closedHashIndex,
                binarySearchIndex = closedBinarySearchIndex,
                bloomFilter = closedBloomFilter,
                minMaxFunction = closedSortedIndex.minMaxFunctionId,
                prepareForCachingSegmentBlocksOnCreate = prepareForCachingSegmentBlocksOnCreate
              )
            }
        }
      }

  /**
   * Pre-condition: Input [[SortedIndexBlock.State]] MUST be closed.
   *
   * Concurrently builds hashIndex and binarySearch index.
   */
  private def closeIndexBlocks(closedSortedIndex: SortedIndexBlock.State, //SortedIndexBlock must be closed
                               hashIndexConfig: HashIndexBlock.Config,
                               binarySearchIndexConfig: BinarySearchIndexBlock.Config)(implicit ec: ExecutionContext): Future[(Option[HashIndexBlock.State], Option[BinarySearchIndexBlock.State])] =
    Future
      .unit
      .flatMapUnit {
        //initialise hashIndex
        val hashIndexOption =
          HashIndexBlock.init(
            sortedIndexState = closedSortedIndex,
            hashIndexConfig = hashIndexConfig
          )

        //initialise binarySearchIndex
        val binarySearchIndexOption =
          BinarySearchIndexBlock.init(
            sortedIndexState = closedSortedIndex,
            binarySearchConfig = binarySearchIndexConfig
          )

        //if either one is defined then build the index
        if (hashIndexOption.isDefined || binarySearchIndexOption.isDefined)
          if (binarySearchIndexConfig.fullIndex) {
            //fullIndex is required for binary search for binarySearch has no dependency
            //on hashIndex therefore we can build both indexes concurrently

            val hashIndexFuture: Future[Option[HashIndexBlock.State]] =
              if (hashIndexOption.isDefined)
                Future {
                  val hashIndexState = hashIndexOption.get

                  for (indexEntry <- closedSortedIndex.secondaryIndexEntries)
                    HashIndexBlock.write(
                      entry = indexEntry,
                      state = hashIndexState
                    )

                  HashIndexBlock.close(hashIndexState)
                }
              else
                Futures.none

            //concurrently build binarySearch
            val binarySearchIndexFuture: Future[Option[BinarySearchIndexBlock.State]] =
              if (binarySearchIndexOption.isDefined)
                Future {
                  val binarySearchIndexState = binarySearchIndexOption.get

                  for (indexEntry <- closedSortedIndex.secondaryIndexEntries)
                    BinarySearchIndexBlock.write(
                      entry = indexEntry,
                      state = binarySearchIndexState
                    )

                  BinarySearchIndexBlock.close(binarySearchIndexState, closedSortedIndex.uncompressedPrefixCount)
                }
              else
                Futures.none

            hashIndexFuture.join(binarySearchIndexFuture)
          } else {
            //else binarySearch is partial index and has dependency on hashIndex
            Future {
              for (indexEntry <- closedSortedIndex.secondaryIndexEntries) {
                //write to hashIndex first
                val hit =
                  if (hashIndexOption.isDefined)
                    HashIndexBlock.write(
                      entry = indexEntry,
                      state = hashIndexOption.get
                    )
                  else
                    false

                //if it's a hit and binary search is not configured to be full.
                //no need to check if the value was previously written to binary search here since BinarySearchIndexBlock itself performs this check.
                if (binarySearchIndexOption.isDefined && !hit)
                  BinarySearchIndexBlock.write(
                    entry = indexEntry,
                    state = binarySearchIndexOption.get
                  )
              }
            } flatMapUnit {
              //finally close both indexes concurrently.
              val hashIndexFuture =
                if (hashIndexOption.isDefined)
                  Future(HashIndexBlock.close(hashIndexOption.get))
                else
                  Futures.none

              val binarySearchIndexFuture =
                if (binarySearchIndexOption.isDefined)
                  Future(BinarySearchIndexBlock.close(binarySearchIndexOption.get, closedSortedIndex.uncompressedPrefixCount))
                else
                  Futures.none

              hashIndexFuture.join(binarySearchIndexFuture)
            }
          }
        else
          Future.successful((None, None))
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
