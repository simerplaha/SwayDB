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

package swaydb.core.segment.block.segment

import com.typesafe.scalalogging.LazyLogging
import swaydb.config._
import swaydb.core.segment.{PersistentSegmentMany, PersistentSegmentOne}
import swaydb.core.segment.block._
import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockConfig, BinarySearchIndexBlockState, BinarySearchIndexBlockStateOption}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockConfig, BloomFilterBlockState}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockConfig, HashIndexBlockState, HashIndexBlockStateOption}
import swaydb.core.segment.block.segment.footer.{SegmentFooterBlock, SegmentFooterBlockState}
import swaydb.core.segment.block.segment.transient.{ClosedBlocks, TransientSegment, TransientSegmentRef}
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig, SortedIndexBlockState, SortedIndexBlockStateOption}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockConfig, ValuesBlockState, ValuesBlockStateOption}
import swaydb.core.segment.data.Memory
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.util.{Bytes, MinMax}
import swaydb.slice.{Slice, SliceOption}
import swaydb.slice.order.KeyOrder
import swaydb.utils.ByteSizeOf
import swaydb.utils.Futures._

import scala.collection.compat._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

private[core] case object SegmentBlock extends LazyLogging {

  val blockName = this.productPrefix

  val formatId: Byte = 1.toByte

  val crcBytes: Int = 13

  def read(header: BlockHeader[SegmentBlockOffset]): SegmentBlock =
    SegmentBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )

  def writeOneOrMany(mergeStats: MergeStats.Persistent.Closed[IterableOnce],
                     createdInLevel: Int,
                     bloomFilterConfig: BloomFilterBlockConfig,
                     hashIndexConfig: HashIndexBlockConfig,
                     binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                     sortedIndexConfig: SortedIndexBlockConfig,
                     valuesConfig: ValuesBlockConfig,
                     segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        ec: ExecutionContext): Future[Slice[TransientSegment.OneOrManyOrRemoteRef]] =
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
                     sortedIndexConfig: SortedIndexBlockConfig,
                     hashIndexConfig: HashIndexBlockConfig,
                     binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                     valuesConfig: ValuesBlockConfig,
                     segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        ec: ExecutionContext): Future[Slice[TransientSegment.OneOrManyOrRemoteRef]] =
    if (ones.isEmpty)
      Future.successful(Slice.empty)
    else
      Future
        .unit
        .flatMapUnit {
          val groups: Slice[Slice[TransientSegment.OneOrRemoteRef]] =
            Slice.groupedBySize[TransientSegment.OneOrRemoteRef](
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
                                    sortedIndexConfig: SortedIndexBlockConfig,
                                    hashIndexConfig: HashIndexBlockConfig,
                                    binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                                    valuesConfig: ValuesBlockConfig,
                                    segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                       ec: ExecutionContext): Future[TransientSegment.OneOrManyOrRemoteRef] =
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
            bloomFilterConfig = BloomFilterBlockConfig.disabled(),
            hashIndexConfig = if (segmentConfig.enableHashIndexForListSegment) hashIndexConfig else HashIndexBlockConfig.disabled(),
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

              val fileHeader = Slice.allocate[Byte](headerSize)
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
                bloomFilterConfig: BloomFilterBlockConfig,
                hashIndexConfig: HashIndexBlockConfig,
                binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                sortedIndexConfig: SortedIndexBlockConfig,
                valuesConfig: ValuesBlockConfig,
                segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   ec: ExecutionContext): Future[Slice[TransientSegment.One]] =
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
                  segment = segment,
                  compressions = segmentConfig.compressions(UncompressedBlockInfo(segment.segmentSizeWithoutHeader)),
                  blockName = blockName
                )
              }
          }
      }

  def writeSegmentRefs(mergeStats: MergeStats.Persistent.Closed[IterableOnce],
                       createdInLevel: Int,
                       bloomFilterConfig: BloomFilterBlockConfig,
                       hashIndexConfig: HashIndexBlockConfig,
                       binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                       sortedIndexConfig: SortedIndexBlockConfig,
                       valuesConfig: ValuesBlockConfig,
                       segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
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
              if (values.isSomeS)
                values.getS.compressibleBytes.unwrittenTailSize()
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
            values foreachS (ValuesBlock.write(keyValue, _))

            //Do not include SegmentFooterBlock.optimalBytesRequired here. Screws up the above max segments count estimation.
            var currentSegmentSize = sortedIndex.compressibleBytes.size
            values foreachS (currentSegmentSize += _.compressibleBytes.size)

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

              nextSortedIndex foreachS { //set the newSortedIndex if it was created.
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
            assert(nextSortedIndex.isNoneS && nextValuesBlock.isNoneS, s"${nextSortedIndex.isNoneS} && ${nextValuesBlock.isNoneS} is not empty.")

            segments += segmentRef

            Slice.sequence(segments)
          }
        }

  private def writeSegmentRef(createdInLevel: Int,
                              hasMoreKeyValues: Boolean,
                              bloomFilterIndexableKeys: Iterable[Slice[Byte]],
                              sortedIndex: SortedIndexBlockState,
                              values: ValuesBlockStateOption,
                              bloomFilterConfig: BloomFilterBlockConfig,
                              hashIndexConfig: HashIndexBlockConfig,
                              binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                              sortedIndexConfig: SortedIndexBlockConfig,
                              valuesConfig: ValuesBlockConfig,
                              prepareForCachingSegmentBlocksOnCreate: Boolean)(implicit ec: ExecutionContext): (Future[TransientSegmentRef], SortedIndexBlockStateOption, ValuesBlockStateOption) = {
    //tail bytes before closing and compression is applied.
    val unwrittenTailSortedIndexBytes = sortedIndex.compressibleBytes.unwrittenTail()
    val unwrittenTailValueBytes = values.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.compressibleBytes.unwrittenTail())

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

          val closedFooter: SegmentFooterBlockState =
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

            valuesBlockHeader = closedBlocks.values.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.header.close()),

            valuesBlock = closedBlocks.values.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.compressibleBytes.close()),
            valuesUnblockedReader = closedBlocks.valuesUnblockedReader,

            sortedIndexBlockHeader = closedBlocks.sortedIndex.header.close(),
            sortedIndexBlock = closedBlocks.sortedIndex.compressibleBytes.close(),
            sortedIndexUnblockedReader = closedBlocks.sortedIndexUnblockedReader,
            sortedIndexClosedState = closedBlocks.sortedIndex,

            hashIndexBlockHeader = closedBlocks.hashIndex.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.header.close()),
            hashIndexBlock = closedBlocks.hashIndex.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.compressibleBytes.close()),
            hashIndexUnblockedReader = closedBlocks.hashIndexUnblockedReader,

            binarySearchIndexBlockHeader = closedBlocks.binarySearchIndex.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.header.close()),
            binarySearchIndexBlock = closedBlocks.binarySearchIndex.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.compressibleBytes.close()),
            binarySearchUnblockedReader = closedBlocks.binarySearchUnblockedReader,

            bloomFilterBlockHeader = closedBlocks.bloomFilter.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.header.close()),
            bloomFilterBlock = closedBlocks.bloomFilter.flatMapSomeS[SliceOption[Byte]](Slice.Null)(_.compressibleBytes.close()),
            bloomFilterUnblockedReader = closedBlocks.bloomFilterUnblockedReader,

            footerBlock = closedFooter.bytes.close()
          )
      }

    //start new sortedIndex block only if there are more key-values to process
    val newSortedIndex =
      if (hasMoreKeyValues)
        SortedIndexBlock.init(
          bytes = unwrittenTailSortedIndexBytes,
          compressDuplicateValues = valuesConfig.compressDuplicateValues,
          compressDuplicateRangeValues = valuesConfig.compressDuplicateRangeValues,
          sortedIndexConfig = sortedIndexConfig
        )
      else
        SortedIndexBlockState.Null

    //start new values block only if there are more key-values to process
    val newValues: ValuesBlockStateOption =
      if (hasMoreKeyValues)
        newSortedIndex.flatMapSomeS[ValuesBlockStateOption](ValuesBlockState.Null) {
          newSortedIndex =>
            unwrittenTailValueBytes.flatMapSomeC[ValuesBlockStateOption](ValuesBlockState.Null) {
              tailValueBytes =>
                ValuesBlock.init(
                  bytes = tailValueBytes.asMut(),
                  valuesConfig = valuesConfig,
                  builder = newSortedIndex.builder
                )
            }
        }
      else
        ValuesBlockState.Null

    (ref, newSortedIndex, newValues)
  }

  /**
   * Build closed Blocks and runs each block index concurrently.
   */
  private def closeBlocks(sortedIndex: SortedIndexBlockState,
                          values: ValuesBlockStateOption,
                          bloomFilterIndexableKeys: Iterable[Slice[Byte]],
                          bloomFilterConfig: BloomFilterBlockConfig,
                          hashIndexConfig: HashIndexBlockConfig,
                          binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                          prepareForCachingSegmentBlocksOnCreate: Boolean)(implicit ec: ExecutionContext): Future[ClosedBlocks] =
    Future
      .unit
      .flatMapUnit {
        //close the Values block first since no other block depends on it
        val closedValuesFuture =
          if (values.isSomeS)
            Future(ValuesBlock.close(values.getS))
          else
            Future.successful(ValuesBlockState.Null)

        //close bloomFilter since no other block depends on it
        val closedBloomFilterFuture =
          if (sortedIndex.mightContainRemoveRange || bloomFilterIndexableKeys.size < bloomFilterConfig.minimumNumberOfKeys)
            Future.successful(BloomFilterBlockState.Null)
          else
            Future { //build bloomFilter concurrently
              val bloomFilterOption =
                BloomFilterBlock.init(
                  numberOfKeys = bloomFilterIndexableKeys.size,
                  falsePositiveRate = bloomFilterConfig.falsePositiveRate,
                  updateMaxProbe = bloomFilterConfig.optimalMaxProbe,
                  compressions = bloomFilterConfig.compressions
                )

              if (bloomFilterOption.isSomeS) {
                val bloomFilter = bloomFilterOption.getS

                for (comparableKey <- bloomFilterIndexableKeys)
                  BloomFilterBlock.add(
                    comparableKey = comparableKey,
                    state = bloomFilter
                  )

                BloomFilterBlock.close(bloomFilter)
              } else {
                BloomFilterBlockState.Null
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
   * Pre-condition: Input [[SortedIndexBlockState]] MUST be closed.
   *
   * Concurrently builds hashIndex and binarySearch index.
   */
  private def closeIndexBlocks(closedSortedIndex: SortedIndexBlockState, //SortedIndexBlock must be closed
                               hashIndexConfig: HashIndexBlockConfig,
                               binarySearchIndexConfig: BinarySearchIndexBlockConfig)(implicit ec: ExecutionContext): Future[(HashIndexBlockStateOption, BinarySearchIndexBlockStateOption)] =
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
        if (hashIndexOption.isSomeS || binarySearchIndexOption.isSomeS)
          if (binarySearchIndexConfig.fullIndex) {
            //fullIndex is required for binary search for binarySearch has no dependency
            //on hashIndex therefore we can build both indexes concurrently

            val hashIndexFuture: Future[HashIndexBlockStateOption] =
              if (hashIndexOption.isSomeS)
                Future {
                  val hashIndexState = hashIndexOption.getS

                  for (indexEntry <- closedSortedIndex.secondaryIndexEntries)
                    HashIndexBlock.write(
                      entry = indexEntry,
                      state = hashIndexState
                    )

                  HashIndexBlock.close(hashIndexState)
                }
              else
                Future.successful(HashIndexBlockState.Null)

            //concurrently build binarySearch
            val binarySearchIndexFuture: Future[BinarySearchIndexBlockStateOption] =
              if (binarySearchIndexOption.isSomeS)
                Future {
                  val binarySearchIndexState = binarySearchIndexOption.getS

                  for (indexEntry <- closedSortedIndex.secondaryIndexEntries)
                    BinarySearchIndexBlock.write(
                      entry = indexEntry,
                      state = binarySearchIndexState
                    )

                  BinarySearchIndexBlock.close(binarySearchIndexState, closedSortedIndex.uncompressedPrefixCount)
                }
              else
                Future.successful(BinarySearchIndexBlockState.Null)

            hashIndexFuture.join(binarySearchIndexFuture)
          } else {
            //else binarySearch is partial index and has dependency on hashIndex
            Future {
              for (indexEntry <- closedSortedIndex.secondaryIndexEntries) {
                //write to hashIndex first
                val hit =
                  if (hashIndexOption.isSomeS)
                    HashIndexBlock.write(
                      entry = indexEntry,
                      state = hashIndexOption.getS
                    )
                  else
                    false

                //if it's a hit and binary search is not configured to be full.
                //no need to check if the value was previously written to binary search here since BinarySearchIndexBlock itself performs this check.
                if (binarySearchIndexOption.isSomeS && !hit)
                  BinarySearchIndexBlock.write(
                    entry = indexEntry,
                    state = binarySearchIndexOption.getS
                  )
              }
            } flatMapUnit {
              //finally close both indexes concurrently.
              val hashIndexFuture =
                if (hashIndexOption.isSomeS)
                  Future(HashIndexBlock.close(hashIndexOption.getS))
                else
                  Future.successful(HashIndexBlockState.Null)

              val binarySearchIndexFuture =
                if (binarySearchIndexOption.isSomeS)
                  Future(BinarySearchIndexBlock.close(binarySearchIndexOption.getS, closedSortedIndex.uncompressedPrefixCount))
                else
                  Future.successful(BinarySearchIndexBlockState.Null)

              hashIndexFuture.join(binarySearchIndexFuture)
            }
          }
        else
          Future.successful((HashIndexBlockState.Null, BinarySearchIndexBlockState.Null))
      }

}

private[core] case class SegmentBlock(offset: SegmentBlockOffset,
                                      headerSize: Int,
                                      compressionInfo: BlockCompressionInfoOption) extends Block[SegmentBlockOffset]
