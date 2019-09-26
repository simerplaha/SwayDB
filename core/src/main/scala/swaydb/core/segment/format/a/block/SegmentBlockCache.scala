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

package swaydb.core.segment.format.a.block

import swaydb.core.cache.{Cache, Lazy}
import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.block.ValuesBlock.ValuesBlockOps
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.cache.Lazy
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.util.Benchmark
import swaydb.data.Reserve
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

object SegmentBlockCache {

  def apply(id: String,
            segmentIO: SegmentIO,
            blockRef: BlockRefReader[SegmentBlock.Offset]): SegmentBlockCache =
    new SegmentBlockCache(
      id = id,
      segmentIO = segmentIO,
      segmentBlockRef = blockRef
    )
}

/**
 * Implements configured caching & IO strategies for all blocks within a Segment.
 */
class SegmentBlockCache(id: String,
                        val segmentIO: SegmentIO,
                        segmentBlockRef: BlockRefReader[SegmentBlock.Offset]) {

  /**
   * @note Segment's [[IOStrategy]] is required to be immutable ones read and cannot mutate during runtime.
   *       Changing IOStrategy during runtime causes offset conflicts.
   * @see SegmentBlockCacheSpec which will fail is stored is set to false.
   */
  private val segmentIOStrategyCache = Lazy.value[IOStrategy](synchronised = true, stored = true, initial = None)

  def segmentBlockIO(action: IOAction) =
    segmentIOStrategyCache getOrSet segmentIO.segmentBlockIO(action)

  def hashIndexBlockIO = segmentIO.hashIndexBlockIO
  def bloomFilterBlockIO = segmentIO.bloomFilterBlockIO
  def binarySearchIndexBlockIO = segmentIO.binarySearchIndexBlockIO
  def sortedIndexBlockIO = segmentIO.sortedIndexBlockIO
  def valuesBlockIO = segmentIO.valuesBlockIO
  def segmentFooterBlockIO = segmentIO.segmentFooterBlockIO

  /**
   * Builds a required cache for [[SortedIndexBlock]].
   */
  def buildBlockInfoCache[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy,
                                                           resourceName: String)(implicit blockOps: BlockOps[O, B]): Cache[swaydb.Error.Segment, BlockRefReader[O], B] =
    Cache.io[swaydb.Error.Segment, swaydb.Error.ReservedResource, BlockRefReader[O], B](
      strategy = blockIO(IOAction.ReadDataOverview),
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$id: $resourceName")),
      initial = None
    ) {
      ref =>
        Block
          .readHeader(ref)
          .flatMap(blockOps.readBlock)
    }

  def buildBlockInfoCacheOptional[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy,
                                                                   resourceName: String)(implicit blockOps: BlockOps[O, B]): Cache[swaydb.Error.Segment, Option[BlockRefReader[O]], Option[B]] =
    Cache.io[swaydb.Error.Segment, swaydb.Error.ReservedResource, Option[BlockRefReader[O]], Option[B]](
      strategy = blockIO(IOAction.ReadDataOverview),
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$id: $resourceName")),
      initial = None
    ) {
      ref =>
        ref map {
          ref =>
            Block
              .readHeader(ref)
              .flatMap(blockOps.readBlock)
              .toOptionValue
        } getOrElse IO.none
    }

  def buildBlockReaderCache[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy,
                                                             resourceName: String)(implicit blockOps: BlockOps[O, B]) =
    Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, BlockedReader[O, B], UnblockedReader[O, B]](
      strategy = reader => blockIO(reader.block.dataType).withCacheOnAccess,
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$id: $resourceName"))
    ) {
      blockedReader =>
        UnblockedReader(
          blockedReader = blockedReader,
          readAllIfUncompressed = blockIO(blockedReader.block.dataType).cacheOnAccess
        )
    }

  def buildBlockReaderCacheOptional[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy,
                                                                     resourceName: String)(implicit blockOps: BlockOps[O, B]) =
    Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Option[BlockedReader[O, B]], Option[UnblockedReader[O, B]]](
      strategy = _.map(reader => blockIO(reader.block.dataType).withCacheOnAccess) getOrElse IOStrategy.defaultBlockReadersStored,
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$id: $resourceName"))
    ) {
      blockedReader =>
        blockedReader map {
          blockedReader =>
            UnblockedReader(
              blockedReader = blockedReader,
              readAllIfUncompressed = blockIO(blockedReader.block.dataType).cacheOnAccess
            ).toOptionValue
        } getOrElse IO.none
    }

  private[block] def createSegmentBlockReader(): UnblockedReader[SegmentBlock.Offset, SegmentBlock] = {
    segmentReaderCache getOrElse {
      BlockedReader(segmentBlockRef.copy()) flatMap {
        blockedReader =>
          segmentReaderCache.value(blockedReader)
      }
    }
    }.map(_.copy())

  def getBlockOptional[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, Option[BlockRefReader[O]], Option[B]],
                                                        offset: SegmentFooterBlock => Option[O])(implicit blockOps: BlockOps[O, B]): Option[B] =
    cache getOrElse {
      getFooter() flatMap {
        footer =>
          offset(footer) map {
            offset =>
              createSegmentBlockReader() flatMap {
                segmentReader =>
                  cache.value(Some(BlockRefReader.moveTo(offset, segmentReader)))
              }
          } getOrElse cache.value(None)
      }
    }

  def getBlock[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, BlockRefReader[O], B],
                                                offset: SegmentFooterBlock => O)(implicit blockOps: BlockOps[O, B]): B =
    cache getOrElse {
      getFooter() flatMap {
        footer =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              cache.value(BlockRefReader.moveTo(offset(footer), segmentReader))
          }
      }
    }

  def createReaderOptional[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, Option[BlockedReader[O, B]], Option[UnblockedReader[O, B]]],
                                                            getBlock: => IO[swaydb.Error.Segment, Option[B]])(implicit blockOps: BlockOps[O, B]): Option[UnblockedReader[O, B]] = {
    cache getOrElse {
      getBlock flatMap {
        block =>
          block map {
            block =>
              createSegmentBlockReader() flatMap {
                segmentReader =>
                  cache
                    .value(Some(BlockedReader(block, segmentReader)))
              }
          } getOrElse cache.value(None)
      }
    }
    }.map(_.map(_.copy()))

  def createReader[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, BlockedReader[O, B], UnblockedReader[O, B]],
                                                    getBlock: => IO[swaydb.Error.Segment, B])(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] = {
    cache getOrElse {
      getBlock flatMap {
        block =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              cache
                .value(BlockedReader(block, segmentReader))
          }
      }
    }
    }.map(_.copy())

  private[block] val footerBlockCache =
    Cache.io[swaydb.Error.Segment, swaydb.Error.ReservedResource, UnblockedReader[SegmentBlock.Offset, SegmentBlock], SegmentFooterBlock](
      strategy = segmentFooterBlockIO(IOAction.ReadDataOverview),
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$id: footerBlockCache")),
      initial = None
    ) {
      reader =>
        SegmentFooterBlock.read(reader)
    }

  //info caches
  private[block] val sortedIndexBlockCache =
    buildBlockInfoCache[SortedIndexBlock.Offset, SortedIndexBlock](sortedIndexBlockIO, "sortedIndexBlockCache")

  private[block] val hashIndexBlockCache =
    buildBlockInfoCacheOptional[HashIndexBlock.Offset, HashIndexBlock](hashIndexBlockIO, "hashIndexBlockCache")

  private[block] val bloomFilterBlockCache =
    buildBlockInfoCacheOptional[BloomFilterBlock.Offset, BloomFilterBlock](bloomFilterBlockIO, "bloomFilterBlockCache")

  private[block] val binarySearchIndexBlockCache =
    buildBlockInfoCacheOptional[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](binarySearchIndexBlockIO, "binarySearchIndexBlockCache")

  private[block] val valuesBlockCache =
    buildBlockInfoCacheOptional[ValuesBlock.Offset, ValuesBlock](valuesBlockIO, "valuesBlockCache")

  //reader caches
  private[block] val segmentReaderCache =
    buildBlockReaderCache[SegmentBlock.Offset, SegmentBlock](segmentBlockIO, "segmentReaderCache")

  private[block] val sortedIndexReaderCache =
    buildBlockReaderCache[SortedIndexBlock.Offset, SortedIndexBlock](sortedIndexBlockIO, "sortedIndexReaderCache")

  private[block] val hashIndexReaderCache =
    buildBlockReaderCacheOptional[HashIndexBlock.Offset, HashIndexBlock](hashIndexBlockIO, "hashIndexReaderCache")

  private[block] val bloomFilterReaderCache =
    buildBlockReaderCacheOptional[BloomFilterBlock.Offset, BloomFilterBlock](bloomFilterBlockIO, "bloomFilterReaderCache")

  private[block] val binarySearchIndexReaderCache =
    buildBlockReaderCacheOptional[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](binarySearchIndexBlockIO, "binarySearchIndexReaderCache")

  private[block] val valuesReaderCache: Cache[swaydb.Error.Segment, Option[BlockedReader[ValuesBlock.Offset, ValuesBlock]], Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]] =
    buildBlockReaderCacheOptional[ValuesBlock.Offset, ValuesBlock](valuesBlockIO, "valuesReaderCache")

  private[block] val allCaches =
    Seq(
      footerBlockCache,
      sortedIndexBlockCache,
      hashIndexBlockCache,
      bloomFilterBlockCache,
      binarySearchIndexBlockCache,
      valuesBlockCache,
      //readers
      segmentReaderCache,
      sortedIndexReaderCache,
      hashIndexReaderCache,
      bloomFilterReaderCache,
      binarySearchIndexReaderCache,
      valuesReaderCache
    )

  def getFooter(): SegmentFooterBlock =
    footerBlockCache getOrElse {
      createSegmentBlockReader().flatMap(footerBlockCache.value(_))
    }

  def getHashIndex(): Option[HashIndexBlock] =
    getBlockOptional(hashIndexBlockCache, _.hashIndexOffset)

  def getBloomFilter(): Option[BloomFilterBlock] =
    getBlockOptional(bloomFilterBlockCache, _.bloomFilterOffset)

  def getBinarySearchIndex(): Option[BinarySearchIndexBlock] =
    getBlockOptional(binarySearchIndexBlockCache, _.binarySearchIndexOffset)

  def getSortedIndex(): SortedIndexBlock =
    getBlock(sortedIndexBlockCache, _.sortedIndexOffset)

  def getValues(): Option[ValuesBlock] =
    getBlockOptional(valuesBlockCache, _.valuesOffset)

  def createHashIndexReader(): Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]] =
    createReaderOptional(hashIndexReaderCache, getHashIndex())

  def createBloomFilterReader(): Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]] =
    createReaderOptional(bloomFilterReaderCache, getBloomFilter())

  def createBinarySearchIndexReader(): Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]] =
    createReaderOptional(binarySearchIndexReaderCache, getBinarySearchIndex())

  def createValuesReader(): Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    createReaderOptional(valuesReaderCache, getValues())

  def createSortedIndexReader(): UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock] =
    createReader(sortedIndexReaderCache, getSortedIndex())

  def readAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): Slice[KeyValue.ReadOnly] =
    getFooter() flatMap {
      footer =>
        createSortedIndexReader() flatMap {
          sortedIndexReader =>
            createValuesReader() flatMap {
              valuesReader =>
                SortedIndexBlock.readAll(
                  keyValueCount = footer.keyValueCount,
                  sortedIndexReader = sortedIndexReader,
                  valuesReader = valuesReader,
                  addTo = addTo
                )
            }
        }
    }

  def readAllBytes(): Slice[Byte] =
    segmentBlockRef.copy().readFullBlock()

  def clear(): Unit =
    allCaches.foreach(_.clear())

  def isCached: Boolean =
    allCaches.exists(_.isCached)

  def isFooterDefined =
    footerBlockCache.isCached

  def isBloomFilterDefined =
    bloomFilterBlockCache.isCached
}
