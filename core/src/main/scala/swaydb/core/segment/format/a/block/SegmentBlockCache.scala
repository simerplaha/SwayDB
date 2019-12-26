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

import java.nio.file.Path

import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.actor.MemorySweeper
import swaydb.core.cache.{Cache, Lazy}
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.segment.format.a.block.ValuesBlock.ValuesBlockOps
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.data.Reserve
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.slice.Slice
import swaydb.{Aggregator, Error, IO}

object SegmentBlockCache {

  def apply(path: Path,
            segmentIO: SegmentIO,
            blockRef: BlockRefReader[SegmentBlock.Offset],
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
            footerCacheable: Option[SegmentFooterBlock])(implicit cacheMemorySweeper: Option[MemorySweeper.Cache]): SegmentBlockCache =
    new SegmentBlockCache(
      path = path,
      segmentIO = segmentIO,
      segmentBlockRef = blockRef,
      valuesReaderCacheable = valuesReaderCacheable,
      sortedIndexReaderCacheable = sortedIndexReaderCacheable,
      hashIndexReaderCacheable = hashIndexReaderCacheable,
      binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
      bloomFilterReaderCacheable = bloomFilterReaderCacheable,
      footerCacheable = footerCacheable
    )
}

/**
 * Implements configured caching & IO strategies for all blocks within a Segment.
 */
class SegmentBlockCache(path: Path,
                        val segmentIO: SegmentIO,
                        segmentBlockRef: BlockRefReader[SegmentBlock.Offset],
                        var valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                        var sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                        var hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                        var binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                        var bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                        var footerCacheable: Option[SegmentFooterBlock])(implicit cacheMemorySweeper: Option[MemorySweeper.Cache]) {

  /**
   * Value configured in [[SegmentBlock.Config.cacheBlocksOnCreate]]
   */
  val areBlocksCacheableOnCreate = sortedIndexReaderCacheable.isDefined

  //names for Unblocked reader caches.
  private val sortedIndexReaderCacheName = "sortedIndexReaderCache"
  private val valuesReaderCacheName = "valuesReaderCache"
  private val segmentReaderCacheName = "segmentReaderCache"
  private val hashIndexReaderCacheName = "hashIndexReaderCache"
  private val bloomFilterReaderCacheName = "bloomFilterReaderCache"
  private val binarySearchIndexReaderCacheName = "binarySearchIndexReaderCache"

  /**
   * @note Segment's [[IOStrategy]] is required to be immutable ones read and cannot mutate during runtime.
   *       Changing IOStrategy during runtime causes offset conflicts.
   * @see SegmentBlockCacheSpec which will fail is stored is set to false.
   */
  private val segmentIOStrategyCache = Lazy.value[IOStrategy](synchronised = true, stored = true, initial = None)

  val nullIO = IO(null)

  @volatile var forceCacheSortedIndexAndValueReaders = false

  def segmentBlockIO(action: IOAction) =
    segmentIOStrategyCache getOrSet segmentIO.segmentBlockIO(action)

  def hashIndexBlockIO = segmentIO.hashIndexBlockIO
  def bloomFilterBlockIO = segmentIO.bloomFilterBlockIO
  def binarySearchIndexBlockIO = segmentIO.binarySearchIndexBlockIO
  def sortedIndexBlockIO = segmentIO.sortedIndexBlockIO
  def valuesBlockIO = segmentIO.valuesBlockIO
  def segmentFooterBlockIO = segmentIO.segmentFooterBlockIO

  def invalidateCachedReaders() = {
    this.valuesReaderCacheable = None
    this.sortedIndexReaderCacheable = None
    this.hashIndexReaderCacheable = None
    this.binarySearchIndexReaderCacheable = None
    this.bloomFilterReaderCacheable = None
    this.footerCacheable = None
  }

  /**
   * Builds a required cache for [[SortedIndexBlock]].
   */
  def buildBlockInfoCache[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy,
                                                           resourceName: String)(implicit blockOps: BlockOps[O, B]): Cache[swaydb.Error.Segment, BlockRefReader[O], B] =
    Cache.io[swaydb.Error.Segment, swaydb.Error.ReservedResource, BlockRefReader[O], B](
      strategy = blockIO(IOAction.ReadDataOverview),
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$path: $resourceName")),
      initial = None
    ) {
      (ref, self) =>
        IO {
          val header = Block.readHeader(ref)
          val block = blockOps.readBlock(header)

          if (self.isStored)
            cacheMemorySweeper foreach {
              sweeper =>
                sweeper.add(block.offset.size, self)
            }

          block
        }
    }

  def buildBlockInfoCacheOptional[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy,
                                                                   resourceName: String)(implicit blockOps: BlockOps[O, B]): Cache[swaydb.Error.Segment, Option[BlockRefReader[O]], Option[B]] =
    Cache.io[swaydb.Error.Segment, swaydb.Error.ReservedResource, Option[BlockRefReader[O]], Option[B]](
      strategy = blockIO(IOAction.ReadDataOverview),
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$path: $resourceName")),
      initial = None
    ) {
      case (Some(ref), self) =>
        IO {
          val header = Block.readHeader(ref)
          val block = blockOps.readBlock(header)

          if (self.isStored)
            cacheMemorySweeper foreach {
              sweeper =>
                sweeper.add(block.offset.size, self)
            }

          Some(block)
        }

      case (None, _) =>
        IO.none
    }

  def shouldForceCache(resourceName: String): Boolean =
    forceCacheSortedIndexAndValueReaders && (resourceName == sortedIndexReaderCacheName || resourceName == valuesReaderCacheName)

  def buildBlockReaderCache[O <: BlockOffset, B <: Block[O]](initial: Option[UnblockedReader[O, B]],
                                                             blockIO: IOAction => IOStrategy,
                                                             resourceName: String)(implicit blockOps: BlockOps[O, B]) =
    Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, BlockedReader[O, B], UnblockedReader[O, B]](
      initial = initial,
      strategy = reader => blockIO(reader.block.dataType).forceCacheOnAccess,
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$path: $resourceName"))
    ) {
      (initial, self) => //initial set clean up.
        cacheMemorySweeper foreach {
          cacheMemorySweeper =>
            cacheMemorySweeper.add(initial.underlyingArraySizeOrReaderSize, self)
        }
    } {
      (blockedReader, self) =>
        IO {

          val readerIsCacheOnAccess = shouldForceCache(resourceName) || blockIO(blockedReader.block.dataType).cacheOnAccess

          val reader =
            UnblockedReader(
              blockedReader = blockedReader,
              readAllIfUncompressed = readerIsCacheOnAccess
            )

          if (self.isStored && readerIsCacheOnAccess)
            cacheMemorySweeper foreach {
              sweeper =>
                sweeper.add(reader.block.offset.size, self)
            }

          reader
        }
    }

  def buildBlockReaderCacheNullable[O <: BlockOffset, B <: Block[O]](initial: Option[UnblockedReader[O, B]],
                                                                     blockIO: IOAction => IOStrategy,
                                                                     resourceName: String)(implicit blockOps: BlockOps[O, B]) =
    Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Option[BlockedReader[O, B]], UnblockedReader[O, B]](
      initial = if (areBlocksCacheableOnCreate && initial.isEmpty) Some(null) else initial,
      strategy = _.map(reader => blockIO(reader.block.dataType).forceCacheOnAccess) getOrElse IOStrategy.defaultBlockReadersStored,
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$path: $resourceName"))
    ) {
      (initial, self) => //initial set clean up.
        if (initial != null)
          cacheMemorySweeper foreach {
            cacheMemorySweeper =>
              cacheMemorySweeper.add(initial.underlyingArraySizeOrReaderSize, self)
          }
    } {
      case (Some(blockedReader), self) =>
        IO {
          val cacheOnAccess = shouldForceCache(resourceName) || blockIO(blockedReader.block.dataType).cacheOnAccess

          val reader =
            UnblockedReader(
              blockedReader = blockedReader,
              readAllIfUncompressed = cacheOnAccess
            )

          if (cacheOnAccess && self.isStored)
            cacheMemorySweeper foreach {
              sweeper =>
                sweeper.add(reader.block.offset.size, self)
            }

          reader

        }

      case (None, _) =>
        //UnblockReader for indexes can be null. These indexes could get read very often (1 million+).
        //So instead of using Option. Null checks are performed by searchers. All nullable params are
        //suffixed *Nullable.
        nullIO
    }

  private[block] def createSegmentBlockReader(): UnblockedReader[SegmentBlock.Offset, SegmentBlock] =
    segmentReaderCache
      .getOrElse(segmentReaderCache value BlockedReader(segmentBlockRef.copy()))
      .get
      .copy()

  def getBlockOptional[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, Option[BlockRefReader[O]], Option[B]],
                                                        getOffset: SegmentFooterBlock => Option[O])(implicit blockOps: BlockOps[O, B]): Option[B] =
    cache
      .getOrElse {
        val footer = getFooter()
        val offset = getOffset(footer)
        offset match {
          case Some(offset) =>
            cache value Some(BlockRefReader.moveTo(offset, createSegmentBlockReader()))

          case None =>
            cache value None
        }
      }
      .get

  def getBlock[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, BlockRefReader[O], B],
                                                offset: SegmentFooterBlock => O)(implicit blockOps: BlockOps[O, B]): B =
    cache
      .getOrElse {
        cache.value {
          val footer = getFooter()
          val segmentReader = createSegmentBlockReader()
          BlockRefReader.moveTo(offset(footer), segmentReader)
        }
      }
      .get

  def createReaderOptional[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, Option[BlockedReader[O, B]], UnblockedReader[O, B]],
                                                            getBlock: => Option[B])(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] = {

    val reader =
      cache
        .getOrElse {
          getBlock match {
            case Some(block) =>
              cache value Some(BlockedReader(block, createSegmentBlockReader()))

            case None =>
              cache value None
          }
        }.get

    if (reader != null)
      reader.copy()
    else
      reader
  }

  def createReader[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, BlockedReader[O, B], UnblockedReader[O, B]],
                                                    getBlock: => B)(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] = {
    cache
      .getOrElse {
        cache.value(
          BlockedReader(
            block = getBlock,
            reader = createSegmentBlockReader()
          )
        )
      }
    }
    .get
    .copy()

  private[block] val footerBlockCache =
    Cache.io[swaydb.Error.Segment, swaydb.Error.ReservedResource, UnblockedReader[SegmentBlock.Offset, SegmentBlock], SegmentFooterBlock](
      strategy = segmentFooterBlockIO(IOAction.ReadDataOverview),
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$path: footerBlockCache")),
      initial = footerCacheable
    ) {
      (reader, self) =>
        IO {
          val block = SegmentFooterBlock.read(reader)

          if (self.isStored)
            cacheMemorySweeper foreach {
              sweeper =>
                sweeper.add(block.offset.size, self)
            }

          block
        }
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
    buildBlockReaderCache[SegmentBlock.Offset, SegmentBlock](
      initial = None,
      blockIO = segmentBlockIO,
      resourceName = segmentReaderCacheName
    )

  private[block] val sortedIndexReaderCache =
    buildBlockReaderCache[SortedIndexBlock.Offset, SortedIndexBlock](
      initial = sortedIndexReaderCacheable,
      blockIO = sortedIndexBlockIO,
      resourceName = sortedIndexReaderCacheName
    )

  private[block] val hashIndexReaderCacheNullable =
    buildBlockReaderCacheNullable[HashIndexBlock.Offset, HashIndexBlock](
      initial = hashIndexReaderCacheable,
      blockIO = hashIndexBlockIO,
      resourceName = hashIndexReaderCacheName
    )

  private[block] val bloomFilterReaderCacheNullable =
    buildBlockReaderCacheNullable[BloomFilterBlock.Offset, BloomFilterBlock](
      initial = bloomFilterReaderCacheable,
      blockIO = bloomFilterBlockIO,
      resourceName = bloomFilterReaderCacheName
    )

  private[block] val binarySearchIndexReaderCacheNullable =
    buildBlockReaderCacheNullable[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](
      initial = binarySearchIndexReaderCacheable,
      blockIO = binarySearchIndexBlockIO,
      resourceName = binarySearchIndexReaderCacheName
    )

  private[block] val valuesReaderCacheNullable: Cache[Error.Segment, Option[BlockedReader[ValuesBlock.Offset, ValuesBlock]], UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    buildBlockReaderCacheNullable[ValuesBlock.Offset, ValuesBlock](
      initial = valuesReaderCacheable,
      blockIO = valuesBlockIO,
      resourceName = valuesReaderCacheName
    )

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
      //nullable caches
      hashIndexReaderCacheNullable,
      bloomFilterReaderCacheNullable,
      binarySearchIndexReaderCacheNullable,
      valuesReaderCacheNullable
    )

  def getFooter(): SegmentFooterBlock =
    footerBlockCache
      .getOrElse(footerBlockCache.value(createSegmentBlockReader()))
      .get

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

  def createHashIndexReaderNullable(): UnblockedReader[HashIndexBlock.Offset, HashIndexBlock] =
    createReaderOptional(hashIndexReaderCacheNullable, getHashIndex())

  def createBloomFilterReaderNullable(): UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock] =
    createReaderOptional(bloomFilterReaderCacheNullable, getBloomFilter())

  def createBinarySearchIndexReaderNullable(): UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock] =
    createReaderOptional(binarySearchIndexReaderCacheNullable, getBinarySearchIndex())

  def createValuesReaderNullable(): UnblockedReader[ValuesBlock.Offset, ValuesBlock] =
    createReaderOptional(valuesReaderCacheNullable, getValues())

  def createSortedIndexReader(): UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock] =
    createReader(sortedIndexReaderCache, getSortedIndex())

  def readAll(): Slice[KeyValue] = {
    val keyValueCount = getFooter().keyValueCount
    val aggregator = Slice.newAggregator[KeyValue](keyValueCount)
    readAll(
      keyValueCount = keyValueCount,
      aggregator = aggregator
    )
    aggregator.result
  }

  def readAll[T](aggregator: Aggregator[KeyValue, T]): Unit =
    readAll(
      keyValueCount = getFooter().keyValueCount,
      aggregator = aggregator
    )

  def readAll[T](keyValueCount: Int): Slice[KeyValue] = {
    val aggregator = Slice.newAggregator[KeyValue](keyValueCount)
    readAll(
      keyValueCount = keyValueCount,
      aggregator = aggregator
    )
    aggregator.result
  }

  /**
   * Read all but also cache sortedIndex and valueBytes if they are not already cached.
   */
  def readAll[T](keyValueCount: Int,
                 aggregator: Aggregator[KeyValue, T]): Unit =
    try {
      var sortedIndexReader = createSortedIndexReader()
      if (sortedIndexReader.isFile) {
        forceCacheSortedIndexAndValueReaders = true
        sortedIndexReaderCache.clear()
        sortedIndexReader = createSortedIndexReader()
      }

      var valuesReaderNullable = createValuesReaderNullable()
      if (valuesReaderNullable != null && valuesReaderNullable.isFile) {
        forceCacheSortedIndexAndValueReaders = true
        valuesReaderCacheNullable.clear()
        valuesReaderNullable = createValuesReaderNullable()
      }

      SortedIndexBlock.readAll(
        sortedIndexReader = sortedIndexReader,
        valuesReaderNullable = valuesReaderNullable,
        aggregator = aggregator
      )
    } finally {
      forceCacheSortedIndexAndValueReaders = false
    }

  def iterator(): Iterator[Persistent] =
    try {
      var sortedIndexReader = createSortedIndexReader()
      if (sortedIndexReader.isFile) {
        forceCacheSortedIndexAndValueReaders = true
        sortedIndexReaderCache.clear()
        sortedIndexReader = createSortedIndexReader()
      }

      var valuesReaderNullable = createValuesReaderNullable()
      if (valuesReaderNullable != null && valuesReaderNullable.isFile) {
        forceCacheSortedIndexAndValueReaders = true
        valuesReaderCacheNullable.clear()
        valuesReaderNullable = createValuesReaderNullable()
      }

      SortedIndexBlock.iterator(
        sortedIndexReader = sortedIndexReader,
        valuesReaderNullable = valuesReaderNullable
      )
    } finally {
      forceCacheSortedIndexAndValueReaders = false
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

  invalidateCachedReaders()
}
