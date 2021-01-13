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

package swaydb.core.segment.block.segment

import java.nio.file.Path
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.data.Persistent
import swaydb.core.io.file.DBFile
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.block.{Block, BlockCache, BlockOffset, BlockOps}
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.sweeper.MemorySweeper
import swaydb.data.Reserve
import swaydb.data.cache.{Cache, Lazy}
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

private[core] object SegmentBlockCache {

  private val nullIO: IO[Error.Segment, Null] = IO(null)

  //names for Unblocked reader caches.
  private val sortedIndexReaderCacheName = "sortedIndexReaderCache"
  private val valuesReaderCacheName = "valuesReaderCache"
  private val segmentReaderCacheName = "segmentReaderCache"
  private val hashIndexReaderCacheName = "hashIndexReaderCache"
  private val bloomFilterReaderCacheName = "bloomFilterReaderCache"
  private val binarySearchIndexReaderCacheName = "binarySearchIndexReaderCache"

  def apply(path: Path,
            segmentIO: SegmentReadIO,
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

      /**
       * Cannot be used anymore because of partial caching on copied can be applied
       * if only some of the Segment's block were slice readers. See [[SegmentBlockCache.validateCachedReaderForCopiedSegment]]
       */
      //Value configured in [[SegmentBlock.Config.cacheBlocksOnCreate]]
      //areBlocksCacheableOnCreate = sortedIndexReaderCacheable.isDefined
    )
}

/**
 * Implements configured caching & IO strategies for all blocks within a Segment.
 */
private[core] class SegmentBlockCache private(path: Path,
                                              val segmentIO: SegmentReadIO,
                                              segmentBlockRef: BlockRefReader[SegmentBlock.Offset],
                                              private var valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                                              private var sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                                              private var hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                                              private var binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                                              private var bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                                              private var footerCacheable: Option[SegmentFooterBlock])(implicit cacheMemorySweeper: Option[MemorySweeper.Cache]) {


  /**
   * @note Segment's [[IOStrategy]] is required to be immutable ones read and cannot mutate during runtime.
   *       Changing IOStrategy during runtime causes offset conflicts.
   * @see SegmentBlockCacheSpec which will fail is stored is set to false.
   */
  private val segmentIOStrategyCache = Lazy.value[IOStrategy](synchronised = true, stored = true, initial = None)

  @volatile var forceCacheSortedIndexAndValueReaders = false

  def segmentBlockIO(action: IOAction): IOStrategy =
    segmentIOStrategyCache getOrSet segmentIO.segmentBlockIO(action)

  def hashIndexBlockIO = segmentIO.hashIndexBlockIO
  def bloomFilterBlockIO = segmentIO.bloomFilterBlockIO
  def binarySearchIndexBlockIO = segmentIO.binarySearchIndexBlockIO
  def sortedIndexBlockIO = segmentIO.sortedIndexBlockIO
  def valuesBlockIO = segmentIO.valuesBlockIO
  def segmentFooterBlockIO = segmentIO.segmentFooterBlockIO

  final def invalidateCachedReaders() = {
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
    forceCacheSortedIndexAndValueReaders && (resourceName == SegmentBlockCache.sortedIndexReaderCacheName || resourceName == SegmentBlockCache.valuesReaderCacheName)

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

  /**
   * TODO switch out null with [[swaydb.core.segment.block.reader.UnblockedReaderOption]] for type-safety.
   */
  def buildBlockReaderCacheOrNull[O <: BlockOffset, B <: Block[O]](initial: Option[UnblockedReader[O, B]],
                                                                   blockIO: IOAction => IOStrategy,
                                                                   resourceName: String)(implicit blockOps: BlockOps[O, B]) =
    Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Option[BlockedReader[O, B]], UnblockedReader[O, B]](
      /**
       * areBlocksCacheableOnCreate cannot be used anymore because of partial caching on copied can be applied
       * if only some of the Segment's block were slice readers. See [[SegmentBlockCache.validateCachedReaderForCopiedSegment]]
       *
       * This is ok because if blocks are cached on create then only the footer (already cached) will be read to
       * populate the null readers (means no reader exists). Therefore no IO is performed.
       */
      //      initial = if (areBlocksCacheableOnCreate && initial.isEmpty) Some(null) else initial,
      initial = initial,
      strategy = _.map(reader => blockIO(reader.block.dataType).forceCacheOnAccess) getOrElse IOStrategy.ConcurrentIO(true),
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
        //suffixed *OrNull.
        SegmentBlockCache.nullIO
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
            cache value
              Some(
                BlockRefReader.moveTo(
                  offset = offset,
                  reader = createSegmentBlockReader(),
                  blockCache = segmentBlockRef.blockCache
                )
              )

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
          BlockRefReader.moveTo(
            offset = offset(footer),
            reader = segmentReader,
            blockCache = segmentBlockRef.blockCache
          )
        }
      }
      .get

  def createReaderOptional[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, Option[BlockedReader[O, B]], UnblockedReader[O, B]],
                                                            getBlock: => Option[B]): UnblockedReader[O, B] = {

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
                                                    getBlock: => B): UnblockedReader[O, B] = {
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
      resourceName = SegmentBlockCache.segmentReaderCacheName
    )

  private[block] val sortedIndexReaderCache =
    buildBlockReaderCache[SortedIndexBlock.Offset, SortedIndexBlock](
      initial = sortedIndexReaderCacheable,
      blockIO = sortedIndexBlockIO,
      resourceName = SegmentBlockCache.sortedIndexReaderCacheName
    )

  private[block] val hashIndexReaderCacheOrNull =
    buildBlockReaderCacheOrNull[HashIndexBlock.Offset, HashIndexBlock](
      initial = hashIndexReaderCacheable,
      blockIO = hashIndexBlockIO,
      resourceName = SegmentBlockCache.hashIndexReaderCacheName
    )

  private[block] val bloomFilterReaderCacheOrNull =
    buildBlockReaderCacheOrNull[BloomFilterBlock.Offset, BloomFilterBlock](
      initial = bloomFilterReaderCacheable,
      blockIO = bloomFilterBlockIO,
      resourceName = SegmentBlockCache.bloomFilterReaderCacheName
    )

  private[block] val binarySearchIndexReaderCacheOrNull =
    buildBlockReaderCacheOrNull[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](
      initial = binarySearchIndexReaderCacheable,
      blockIO = binarySearchIndexBlockIO,
      resourceName = SegmentBlockCache.binarySearchIndexReaderCacheName
    )

  private[block] val valuesReaderCacheOrNull: Cache[Error.Segment, Option[BlockedReader[ValuesBlock.Offset, ValuesBlock]], UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    buildBlockReaderCacheOrNull[ValuesBlock.Offset, ValuesBlock](
      initial = valuesReaderCacheable,
      blockIO = valuesBlockIO,
      resourceName = SegmentBlockCache.valuesReaderCacheName
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
      hashIndexReaderCacheOrNull,
      bloomFilterReaderCacheOrNull,
      binarySearchIndexReaderCacheOrNull,
      valuesReaderCacheOrNull
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

  def createHashIndexReaderOrNull(): UnblockedReader[HashIndexBlock.Offset, HashIndexBlock] =
    createReaderOptional(hashIndexReaderCacheOrNull, getHashIndex())

  def createBloomFilterReaderOrNull(): UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock] =
    createReaderOptional(bloomFilterReaderCacheOrNull, getBloomFilter())

  def createBinarySearchIndexReaderOrNull(): UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock] =
    createReaderOptional(binarySearchIndexReaderCacheOrNull, getBinarySearchIndex())

  def createValuesReaderOrNull(): UnblockedReader[ValuesBlock.Offset, ValuesBlock] =
    createReaderOptional(valuesReaderCacheOrNull, getValues())

  def createSortedIndexReader(): UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock] =
    createReader(sortedIndexReaderCache, getSortedIndex())

  private def validateCachedReaderForCopiedSegment[O <: BlockOffset, B <: Block[O]](optionReader: Option[UnblockedReader[O, B]]): Option[UnblockedReader[O, B]] =
    optionReader match {
      case Some(reader) =>
        if (reader == null)
          optionReader
        else if (reader.isFile)
          None
        else
          Some(reader.copy())

      case None =>
        None
    }

  def cachedValuesSliceReader(): Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    validateCachedReaderForCopiedSegment(valuesReaderCacheOrNull.get())

  def cachedSortedIndexSliceReader(): Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]] =
    validateCachedReaderForCopiedSegment(sortedIndexReaderCache.get())

  def cachedHashIndexSliceReader(): Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]] =
    validateCachedReaderForCopiedSegment(hashIndexReaderCacheOrNull.get())

  def cachedBinarySearchIndexSliceReader(): Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]] =
    validateCachedReaderForCopiedSegment(binarySearchIndexReaderCacheOrNull.get())

  def cachedBloomFilterSliceReader(): Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]] =
    validateCachedReaderForCopiedSegment(bloomFilterReaderCacheOrNull.get())

  def cachedFooter(): Option[SegmentFooterBlock] =
    footerBlockCache.get()

  /**
   * Read all but also cache sortedIndex and valueBytes if they are not already cached.
   */

  def iterator(): Iterator[Persistent] =
    try {
      var sortedIndexReader = createSortedIndexReader()
      if (sortedIndexReader.isFile) {
        forceCacheSortedIndexAndValueReaders = true
        sortedIndexReaderCache.clear()
        sortedIndexReader = createSortedIndexReader()
      }

      var valuesReaderOrNull = createValuesReaderOrNull()
      if (valuesReaderOrNull != null && valuesReaderOrNull.isFile) {
        forceCacheSortedIndexAndValueReaders = true
        valuesReaderCacheOrNull.clear()
        valuesReaderOrNull = createValuesReaderOrNull()
      }

      SortedIndexBlock.iterator(
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    } finally {
      forceCacheSortedIndexAndValueReaders = false
    }

  def readAllBytes(): Slice[Byte] =
    segmentBlockRef.copy().readFullBlock()

  def offset(): SegmentBlock.Offset =
    segmentBlockRef.offset

  def transfer(position: Int, count: Int, transferTo: DBFile): Unit =
    segmentBlockRef.transfer(position = position, count = count, transferTo = transferTo)

  /**
   * Transfers bytes at file level. Ignores the [[BlockRefReader]]'s offset.
   */
  def transferIgnoreOffset(position: Int, count: Int, transferTo: DBFile): Unit =
    segmentBlockRef.transferIgnoreOffset(position = position, count = count, transferTo = transferTo)

  def clear(): Unit =
    allCaches.foreach(_.clear())

  def isCached: Boolean =
    allCaches.exists(_.isCached)

  def isFooterDefined =
    footerBlockCache.isCached

  def isBloomFilterDefined =
    bloomFilterBlockCache.isCached

  def segmentSize: Int =
    segmentBlockRef.offset.size

  def blockCache(): Option[BlockCache.State] =
    segmentBlockRef.blockCache

  invalidateCachedReaders()
}
