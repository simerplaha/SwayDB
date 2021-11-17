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

import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.cache.{Cache, Lazy}
import swaydb.core.segment.data.Persistent
import swaydb.core.file.DBFile
import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockOffset}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset}
import swaydb.core.segment.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}
import swaydb.core.segment.block.{Block, BlockCache, BlockCacheState, BlockOffset, BlockOps}
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.sweeper.MemorySweeper
import swaydb.slice.Slice
import swaydb.effect.{IOAction, IOStrategy, Reserve}
import swaydb.{Error, IO}

import java.nio.file.Path

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
            blockRef: BlockRefReader[SegmentBlockOffset],
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]],
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
      //Value configured in [[SegmentBlockConfig.cacheBlocksOnCreate]]
      //areBlocksCacheableOnCreate = sortedIndexReaderCacheable.isDefined
    )
}

/**
 * Implements configured caching & IO strategies for all blocks within a Segment.
 */
private[core] class SegmentBlockCache private(path: Path,
                                              val segmentIO: SegmentReadIO,
                                              segmentBlockRef: BlockRefReader[SegmentBlockOffset],
                                              private var valuesReaderCacheable: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]],
                                              private var sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]],
                                              private var hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]],
                                              private var binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]],
                                              private var bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]],
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
      strategy = reader => blockIO(reader.block.decompressionAction).forceCacheOnAccess,
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

          val readerIsCacheOnAccess = shouldForceCache(resourceName) || blockIO(blockedReader.block.decompressionAction).cacheOnAccess

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
      strategy = _.map(reader => blockIO(reader.block.decompressionAction).forceCacheOnAccess) getOrElse IOStrategy.ConcurrentIO(true),
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
          val cacheOnAccess = shouldForceCache(resourceName) || blockIO(blockedReader.block.decompressionAction).cacheOnAccess

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

  private[block] def createSegmentBlockReader(): UnblockedReader[SegmentBlockOffset, SegmentBlock] =
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
    Cache.io[swaydb.Error.Segment, swaydb.Error.ReservedResource, UnblockedReader[SegmentBlockOffset, SegmentBlock], SegmentFooterBlock](
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
    buildBlockInfoCache[SortedIndexBlockOffset, SortedIndexBlock](sortedIndexBlockIO, "sortedIndexBlockCache")

  private[block] val hashIndexBlockCache =
    buildBlockInfoCacheOptional[HashIndexBlockOffset, HashIndexBlock](hashIndexBlockIO, "hashIndexBlockCache")

  private[block] val bloomFilterBlockCache =
    buildBlockInfoCacheOptional[BloomFilterBlockOffset, BloomFilterBlock](bloomFilterBlockIO, "bloomFilterBlockCache")

  private[block] val binarySearchIndexBlockCache =
    buildBlockInfoCacheOptional[BinarySearchIndexBlockOffset, BinarySearchIndexBlock](binarySearchIndexBlockIO, "binarySearchIndexBlockCache")

  private[block] val valuesBlockCache =
    buildBlockInfoCacheOptional[ValuesBlockOffset, ValuesBlock](valuesBlockIO, "valuesBlockCache")

  //reader caches
  private[block] val segmentReaderCache =
    buildBlockReaderCache[SegmentBlockOffset, SegmentBlock](
      initial = None,
      blockIO = segmentBlockIO,
      resourceName = SegmentBlockCache.segmentReaderCacheName
    )

  private[block] val sortedIndexReaderCache =
    buildBlockReaderCache[SortedIndexBlockOffset, SortedIndexBlock](
      initial = sortedIndexReaderCacheable,
      blockIO = sortedIndexBlockIO,
      resourceName = SegmentBlockCache.sortedIndexReaderCacheName
    )

  private[block] val hashIndexReaderCacheOrNull =
    buildBlockReaderCacheOrNull[HashIndexBlockOffset, HashIndexBlock](
      initial = hashIndexReaderCacheable,
      blockIO = hashIndexBlockIO,
      resourceName = SegmentBlockCache.hashIndexReaderCacheName
    )

  private[block] val bloomFilterReaderCacheOrNull =
    buildBlockReaderCacheOrNull[BloomFilterBlockOffset, BloomFilterBlock](
      initial = bloomFilterReaderCacheable,
      blockIO = bloomFilterBlockIO,
      resourceName = SegmentBlockCache.bloomFilterReaderCacheName
    )

  private[block] val binarySearchIndexReaderCacheOrNull =
    buildBlockReaderCacheOrNull[BinarySearchIndexBlockOffset, BinarySearchIndexBlock](
      initial = binarySearchIndexReaderCacheable,
      blockIO = binarySearchIndexBlockIO,
      resourceName = SegmentBlockCache.binarySearchIndexReaderCacheName
    )

  private[block] val valuesReaderCacheOrNull: Cache[Error.Segment, Option[BlockedReader[ValuesBlockOffset, ValuesBlock]], UnblockedReader[ValuesBlockOffset, ValuesBlock]] =
    buildBlockReaderCacheOrNull[ValuesBlockOffset, ValuesBlock](
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

  def createHashIndexReaderOrNull(): UnblockedReader[HashIndexBlockOffset, HashIndexBlock] =
    createReaderOptional(hashIndexReaderCacheOrNull, getHashIndex())

  def createBloomFilterReaderOrNull(): UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock] =
    createReaderOptional(bloomFilterReaderCacheOrNull, getBloomFilter())

  def createBinarySearchIndexReaderOrNull(): UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock] =
    createReaderOptional(binarySearchIndexReaderCacheOrNull, getBinarySearchIndex())

  def createValuesReaderOrNull(): UnblockedReader[ValuesBlockOffset, ValuesBlock] =
    createReaderOptional(valuesReaderCacheOrNull, getValues())

  def createSortedIndexReader(): UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock] =
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

  def cachedValuesSliceReader(): Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]] =
    validateCachedReaderForCopiedSegment(valuesReaderCacheOrNull.get())

  def cachedSortedIndexSliceReader(): Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]] =
    validateCachedReaderForCopiedSegment(sortedIndexReaderCache.get())

  def cachedHashIndexSliceReader(): Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]] =
    validateCachedReaderForCopiedSegment(hashIndexReaderCacheOrNull.get())

  def cachedBinarySearchIndexSliceReader(): Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]] =
    validateCachedReaderForCopiedSegment(binarySearchIndexReaderCacheOrNull.get())

  def cachedBloomFilterSliceReader(): Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]] =
    validateCachedReaderForCopiedSegment(bloomFilterReaderCacheOrNull.get())

  def cachedFooter(): Option[SegmentFooterBlock] =
    footerBlockCache.get()

  /**
   * Read all but also cache sortedIndex and valueBytes if they are not already cached.
   */

  def iterator(inOneSeek: Boolean): Iterator[Persistent] =
    try {
      var sortedIndexReader = createSortedIndexReader()
      if (inOneSeek && sortedIndexReader.isFile) {
        forceCacheSortedIndexAndValueReaders = true
        sortedIndexReaderCache.clear()
        sortedIndexReader = createSortedIndexReader()
      }

      var valuesReaderOrNull = createValuesReaderOrNull()
      if (inOneSeek && valuesReaderOrNull != null && valuesReaderOrNull.isFile) {
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

  def offset(): SegmentBlockOffset =
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

  def isFooterDefined: Boolean =
    footerBlockCache.isCached

  def isBloomFilterDefined: Boolean =
    bloomFilterBlockCache.isCached

  def segmentSize: Int =
    segmentBlockRef.offset.size

  def blockCache(): Option[BlockCacheState] =
    segmentBlockRef.blockCache

  invalidateCachedReaders()
}
