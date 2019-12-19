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
import swaydb.IO
import swaydb.core.actor.MemorySweeper
import swaydb.core.cache.{Cache, Lazy}
import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.block.ValuesBlock.ValuesBlockOps
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.data.Reserve
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.slice.Slice

import scala.collection.mutable

object SegmentBlockCache {

  def apply(path: Path,
            segmentIO: SegmentIO,
            blockRef: BlockRefReader[SegmentBlock.Offset])(implicit cacheMemorySweeper: Option[MemorySweeper.Cache]): SegmentBlockCache =
    new SegmentBlockCache(
      path = path,
      segmentIO = segmentIO,
      segmentBlockRef = blockRef
    )
}

/**
 * Implements configured caching & IO strategies for all blocks within a Segment.
 */
class SegmentBlockCache(path: Path,
                        val segmentIO: SegmentIO,
                        segmentBlockRef: BlockRefReader[SegmentBlock.Offset])(implicit cacheMemorySweeper: Option[MemorySweeper.Cache]) {

  /**
   * @note Segment's [[IOStrategy]] is required to be immutable ones read and cannot mutate during runtime.
   *       Changing IOStrategy during runtime causes offset conflicts.
   * @see SegmentBlockCacheSpec which will fail is stored is set to false.
   */
  private val segmentIOStrategyCache = Lazy.value[IOStrategy](synchronised = true, stored = true, initial = None)

  @volatile var forceCacheSortedIndexAndValueReaders = false

  val sortedIndexReaderCacheName = "sortedIndexReaderCache"
  val valuesReaderCacheName = "valuesReaderCache"

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

  def buildBlockReaderCache[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy,
                                                             resourceName: String)(implicit blockOps: BlockOps[O, B]) =
    Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, BlockedReader[O, B], UnblockedReader[O, B]](
      strategy = reader => blockIO(reader.block.dataType).forceCacheOnAccess,
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$path: $resourceName"))
    ) {
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

  def buildBlockReaderCacheOptional[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy,
                                                                     resourceName: String)(implicit blockOps: BlockOps[O, B]) =
    Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Option[BlockedReader[O, B]], Option[UnblockedReader[O, B]]](
      strategy = _.map(reader => blockIO(reader.block.dataType).forceCacheOnAccess) getOrElse IOStrategy.defaultBlockReadersStored,
      reserveError = swaydb.Error.ReservedResource(Reserve.free(name = s"$path: $resourceName"))
    ) {
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

          Some(reader)

        }

      case (None, _) =>
        IO.none
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
            val segmentReader = createSegmentBlockReader()
            cache.value(Some(BlockRefReader.moveTo(offset, segmentReader)))

          case None =>
            cache.value(None)
        }
      }
      .get

  def getBlock[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, BlockRefReader[O], B],
                                                offset: SegmentFooterBlock => O)(implicit blockOps: BlockOps[O, B]): B =
    cache
      .getOrElse {
        val footer = getFooter()
        val segmentReader = createSegmentBlockReader()
        cache.value(BlockRefReader.moveTo(offset(footer), segmentReader))
      }
      .get

  def createReaderOptional[O <: BlockOffset, B <: Block[O]](cache: Cache[swaydb.Error.Segment, Option[BlockedReader[O, B]], Option[UnblockedReader[O, B]]],
                                                            getBlock: => Option[B])(implicit blockOps: BlockOps[O, B]): Option[UnblockedReader[O, B]] = {
    cache
      .getOrElse {
        getBlock match {
          case Some(block) =>
            val segmentReader = createSegmentBlockReader()
            cache.value(Some(BlockedReader(block, segmentReader)))

          case None =>
            cache.value(None)
        }
      }
    }
    .get
    .map(_.copy())

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
      initial = None
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
    buildBlockReaderCache[SegmentBlock.Offset, SegmentBlock](segmentBlockIO, "segmentReaderCache")

  private[block] val sortedIndexReaderCache =
    buildBlockReaderCache[SortedIndexBlock.Offset, SortedIndexBlock](sortedIndexBlockIO, sortedIndexReaderCacheName)

  private[block] val hashIndexReaderCache =
    buildBlockReaderCacheOptional[HashIndexBlock.Offset, HashIndexBlock](hashIndexBlockIO, "hashIndexReaderCache")

  private[block] val bloomFilterReaderCache =
    buildBlockReaderCacheOptional[BloomFilterBlock.Offset, BloomFilterBlock](bloomFilterBlockIO, "bloomFilterReaderCache")

  private[block] val binarySearchIndexReaderCache =
    buildBlockReaderCacheOptional[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](binarySearchIndexBlockIO, "binarySearchIndexReaderCache")

  private[block] val valuesReaderCache: Cache[swaydb.Error.Segment, Option[BlockedReader[ValuesBlock.Offset, ValuesBlock]], Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]] =
    buildBlockReaderCacheOptional[ValuesBlock.Offset, ValuesBlock](valuesBlockIO, valuesReaderCacheName)

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

  def readAll(): Slice[KeyValue] = {
    val keyValueCount = getFooter().keyValueCount
    val builder = Slice.newBuilder[KeyValue](keyValueCount)
    readAll(builder)
    builder.result
  }

  def readAll[T[_]](builder: mutable.Builder[KeyValue, T[KeyValue]]): Unit =
    readAll(
      keyValueCount = getFooter().keyValueCount,
      builder = builder
    )

  def readAll[T[_]](keyValueCount: Int): Slice[KeyValue] = {
    val builder = Slice.newBuilder[KeyValue](keyValueCount)
    readAll(
      keyValueCount = keyValueCount,
      builder = builder
    )
    builder.result
  }

  /**
   * Read all but also cache sortedIndex and valueBytes if they are not already cached.
   */
  def readAll[T[_]](keyValueCount: Int,
                    builder: mutable.Builder[KeyValue, T[KeyValue]]): Unit =
    try {
      var sortedIndexReader = createSortedIndexReader()
      if (sortedIndexReader.isFile) {
        forceCacheSortedIndexAndValueReaders = true
        sortedIndexReaderCache.clear()
        sortedIndexReader = createSortedIndexReader()
      }

      var valuesReader = createValuesReader()
      if (valuesReader.exists(_.isFile)) {
        forceCacheSortedIndexAndValueReaders = true
        valuesReaderCache.clear()
        valuesReader = createValuesReader()
      }

      SortedIndexBlock.readAll(
        keyValueCount = keyValueCount,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReader,
        builder = builder
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
}
