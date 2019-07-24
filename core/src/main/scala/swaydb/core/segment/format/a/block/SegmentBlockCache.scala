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

import swaydb.IO
import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.block.ValuesBlock.ValuesBlockOps
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.util.cache.Cache
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.slice.Slice
import swaydb.data.Reserve
import swaydb.data.io.Core

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

  def segmentBlockIO = segmentIO.segmentBlockIO
  def hashIndexBlockIO = segmentIO.hashIndexBlockIO
  def bloomFilterBlockIO = segmentIO.bloomFilterBlockIO
  def binarySearchIndexBlockIO = segmentIO.binarySearchIndexBlockIO
  def sortedIndexBlockIO = segmentIO.sortedIndexBlockIO
  def valuesBlockIO = segmentIO.valuesBlockIO
  def segmentFooterBlockIO = segmentIO.segmentFooterBlockIO

  /**
    * Builds a required cache for [[SortedIndexBlock]].
    */
  def buildBlockInfoCache[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy)(implicit blockOps: BlockOps[O, B]): Cache[BlockRefReader[O], B] =
    Cache.blockIO[BlockRefReader[O], B](
      blockIO = ref => blockIO(IOAction.ReadDataOverview(ref.offset.size)),
      reserveError = Core.IO.Error.ReservedValue(Reserve())
    ) {
      ref =>
        Block
          .readHeader(ref)
          .flatMap(blockOps.readBlock)
    }

  def buildBlockInfoCacheOptional[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy)(implicit blockOps: BlockOps[O, B]): Cache[Option[BlockRefReader[O]], Option[B]] =
    Cache.blockIO[Option[BlockRefReader[O]], Option[B]](
      blockIO = ref => ref.map(ref => blockIO(IOAction.ReadDataOverview(ref.offset.size))) getOrElse IOStrategy.defaultBlockInfoStored,
      reserveError = Core.IO.Error.ReservedValue(Reserve())
    ) {
      ref =>
        ref map {
          ref =>
            Block
              .readHeader(ref)
              .flatMap(blockOps.readBlock)
              .map(Some(_))
        } getOrElse IO.none
    }

  def buildBlockReaderCache[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy)(implicit blockOps: BlockOps[O, B]) =
    Cache.blockIO[BlockedReader[O, B], UnblockedReader[O, B]](
      blockIO = reader => blockIO(reader.block.dataType),
      reserveError = Core.IO.Error.ReservedValue(Reserve())
    ) {
      blockedReader =>
        UnblockedReader(
          blockedReader = blockedReader,
          readAllIfUncompressed = blockIO(blockedReader.block.dataType).cacheOnAccess
        )
    }

  def buildBlockReaderCacheOptional[O <: BlockOffset, B <: Block[O]](blockIO: IOAction => IOStrategy)(implicit blockOps: BlockOps[O, B]) =
    Cache.blockIO[Option[BlockedReader[O, B]], Option[UnblockedReader[O, B]]](
      blockIO = _.map(reader => blockIO(reader.block.dataType)) getOrElse IOStrategy.defaultBlockReadersStored,
      reserveError = Core.IO.Error.ReservedValue(Reserve())
    ) {
      blockedReader =>
        blockedReader map {
          blockedReader =>
            UnblockedReader(
              blockedReader = blockedReader,
              readAllIfUncompressed = blockIO(blockedReader.block.dataType).cacheOnAccess
            ).map(Some(_))
        } getOrElse IO.none
    }

  private[block] val segmentBlockReaderCache =
    Cache.blockIO[BlockRefReader[SegmentBlock.Offset], UnblockedReader[SegmentBlock.Offset, SegmentBlock]](
      blockIO = ref => segmentBlockIO(IOAction.ReadDataOverview(ref.offset.size)),
      reserveError = Core.IO.Error.ReservedValue(Reserve())
    ) {
      ref =>
        BlockedReader(ref) flatMap {
          blockedReader =>
            UnblockedReader(
              blockedReader = blockedReader,
              readAllIfUncompressed = segmentBlockIO(blockedReader.block.dataType).cacheOnAccess
            )
        }
    }

  private[block] val footerBlockCache =
    Cache.blockIO[UnblockedReader[SegmentBlock.Offset, SegmentBlock], SegmentFooterBlock](
      blockIO =
        _ =>
          //reader does not contain any footer related info. Use the default known info about footer.
          segmentFooterBlockIO(IOAction.ReadDataOverview(SegmentFooterBlock.optimalBytesRequired)),
      reserveError = Core.IO.Error.ReservedValue(Reserve())
    ) {
      reader =>
        SegmentFooterBlock.read(reader)
    }

  //info caches
  private[block] val sortedIndexBlockCache =
    buildBlockInfoCache[SortedIndexBlock.Offset, SortedIndexBlock](sortedIndexBlockIO)

  private[block] val hashIndexBlockCache =
    buildBlockInfoCacheOptional[HashIndexBlock.Offset, HashIndexBlock](hashIndexBlockIO)

  private[block] val bloomFilterBlockCache =
    buildBlockInfoCacheOptional[BloomFilterBlock.Offset, BloomFilterBlock](bloomFilterBlockIO)

  private[block] val binarySearchIndexBlockCache =
    buildBlockInfoCacheOptional[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](binarySearchIndexBlockIO)

  private[block] val valuesBlockCache =
    buildBlockInfoCacheOptional[ValuesBlock.Offset, ValuesBlock](valuesBlockIO)

  //reader caches
  private[block] val sortedIndexReaderCache =
    buildBlockReaderCache[SortedIndexBlock.Offset, SortedIndexBlock](sortedIndexBlockIO)

  private[block] val hashIndexReaderCache =
    buildBlockReaderCacheOptional[HashIndexBlock.Offset, HashIndexBlock](hashIndexBlockIO)

  private[block] val bloomFilterReaderCache =
    buildBlockReaderCacheOptional[BloomFilterBlock.Offset, BloomFilterBlock](bloomFilterBlockIO)

  private[block] val binarySearchIndexReaderCache =
    buildBlockReaderCacheOptional[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](binarySearchIndexBlockIO)

  private[block] val valuesReaderCache: Cache[Option[BlockedReader[ValuesBlock.Offset, ValuesBlock]], Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]] =
    buildBlockReaderCacheOptional[ValuesBlock.Offset, ValuesBlock](valuesBlockIO)

  private[block] val allCaches =
    Seq(
      segmentBlockReaderCache, footerBlockCache, sortedIndexBlockCache, hashIndexBlockCache, bloomFilterBlockCache,
      binarySearchIndexBlockCache, valuesBlockCache, hashIndexReaderCache, bloomFilterReaderCache, binarySearchIndexReaderCache,
      sortedIndexReaderCache, valuesReaderCache
    )

  private[block] def createSegmentBlockReader(): IO[Core.IO.Error, UnblockedReader[SegmentBlock.Offset, SegmentBlock]] =
    segmentBlockReaderCache
      .value(segmentBlockRef.copy())
      .map(_.copy())

  def getFooter(): IO[Core.IO.Error, SegmentFooterBlock] =
    footerBlockCache getOrElse {
      createSegmentBlockReader().flatMap(footerBlockCache.value(_))
    }

  def getBlockOptional[O <: BlockOffset, B <: Block[O]](cache: Cache[Option[BlockRefReader[O]], Option[B]], offset: SegmentFooterBlock => Option[O]) =
    cache getOrElse {
      getFooter() flatMap {
        footer =>
          offset(footer) map {
            offset =>
              createSegmentBlockReader() flatMap {
                segmentReader =>
                  cache.value(Some(BlockRefReader.moveWithin(offset, segmentReader)))
              }
          } getOrElse cache.value(None)
      }
    }

  def getBlock[O <: BlockOffset, B <: Block[O]](cache: Cache[BlockRefReader[O], B], offset: SegmentFooterBlock => O) =
    cache getOrElse {
      getFooter() flatMap {
        footer =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              cache.value(BlockRefReader.moveWithin(offset(footer), segmentReader))
          }
      }
    }

  def getHashIndex(): IO[Core.IO.Error, Option[HashIndexBlock]] =
    getBlockOptional(hashIndexBlockCache, _.hashIndexOffset)

  def getBloomFilter(): IO[Core.IO.Error, Option[BloomFilterBlock]] =
    getBlockOptional(bloomFilterBlockCache, _.bloomFilterOffset)

  def getBinarySearchIndex(): IO[Core.IO.Error, Option[BinarySearchIndexBlock]] =
    getBlockOptional(binarySearchIndexBlockCache, _.binarySearchIndexOffset)

  def getSortedIndex(): IO[Core.IO.Error, SortedIndexBlock] =
    getBlock(sortedIndexBlockCache, _.sortedIndexOffset)

  def getValues(): IO[Core.IO.Error, Option[ValuesBlock]] =
    getBlockOptional(valuesBlockCache, _.valuesOffset)

  def createReaderOptional[O <: BlockOffset, B <: Block[O]](cache: Cache[Option[BlockedReader[O, B]], Option[UnblockedReader[O, B]]], getBlock: => IO[Core.IO.Error, Option[B]]): IO[Core.IO.Error, Option[UnblockedReader[O, B]]] = {
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

  def createReader[O <: BlockOffset, B <: Block[O]](cache: Cache[BlockedReader[O, B], UnblockedReader[O, B]], getBlock: => IO[Core.IO.Error, B]): IO[Core.IO.Error, UnblockedReader[O, B]] = {
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

  def createHashIndexReader(): IO[Core.IO.Error, Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]]] =
    createReaderOptional(hashIndexReaderCache, getHashIndex())

  def createBloomFilterReader(): IO[Core.IO.Error, Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]]] =
    createReaderOptional(bloomFilterReaderCache, getBloomFilter())

  def createBinarySearchIndexReader(): IO[Core.IO.Error, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]] =
    createReaderOptional(binarySearchIndexReaderCache, getBinarySearchIndex())

  def createValuesReader(): IO[Core.IO.Error, Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]] =
    createReaderOptional(valuesReaderCache, getValues())

  def createSortedIndexReader(): IO[Core.IO.Error, UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]] =
    createReader(sortedIndexReaderCache, getSortedIndex())

  def readAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None) =
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

  def readAllBytes(): IO[Core.IO.Error, Slice[Byte]] =
    segmentBlockRef.copy().readAll()

  def clear(): Unit =
    allCaches.foreach(_.clear())

  def isCached: Boolean =
    allCaches.exists(_.isCached)

  def isFooterDefined =
    footerBlockCache.isCached

  def isBloomFilterDefined =
    bloomFilterBlockCache.isCached
}
