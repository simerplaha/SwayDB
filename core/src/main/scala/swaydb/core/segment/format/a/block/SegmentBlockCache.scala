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

import swaydb.core.io.reader.BlockReader
import swaydb.core.util.cache.Cache
import swaydb.data.IO
import swaydb.data.slice.Reader

object SegmentBlockCache {
  def apply(id: String,
            segmentBlockOffset: SegmentBlock.Offset,
            rawSegmentReader: () => Reader): SegmentBlockCache =
    new SegmentBlockCache(
      id = id,
      segmentBlockInfo = new SegmentBlockInfo(segmentBlockOffset, rawSegmentReader)
    )

  def createBlockReaderCache[B <: Block](segmentBlockReader: => IO[BlockReader[SegmentBlock]]): Cache[B, BlockReader[B]] =
    Cache.delayedIO[B, BlockReader[B]](synchronised = _.compressionInfo.isDefined, reserved = _ => false, stored = _.compressionInfo.isDefined) {
      block =>
        segmentBlockReader flatMap {
          segmentBlockReader =>
            Block.createDecompressedBlockReader(
              block = block,
              readFullBlockIfUncompressed = false,
              segmentReader = segmentBlockReader
            )
        }
    }

  private[block] def segmentBlock(blockInfo: SegmentBlockInfo): IO[SegmentBlock] =
    SegmentBlock.read(
      offset = blockInfo.segmentBlockOffset,
      segmentReader = blockInfo.rawSegmentReader()
    )

  private[block] def hashIndex(footer: SegmentBlock.Footer,
                               blockReader: BlockReader[SegmentBlock]): IO[Option[HashIndex]] =
    footer.hashIndexOffset map {
      hashIndexOffset =>
        HashIndex
          .read(hashIndexOffset, blockReader)
          .map(Some(_))
    } getOrElse IO.none

  private[block] def hashIndex(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                               blockReader: BlockReader[SegmentBlock]): IO[Option[HashIndex]] =
    footer.value(blockReader) flatMap (hashIndex(_, blockReader))

  private[block] def bloomFilter(footer: SegmentBlock.Footer,
                                 blockReader: BlockReader[SegmentBlock]): IO[Option[BloomFilter]] =
    footer.bloomFilterOffset map {
      bloomFilterOffset =>
        BloomFilter
          .read(bloomFilterOffset, blockReader)
          .map(Some(_))
    } getOrElse IO.none

  private[block] def bloomFilter(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                                 blockReader: BlockReader[SegmentBlock]): IO[Option[BloomFilter]] =
    footer.value(blockReader) flatMap (bloomFilter(_, blockReader))

  private[block] def binarySearchIndex(footer: SegmentBlock.Footer,
                                       blockReader: BlockReader[SegmentBlock]): IO[Option[BinarySearchIndex]] =
    footer.binarySearchIndexOffset map {
      binarySearchIndexOffset =>
        BinarySearchIndex
          .read(binarySearchIndexOffset, blockReader)
          .map(Some(_))
    } getOrElse IO.none

  private[block] def binarySearchIndex(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                                       blockReader: BlockReader[SegmentBlock]): IO[Option[BinarySearchIndex]] =
    footer.value(blockReader) flatMap (binarySearchIndex(_, blockReader))

  private[block] def values(footer: SegmentBlock.Footer,
                            blockReader: BlockReader[SegmentBlock]): IO[Option[Values]] =
    footer.valuesOffset map {
      valuesOffset =>
        Values
          .read(valuesOffset, blockReader)
          .map(Some(_))
    } getOrElse IO.none

  private[block] def values(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                            blockReader: BlockReader[SegmentBlock]): IO[Option[Values]] =
    footer.value(blockReader) flatMap (values(_, blockReader))

  private[block] def sortedIndex(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                                 blockReader: BlockReader[SegmentBlock]): IO[SortedIndex] =
    footer.value(blockReader) flatMap {
      footer =>
        SortedIndex
          .read(
            offset = footer.sortedIndexOffset,
            segmentReader = blockReader
          )
    }
}

protected class SegmentBlockInfo(val segmentBlockOffset: SegmentBlock.Offset,
                                 val rawSegmentReader: () => Reader)

class SegmentBlockCache(id: String,
                        segmentBlockInfo: SegmentBlockInfo) {

  private[block] val segmentBlockCache: Cache[SegmentBlockInfo, SegmentBlock] =
    Cache.io[SegmentBlockInfo, SegmentBlock](
      synchronised = true,
      reserved = false,
      stored = false
    )(SegmentBlockCache.segmentBlock)

  private[block] val footerCache: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer] =
    Cache.io[BlockReader[SegmentBlock], SegmentBlock.Footer](
      synchronised = true,
      reserved = false,
      stored = true
    )(SegmentBlock.readFooter)

  private[block] val hashIndexCache: Cache[BlockReader[SegmentBlock], Option[HashIndex]] =
    Cache.io[BlockReader[SegmentBlock], Option[HashIndex]](
      synchronised = true,
      reserved = false,
      stored = true
    )(SegmentBlockCache.hashIndex(footerCache, _))

  private[block] val bloomFilterCache: Cache[BlockReader[SegmentBlock], Option[BloomFilter]] =
    Cache.io[BlockReader[SegmentBlock], Option[BloomFilter]](
      synchronised = true,
      reserved = false,
      stored = true
    )(SegmentBlockCache.bloomFilter(footerCache, _))

  private[block] val binarySearchIndexCache: Cache[BlockReader[SegmentBlock], Option[BinarySearchIndex]] =
    Cache.io[BlockReader[SegmentBlock], Option[BinarySearchIndex]](
      synchronised = true,
      reserved = false,
      stored = true
    )(SegmentBlockCache.binarySearchIndex(footerCache, _))

  private[block] val sortedIndexCache: Cache[BlockReader[SegmentBlock], SortedIndex] =
    Cache.io[BlockReader[SegmentBlock], SortedIndex](
      synchronised = true,
      reserved = false,
      stored = true
    )(SegmentBlockCache.sortedIndex(footerCache, _))

  private[block] val valuesCache: Cache[BlockReader[SegmentBlock], Option[Values]] =
    Cache.io[BlockReader[SegmentBlock], Option[Values]](
      synchronised = true,
      reserved = false,
      stored = true
    )(SegmentBlockCache.values(footerCache, _))

  private[block] val segmentBlockReaderCache: Cache[SegmentBlock, BlockReader[SegmentBlock]] =
    SegmentBlockCache.createBlockReaderCache[SegmentBlock](
      segmentBlockReader = SegmentBlock.createUnblockedReader(segmentBlockInfo.rawSegmentReader())
    )

  private[block] val hashIndexReaderCache: Cache[HashIndex, BlockReader[HashIndex]] =
    SegmentBlockCache.createBlockReaderCache[HashIndex](
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val bloomFilterReaderCache: Cache[BloomFilter, BlockReader[BloomFilter]] =
    SegmentBlockCache.createBlockReaderCache[BloomFilter](
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val binarySearchIndexReaderCache: Cache[BinarySearchIndex, BlockReader[BinarySearchIndex]] =
    SegmentBlockCache.createBlockReaderCache[BinarySearchIndex](
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val sortedIndexReaderCache: Cache[SortedIndex, BlockReader[SortedIndex]] =
    SegmentBlockCache.createBlockReaderCache[SortedIndex](
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val valuesReaderCache: Cache[Values, BlockReader[Values]] =
    SegmentBlockCache.createBlockReaderCache[Values](
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val allCaches =
    Seq(
      segmentBlockCache, footerCache, hashIndexCache, bloomFilterCache, binarySearchIndexCache, sortedIndexCache, valuesCache,
      segmentBlockReaderCache, hashIndexReaderCache, bloomFilterReaderCache, binarySearchIndexReaderCache, sortedIndexReaderCache, valuesReaderCache
    )

  private[block] def getSegmentBlock(): IO[SegmentBlock] =
    segmentBlockCache.value(segmentBlockInfo)

  private[block] def createSegmentBlockReader(): IO[BlockReader[SegmentBlock]] =
    getSegmentBlock() flatMap {
      segmentBlock =>
        segmentBlockReaderCache
          .value(segmentBlock)
          .map(_.copy())
    }

  def getFooter(): IO[SegmentBlock.Footer] =
    createSegmentBlockReader() flatMap {
      segmentBlock =>
        footerCache.value(segmentBlock)
    }

  def getHashIndex(): IO[Option[HashIndex]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        hashIndexCache.value(segmentBlockReader)
    }

  def createHashIndexReader(): IO[Option[BlockReader[HashIndex]]] =
    getHashIndex()
      .flatMap {
        block =>
          block map {
            hashIndex =>
              hashIndexReaderCache
                .value(hashIndex)
                .map {
                  reader =>
                    Some(reader.copy())
                }
          } getOrElse IO.none
      }

  def getBloomFilter(): IO[Option[BloomFilter]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        bloomFilterCache.value(segmentBlockReader)
    }

  def createBloomFilterReader(): IO[Option[BlockReader[BloomFilter]]] =
    getBloomFilter()
      .flatMap {
        block =>
          block map {
            block =>
              bloomFilterReaderCache
                .value(block)
                .map {
                  reader =>
                    Some(reader.copy())
                }
          } getOrElse IO.none
      }

  def getBinarySearchIndex(): IO[Option[BinarySearchIndex]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        binarySearchIndexCache
          .value(segmentBlockReader)
    }

  def createBinarySearchIndexReader(): IO[Option[BlockReader[BinarySearchIndex]]] =
    getBinarySearchIndex()
      .flatMap {
        block =>
          block map {
            block =>
              binarySearchIndexReaderCache
                .value(block)
                .map(reader => Some(reader.copy()))
          } getOrElse IO.none
      }

  def getSortedIndex(): IO[SortedIndex] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        sortedIndexCache.value(segmentBlockReader)
    }

  def createSortedIndexReader(): IO[BlockReader[SortedIndex]] =
    getSortedIndex()
      .flatMap {
        block =>
          sortedIndexReaderCache
            .value(block)
            .map(_.copy())
      }

  def getValues(): IO[Option[Values]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        valuesCache.value(segmentBlockReader)
    }

  def createValuesReader(): IO[Option[BlockReader[Values]]] =
    getValues()
      .flatMap {
        block =>
          block map {
            block =>
              valuesReaderCache
                .value(block)
                .map {
                  reader =>
                    Some(reader.copy())
                }
          } getOrElse IO.none
      }

  def clear(): Unit =
    allCaches.foreach(_.clear())

  def isOpen: Boolean =
    allCaches.exists(_.isCached)

  def isFooterDefined =
    footerCache.isCached

  def isBloomFilterDefined =
    bloomFilterCache.isCached
}
