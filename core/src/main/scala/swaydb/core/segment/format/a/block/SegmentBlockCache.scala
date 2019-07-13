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

import swaydb.core.data.KeyValue
import swaydb.core.io.reader.BlockReader
import swaydb.core.util.FunctionUtil
import swaydb.core.util.cache.Cache
import swaydb.data.{IO, Reserve}
import swaydb.data.config.BlockIO
import swaydb.data.slice.{Reader, Slice}

object SegmentBlockCache {
  def apply(id: String,
            segmentBlockOffset: SegmentBlock.Offset,
            rawSegmentReader: () => Reader): SegmentBlockCache =
    new SegmentBlockCache(
      id = id,
      segmentBlockInfo =
        new SegmentBlockInfo(
          segmentBlockOffset = segmentBlockOffset,
          segmentReader = rawSegmentReader
        )
    )

  def createBlockReaderCache[B <: Block](blockIO: B => BlockIO,
                                         reserveError: IO.Error.Busy,
                                         segmentBlockReader: => IO[BlockReader[SegmentBlock]]): Cache[B, BlockReader[B]] =
    Cache.blockIO[B, BlockReader[B]](blockIO, reserveError) {
      block =>
        segmentBlockReader flatMap {
          segmentBlockReader =>
            Block.createDecompressedBlockReader(
              block = block,
              readFullBlockIfUncompressed = FunctionUtil.safeBoolean(blockIO(block).cacheOnAccess),
              segmentReader = segmentBlockReader
            )
        }
    }

  private[block] def segmentBlock(blockInfo: SegmentBlockInfo): IO[SegmentBlock] =
    SegmentBlock.read(
      offset = blockInfo.segmentBlockOffset,
      segmentReader = blockInfo.segmentReader()
    )

  private[block] def hashIndex(footer: SegmentBlock.Footer,
                               blockReader: BlockReader[SegmentBlock]): IO[Option[HashIndexBlock]] =
    footer.hashIndexOffset map {
      hashIndexOffset =>
        HashIndexBlock
          .read(hashIndexOffset, blockReader)
          .map(Some(_))
    } getOrElse IO.none

  private[block] def hashIndex(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                               blockReader: BlockReader[SegmentBlock]): IO[Option[HashIndexBlock]] =
    footer.value(blockReader) flatMap (hashIndex(_, blockReader))

  private[block] def bloomFilter(footer: SegmentBlock.Footer,
                                 blockReader: BlockReader[SegmentBlock]): IO[Option[BloomFilterBlock]] =
    footer.bloomFilterOffset map {
      bloomFilterOffset =>
        BloomFilterBlock
          .read(bloomFilterOffset, blockReader)
          .map(Some(_))
    } getOrElse IO.none

  private[block] def bloomFilter(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                                 blockReader: BlockReader[SegmentBlock]): IO[Option[BloomFilterBlock]] =
    footer.value(blockReader) flatMap (bloomFilter(_, blockReader))

  private[block] def binarySearchIndex(footer: SegmentBlock.Footer,
                                       blockReader: BlockReader[SegmentBlock]): IO[Option[BinarySearchIndexBlock]] =
    footer.binarySearchIndexOffset map {
      binarySearchIndexOffset =>
        BinarySearchIndexBlock
          .read(binarySearchIndexOffset, blockReader)
          .map(Some(_))
    } getOrElse IO.none

  private[block] def binarySearchIndex(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                                       blockReader: BlockReader[SegmentBlock]): IO[Option[BinarySearchIndexBlock]] =
    footer.value(blockReader) flatMap (binarySearchIndex(_, blockReader))

  private[block] def values(footer: SegmentBlock.Footer,
                            blockReader: BlockReader[SegmentBlock]): IO[Option[ValuesBlock]] =
    footer.valuesOffset map {
      valuesOffset =>
        ValuesBlock
          .read(valuesOffset, blockReader)
          .map(Some(_))
    } getOrElse IO.none

  private[block] def values(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                            blockReader: BlockReader[SegmentBlock]): IO[Option[ValuesBlock]] =
    footer.value(blockReader) flatMap (values(_, blockReader))

  private[block] def sortedIndex(footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer],
                                 blockReader: BlockReader[SegmentBlock]): IO[SortedIndexBlock] =
    footer.value(blockReader) flatMap {
      footer =>
        SortedIndexBlock
          .read(
            offset = footer.sortedIndexOffset,
            segmentReader = blockReader
          )
    }
}

protected class SegmentBlockInfo(val segmentBlockOffset: SegmentBlock.Offset,
                                 val segmentReader: () => Reader)

class SegmentBlockCache(id: String,
                        segmentBlockInfo: SegmentBlockInfo) {

  private[block] val segmentBlockCache: Cache[SegmentBlockInfo, SegmentBlock] =
  //    Cache.io[SegmentBlockInfo, SegmentBlock](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.segmentBlock)
    ???

  private[block] val footerCache: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer] =
  //    Cache.io[BlockReader[SegmentBlock], SegmentBlock.Footer](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlock.readFooter)
    ???

  private[block] val hashIndexCache: Cache[BlockReader[SegmentBlock], Option[HashIndexBlock]] =
  //    Cache.io[BlockReader[SegmentBlock], Option[HashIndex]](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.hashIndex(footerCache, _))
    ???

  private[block] val bloomFilterCache: Cache[BlockReader[SegmentBlock], Option[BloomFilterBlock]] =
  //    Cache.io[BlockReader[SegmentBlock], Option[BloomFilter]](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.bloomFilter(footerCache, _))
    ???

  private[block] val binarySearchIndexCache: Cache[BlockReader[SegmentBlock], Option[BinarySearchIndexBlock]] =
  //    Cache.io[BlockReader[SegmentBlock], Option[BinarySearchIndex]](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.binarySearchIndex(footerCache, _))
    ???

  private[block] val sortedIndexCache: Cache[BlockReader[SegmentBlock], SortedIndexBlock] =
  //    Cache.io[BlockReader[SegmentBlock], SortedIndex](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.sortedIndex(footerCache, _))
    ???

  private[block] val valuesCache: Cache[BlockReader[SegmentBlock], Option[ValuesBlock]] =
  //    Cache.io[BlockReader[SegmentBlock], Option[Values]](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.values(footerCache, _))
    ???

  private[block] val segmentBlockReaderCache: Cache[SegmentBlock, BlockReader[SegmentBlock]] =
    SegmentBlockCache.createBlockReaderCache[SegmentBlock](
      reserveError = IO.Error.DecompressingValues(Reserve()),
      blockIO = ???,
      segmentBlockReader = SegmentBlock.createUnblockedReader(segmentBlockInfo.segmentReader())
    )

  private[block] val hashIndexReaderCache: Cache[HashIndexBlock, BlockReader[HashIndexBlock]] =
    SegmentBlockCache.createBlockReaderCache[HashIndexBlock](
      reserveError = IO.Error.DecompressingValues(Reserve()),
      blockIO = ???,
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val bloomFilterReaderCache: Cache[BloomFilterBlock, BlockReader[BloomFilterBlock]] =
    SegmentBlockCache.createBlockReaderCache[BloomFilterBlock](
      reserveError = IO.Error.DecompressingValues(Reserve()),
      blockIO = ???,
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val binarySearchIndexReaderCache: Cache[BinarySearchIndexBlock, BlockReader[BinarySearchIndexBlock]] =
    SegmentBlockCache.createBlockReaderCache[BinarySearchIndexBlock](
      reserveError = IO.Error.DecompressingValues(Reserve()),
      blockIO = ???,
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val sortedIndexReaderCache: Cache[SortedIndexBlock, BlockReader[SortedIndexBlock]] =
    SegmentBlockCache.createBlockReaderCache[SortedIndexBlock](
      reserveError = IO.Error.DecompressingValues(Reserve()),
      blockIO = ???,
      segmentBlockReader = createSegmentBlockReader()
    )

  private[block] val valuesReaderCache: Cache[ValuesBlock, BlockReader[ValuesBlock]] =
    SegmentBlockCache.createBlockReaderCache[ValuesBlock](
      reserveError = IO.Error.DecompressingValues(Reserve()),
      blockIO = ???,
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

  def getHashIndex(): IO[Option[HashIndexBlock]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        hashIndexCache.value(segmentBlockReader)
    }

  def createHashIndexReader(): IO[Option[BlockReader[HashIndexBlock]]] =
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

  def getBloomFilter(): IO[Option[BloomFilterBlock]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        bloomFilterCache.value(segmentBlockReader)
    }

  def createBloomFilterReader(): IO[Option[BlockReader[BloomFilterBlock]]] =
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

  def getBinarySearchIndex(): IO[Option[BinarySearchIndexBlock]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        binarySearchIndexCache
          .value(segmentBlockReader)
    }

  def createBinarySearchIndexReader(): IO[Option[BlockReader[BinarySearchIndexBlock]]] =
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

  def getSortedIndex(): IO[SortedIndexBlock] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        sortedIndexCache.value(segmentBlockReader)
    }

  def createSortedIndexReader(): IO[BlockReader[SortedIndexBlock]] =
    getSortedIndex()
      .flatMap {
        block =>
          sortedIndexReaderCache
            .value(block)
            .map(_.copy())
      }

  def getValues(): IO[Option[ValuesBlock]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        valuesCache.value(segmentBlockReader)
    }

  def createValuesReader(): IO[Option[BlockReader[ValuesBlock]]] =
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

  def clear(): Unit =
    allCaches.foreach(_.clear())

  def isCached: Boolean =
    allCaches.exists(_.isCached)

  def isFooterDefined =
    footerCache.isCached

  def isBloomFilterDefined =
    bloomFilterCache.isCached
}
