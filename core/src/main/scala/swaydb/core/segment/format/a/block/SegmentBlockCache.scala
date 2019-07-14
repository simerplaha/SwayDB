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
import swaydb.core.segment.format.a.block.reader.DecompressedBlockReader
import swaydb.core.util.cache.Cache
import swaydb.data.IO
import swaydb.data.config.{BlockIO, BlockStatus}
import swaydb.data.slice.{Reader, Slice}

object SegmentBlockCache {
  def apply(id: String,
            segmentBlockIO: BlockStatus => BlockIO,
            hashIndexBlockIO: BlockStatus => BlockIO,
            binarySearchIndexBlockIO: BlockStatus => BlockIO,
            sortedIndexBlockIO: BlockStatus => BlockIO,
            valuesBlockIO: BlockStatus => BlockIO,
            segmentFooterBlockIO: BlockStatus => BlockIO,
            segmentBlockOffset: SegmentBlock.Offset,
            rawSegmentReader: () => Reader): SegmentBlockCache =
    new SegmentBlockCache(
      id = id,
      segmentBlockIO = segmentBlockIO,
      hashIndexBlockIO = hashIndexBlockIO,
      binarySearchIndexBlockIO = binarySearchIndexBlockIO,
      sortedIndexBlockIO = sortedIndexBlockIO,
      valuesBlockIO = valuesBlockIO,
      segmentFooterBlockIO = segmentFooterBlockIO,
      segmentBlockInfo =
        new SegmentBlockInfo(
          segmentBlockOffset = segmentBlockOffset,
          segmentReader = rawSegmentReader
        )
    )
  //
  //  def createBlockDataReaderCache[B <: Block](blockIO: B => BlockIO,
  //                                             reserveError: IO.Error.Busy,
  //                                             segmentBlockReader: => IO[BlockedReader[SegmentBlock]])(implicit blockUpdater: BlockUpdater[B]): Cache[B, UnblockReader[B]] =
  //    Cache.blockIO[B, UnblockReader[B]](blockIO, reserveError) {
  //      block =>
  //        segmentBlockReader flatMap {
  //          segmentBlockReader =>
  //            Block.createBlockDataReader(
  //              block = block,
  //              readFullBlockIfUncompressed = FunctionUtil.safeBoolean(blockIO(block).cacheOnAccess),
  //              segmentReader = segmentBlockReader
  //            )
  //        }
  //    }
  //
  //  private[block] def segmentBlock(blockStatus: SegmentBlockInfo): IO[SegmentBlock] =
  //    SegmentBlock.read(
  //      offset = blockStatus.segmentBlockOffset,
  //      segmentReader = blockStatus.segmentReader()
  //    )
  //
  //  private[block] def hashIndex(footer: SegmentFooterBlock,
  //                               blockReader: BlockedReader[SegmentBlock]): IO[Option[HashIndexBlock]] =
  //    footer.hashIndexOffset map {
  //      hashIndexOffset =>
  //        HashIndexBlock
  //          .read(hashIndexOffset, blockReader)
  //          .map(Some(_))
  //    } getOrElse IO.none
  //
  //  private[block] def hashIndex(footer: Cache[BlockedReader[SegmentBlock], SegmentFooterBlock],
  //                               blockReader: BlockedReader[SegmentBlock]): IO[Option[HashIndexBlock]] =
  //    footer.value(blockReader) flatMap (hashIndex(_, blockReader))
  //
  //  private[block] def bloomFilter(footer: SegmentFooterBlock,
  //                                 blockReader: BlockedReader[SegmentBlock]): IO[Option[BloomFilterBlock]] =
  //    footer.bloomFilterOffset map {
  //      bloomFilterOffset =>
  //        BloomFilterBlock
  //          .read(bloomFilterOffset, blockReader)
  //          .map(Some(_))
  //    } getOrElse IO.none
  //
  //  private[block] def bloomFilter(footer: Cache[UnblockReader[SegmentBlock], SegmentFooterBlock],
  //                                 blockReader: UnblockReader[SegmentBlock]): IO[Option[BloomFilterBlock]] =
  //    footer.value(blockReader) flatMap (bloomFilter(_, blockReader))
  //
  //  private[block] def binarySearchIndex(footer: SegmentFooterBlock,
  //                                       blockReader: UnblockReader[SegmentBlock]): IO[Option[BinarySearchIndexBlock]] =
  //    footer.binarySearchIndexOffset map {
  //      binarySearchIndexOffset =>
  //        BinarySearchIndexBlock
  //          .read(binarySearchIndexOffset, blockReader)
  //          .map(Some(_))
  //    } getOrElse IO.none
  //
  //  private[block] def binarySearchIndex(footer: Cache[UnblockReader[SegmentBlock], SegmentFooterBlock],
  //                                       blockReader: UnblockReader[SegmentBlock]): IO[Option[BinarySearchIndexBlock]] =
  //    footer.value(blockReader) flatMap (binarySearchIndex(_, blockReader))
  //
  //  private[block] def values(footer: SegmentFooterBlock,
  //                            blockReader: UnblockReader[SegmentBlock]): IO[Option[ValuesBlock]] =
  //    footer.valuesOffset map {
  //      valuesOffset =>
  //        ValuesBlock
  //          .read(valuesOffset, blockReader)
  //          .map(Some(_))
  //    } getOrElse IO.none
  //
  //  private[block] def values(footer: Cache[UnblockReader[SegmentBlock], SegmentFooterBlock],
  //                            blockReader: UnblockReader[SegmentBlock]): IO[Option[ValuesBlock]] =
  //    footer.value(blockReader) flatMap (values(_, blockReader))
  //
  //  private[block] def sortedIndex(footer: Cache[UnblockReader[SegmentBlock], SegmentFooterBlock],
  //                                 blockReader: UnblockReader[SegmentBlock]): IO[SortedIndexBlock] =
  //    footer.value(blockReader) flatMap {
  //      footer =>
  //        SortedIndexBlock
  //          .read(
  //            offset = footer.sortedIndexOffset,
  //            segmentReader = blockReader
  //          )
  //    }
}

protected class SegmentBlockInfo(val segmentBlockOffset: SegmentBlock.Offset,
                                 val segmentReader: () => Reader)

class SegmentBlockCache(id: String,
                        segmentBlockIO: BlockStatus => BlockIO,
                        hashIndexBlockIO: BlockStatus => BlockIO,
                        binarySearchIndexBlockIO: BlockStatus => BlockIO,
                        sortedIndexBlockIO: BlockStatus => BlockIO,
                        valuesBlockIO: BlockStatus => BlockIO,
                        segmentFooterBlockIO: BlockStatus => BlockIO,
                        segmentBlockInfo: SegmentBlockInfo) {

  private[block] val segmentBlockCache: Cache[SegmentBlockInfo, SegmentBlock] =
  //    Cache.blockIO[SegmentBlockInfo, SegmentBlock](
  //      blockIO = _ => segmentBlockIO(BlockStatus.BlockInfo(8)),
  //      reserveError = IO.Error.ReservedValue(Reserve())
  //    )(SegmentBlockCache.segmentBlock)
    ???

  private[block] val footerCache: Cache[DecompressedBlockReader[SegmentBlock], SegmentFooterBlock] =
  //    Cache.blockIO[UnblockReader[SegmentBlock], SegmentFooterBlock](
  //      blockIO =
  //        blockStatus =>
  //          //          segmentFooterBlockIO(
  //          //            BlockStatus(
  //          //              _isBlockInfo = true,
  //          //              _isCompressed = false,
  //          //              _compressedSize = blockStatus.,
  //          //              _decompressedSize = blockStatus.segmentBlockOffset.size
  //          //            )
  //          //          ),
  //          ???,
  //      reserveError = IO.Error.ReservedValue(Reserve())
  //    )(SegmentFooterBlock.read)
    ???

  private[block] val hashIndexCache: Cache[DecompressedBlockReader[SegmentBlock], Option[HashIndexBlock]] =
  //    Cache.io[BlockReader[SegmentBlock], Option[HashIndex]](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.hashIndex(footerCache, _))
    ???

  private[block] val bloomFilterCache: Cache[DecompressedBlockReader[SegmentBlock], Option[BloomFilterBlock]] =
  //    Cache.io[BlockReader[SegmentBlock], Option[BloomFilter]](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.bloomFilter(footerCache, _))
    ???

  private[block] val binarySearchIndexCache: Cache[DecompressedBlockReader[SegmentBlock], Option[BinarySearchIndexBlock]] =
  //    Cache.io[BlockReader[SegmentBlock], Option[BinarySearchIndex]](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.binarySearchIndex(footerCache, _))
    ???

  private[block] val sortedIndexCache: Cache[DecompressedBlockReader[SegmentBlock], SortedIndexBlock] =
  //    Cache.io[BlockReader[SegmentBlock], SortedIndex](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.sortedIndex(footerCache, _))
    ???

  private[block] val valuesCache: Cache[DecompressedBlockReader[SegmentBlock], Option[ValuesBlock]] =
  //    Cache.io[BlockReader[SegmentBlock], Option[Values]](
  //      synchronised = true,
  //      reserved = false,
  //      stored = true
  //    )(SegmentBlockCache.values(footerCache, _))
    ???

  private[block] val segmentBlockReaderCache: Cache[SegmentBlock, DecompressedBlockReader[SegmentBlock]] =
  //    SegmentBlockCache.createBlockDataReaderCache[SegmentBlock](
  //      reserveError = IO.Error.DecompressingValues(Reserve()),
  //      blockIO = ???,
  //      segmentBlockReader = SegmentBlock.createUnUnblockReader(segmentBlockInfo.segmentReader())
  //    )
    ???

  private[block] val hashIndexReaderCache: Cache[HashIndexBlock, DecompressedBlockReader[HashIndexBlock]] =
  //    SegmentBlockCache.createBlockDataReaderCache[HashIndexBlock](
  //      reserveError = IO.Error.DecompressingValues(Reserve()),
  //      blockIO = ???,
  //      segmentBlockReader = createSegmentBlockReader()
  //    )
    ???

  private[block] val bloomFilterReaderCache: Cache[BloomFilterBlock, DecompressedBlockReader[BloomFilterBlock]] =
  //    SegmentBlockCache.createBlockDataReaderCache[BloomFilterBlock](
  //      reserveError = IO.Error.DecompressingValues(Reserve()),
  //      blockIO = ???,
  //      segmentBlockReader = createSegmentBlockReader()
  //    )
    ???

  private[block] val binarySearchIndexReaderCache: Cache[BinarySearchIndexBlock, DecompressedBlockReader[BinarySearchIndexBlock]] =
  //    SegmentBlockCache.createBlockDataReaderCache[BinarySearchIndexBlock](
  //      reserveError = IO.Error.DecompressingValues(Reserve()),
  //      blockIO = ???,
  //      segmentBlockReader = createSegmentBlockReader()
  //    )
    ???

  private[block] val sortedIndexReaderCache: Cache[SortedIndexBlock, DecompressedBlockReader[SortedIndexBlock]] =
  //    SegmentBlockCache.createBlockDataReaderCache[SortedIndexBlock](
  //      reserveError = IO.Error.DecompressingValues(Reserve()),
  //      blockIO = ???,
  //      segmentBlockReader = createSegmentBlockReader()
  //    )
    ???

  private[block] val valuesReaderCache: Cache[ValuesBlock, DecompressedBlockReader[ValuesBlock]] =
  //    SegmentBlockCache.createBlockDataReaderCache[ValuesBlock](
  //      reserveError = IO.Error.DecompressingValues(Reserve()),
  //      blockIO = ???,
  //      segmentBlockReader = createSegmentBlockReader()
  //    )
    ???

  private[block] val allCaches =
    Seq(
      segmentBlockCache, footerCache, hashIndexCache, bloomFilterCache, binarySearchIndexCache, sortedIndexCache, valuesCache,
      segmentBlockReaderCache, hashIndexReaderCache, bloomFilterReaderCache, binarySearchIndexReaderCache, sortedIndexReaderCache, valuesReaderCache
    )

  private[block] def getSegmentBlock(): IO[SegmentBlock] =
    segmentBlockCache.value(segmentBlockInfo)

  private[block] def createSegmentBlockReader(): IO[DecompressedBlockReader[SegmentBlock]] =
  //    getSegmentBlock() flatMap {
  //      segmentBlock =>
  //        segmentBlockReaderCache
  //          .value(segmentBlock)
  //          .map(_.copy())
  //    }
    ???

  def getFooter(): IO[SegmentFooterBlock] =
  //    createSegmentBlockReader() flatMap {
  //      segmentBlock =>
  //        footerCache.value(segmentBlock)
  //    }
    ???

  def getHashIndex(): IO[Option[HashIndexBlock]] =
  //    createSegmentBlockReader() flatMap {
  //      segmentBlockReader =>
  //        hashIndexCache.value(segmentBlockReader)
  //    }
    ???

  def createHashIndexReader(): IO[Option[DecompressedBlockReader[HashIndexBlock]]] =
  //    getHashIndex()
  //      .flatMap {
  //        block =>
  //          block map {
  //            hashIndex =>
  //              hashIndexReaderCache
  //                .value(hashIndex)
  //                .map {
  //                  reader =>
  //                    Some(reader.copy())
  //                }
  //          } getOrElse IO.none
  //      }
    ???

  def getBloomFilter(): IO[Option[BloomFilterBlock]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        bloomFilterCache.value(segmentBlockReader)
    }

  def createBloomFilterReader(): IO[Option[DecompressedBlockReader[BloomFilterBlock]]] =
  //    getBloomFilter()
  //      .flatMap {
  //        block =>
  //          block map {
  //            block =>
  //              bloomFilterReaderCache
  //                .value(block)
  //                .map {
  //                  reader =>
  //                    Some(reader.copy())
  //                }
  //          } getOrElse IO.none
  //      }
    ???

  def getBinarySearchIndex(): IO[Option[BinarySearchIndexBlock]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        binarySearchIndexCache
          .value(segmentBlockReader)
    }

  def createBinarySearchIndexReader(): IO[Option[DecompressedBlockReader[BinarySearchIndexBlock]]] =
  //    getBinarySearchIndex()
  //      .flatMap {
  //        block =>
  //          block map {
  //            block =>
  //              binarySearchIndexReaderCache
  //                .value(block)
  //                .map(reader => Some(reader.copy()))
  //          } getOrElse IO.none
  //      }
    ???

  def getSortedIndex(): IO[SortedIndexBlock] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        sortedIndexCache.value(segmentBlockReader)
    }

  def createSortedIndexReader(): IO[DecompressedBlockReader[SortedIndexBlock]] =
  //    getSortedIndex()
  //      .flatMap {
  //        block =>
  //          sortedIndexReaderCache
  //            .value(block)
  //            .map(_.copy())
  //      }
    ???

  def getValues(): IO[Option[ValuesBlock]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        valuesCache.value(segmentBlockReader)
    }

  def createValuesReader(): IO[Option[DecompressedBlockReader[ValuesBlock]]] =
  //    getValues()
  //      .flatMap {
  //        block =>
  //          block map {
  //            block =>
  //              valuesReaderCache
  //                .value(block)
  //                .map {
  //                  reader =>
  //                    Some(reader.copy())
  //                }
  //          } getOrElse IO.none
  //      }
    ???

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
