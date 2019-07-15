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
import swaydb.core.segment.format.a.block.reader.{CompressedBlockReader, DecompressedBlockReader}
import swaydb.core.util.cache.Cache
import swaydb.data.config.{BlockIO, BlockStatus}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.{IO, Reserve}

object SegmentBlockCache {
  def apply(id: String,
            segmentBlockIO: BlockStatus => BlockIO,
            hashIndexBlockIO: BlockStatus => BlockIO,
            bloomFilterBlockIO: BlockStatus => BlockIO,
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
      bloomFilterBlockIO = bloomFilterBlockIO,
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
}

protected class SegmentBlockInfo(val segmentBlockOffset: SegmentBlock.Offset,
                                 val segmentReader: () => Reader)

/**
  * Implements configured caching & IO strategies for all blocks within a Segment.
  */
class SegmentBlockCache(id: String,
                        segmentBlockIO: BlockStatus => BlockIO,
                        hashIndexBlockIO: BlockStatus => BlockIO,
                        bloomFilterBlockIO: BlockStatus => BlockIO,
                        binarySearchIndexBlockIO: BlockStatus => BlockIO,
                        sortedIndexBlockIO: BlockStatus => BlockIO,
                        valuesBlockIO: BlockStatus => BlockIO,
                        segmentFooterBlockIO: BlockStatus => BlockIO,
                        segmentBlockInfo: SegmentBlockInfo) {

  /**
    * Full Segment cache.
    */
  private[block] val segmentBlockCache: Cache[SegmentBlockInfo, SegmentBlock] =
    Cache.blockIO[SegmentBlockInfo, SegmentBlock](
      blockIO = _ => segmentBlockIO(BlockStatus.BlockInfo(SegmentBlock.hasCompressionHeaderSize)),
      reserveError = IO.Error.ReservedValue(Reserve())
    ) {
      segmentBlockInfo =>
        SegmentBlock.read(
          offset = segmentBlockInfo.segmentBlockOffset,
          segmentReader = segmentBlockInfo.segmentReader()
        )
    }

  private[block] val footerBlockCache =
    Cache.blockIO[DecompressedBlockReader[SegmentBlock], SegmentFooterBlock](
      blockIO = _ => segmentFooterBlockIO(BlockStatus.BlockInfo(SegmentFooterBlock.optimalBytesRequired)),
      reserveError = IO.Error.ReservedValue(Reserve())
    )(SegmentFooterBlock.read)

  /**
    * Builds a an optional BlockInfo cache. Blocks like [[SortedIndexBlock]], [[BloomFilterBlock]] etc are optional.
    */
  def buildIndexBlockInfoCacheOptional[O <: BlockOffset, B <: Block](blockIO: BlockStatus => BlockIO)(fetch: (Option[O], DecompressedBlockReader[SegmentBlock]) => IO[Option[B]]) =
    Cache.blockIO[(Option[O], DecompressedBlockReader[SegmentBlock]), Option[B]](
      blockIO =
        _._1 match {
          case Some(blockOffset) =>
            blockIO(BlockStatus.BlockInfo(blockOffset.size))

          case None =>
            BlockIO.defaultBlockInfo
        },
      reserveError =
        IO.Error.ReservedValue(Reserve())
    ) {
      case (offset, reader) =>
        fetch(offset, reader)
    }

  /**
    * Builds a required cache for [[SortedIndexBlock]].
    */
  def buildIndexBlockInfoCache[O <: BlockOffset, B <: Block](blockIO: BlockStatus => BlockIO)(fetch: (O, DecompressedBlockReader[SegmentBlock]) => IO[B]) =
    Cache.blockIO[(O, DecompressedBlockReader[SegmentBlock]), B](
      blockIO = blockOffsetAndReader => blockIO(BlockStatus.BlockInfo(blockOffsetAndReader._1.size)),
      reserveError = IO.Error.ReservedValue(Reserve())
    ) {
      case (offset, reader) =>
        fetch(offset, reader)
    }

  private[block] val hashIndexBlockCache =
    buildIndexBlockInfoCacheOptional[HashIndexBlock.Offset, HashIndexBlock](hashIndexBlockIO) {
      case (offset, segmentBlockReader) =>
        offset map {
          hashIndexOffset =>
            HashIndexBlock.read(
              offset = hashIndexOffset,
              reader = segmentBlockReader
            ).map(Some(_))
        } getOrElse IO.none
    }

  private[block] val bloomFilterBlockCache =
    buildIndexBlockInfoCacheOptional[BloomFilterBlock.Offset, BloomFilterBlock](bloomFilterBlockIO) {
      case (offset, segmentBlockReader) =>
        offset map {
          offset =>
            BloomFilterBlock.read(
              offset = offset,
              segmentReader = segmentBlockReader
            ).map(Some(_))
        } getOrElse IO.none
    }

  private[block] val binarySearchIndexBlockCache =
    buildIndexBlockInfoCacheOptional[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock](binarySearchIndexBlockIO) {
      case (offset, segmentBlockReader) =>
        offset map {
          offset =>
            BinarySearchIndexBlock.read(
              offset = offset,
              reader = segmentBlockReader
            ).map(Some(_))
        } getOrElse IO.none
    }

  private[block] val sortedIndexBlockCache =
    buildIndexBlockInfoCache[SortedIndexBlock.Offset, SortedIndexBlock](sortedIndexBlockIO) {
      case (offset, segmentBlockReader) =>
        SortedIndexBlock.read(
          offset = offset,
          segmentReader = segmentBlockReader
        )
    }

  private[block] val valuesBlockCache =
    buildIndexBlockInfoCacheOptional[ValuesBlock.Offset, ValuesBlock](valuesBlockIO) {
      case (offset, segmentBlockReader) =>
        offset map {
          offset =>
            ValuesBlock.read(
              offset = offset,
              segmentReader = segmentBlockReader
            ).map(Some(_))
        } getOrElse IO.none
    }

  private[block] val segmentBlockReaderCache =
    Cache.blockIO[SegmentBlock, DecompressedBlockReader[SegmentBlock]](
      blockIO = segmentBlock => segmentBlockIO(segmentBlock.blockStatus),
      reserveError = IO.Error.ReservedValue(Reserve())
    ) {
      segmentBlock =>
        Block.decompress(
          reader =
            CompressedBlockReader.compressed(
              block = segmentBlock,
              reader = segmentBlockInfo.segmentReader()
            ),
          readAllIfUncompressed =
            segmentBlockIO(segmentBlock.blockStatus).cacheOnAccess
        )
    }

  def buildBlockReaderCacheOptional[B <: Block](blockIO: BlockStatus => BlockIO)(implicit blockUpdater: BlockUpdater[B]) =
    Cache.blockIO[(Option[B], DecompressedBlockReader[SegmentBlock]), Option[DecompressedBlockReader[B]]](
      blockIO =
        _._1 match {
          case Some(block) =>
            blockIO(block.blockStatus)

          case None =>
            BlockIO.defaultBlockReaders
        },
      reserveError =
        IO.Error.ReservedValue(Reserve())
    ) {
      case (block, segmentReader) =>
        block map {
          block =>
            Block.decompress(
              childBlock = block,
              parentBlock = segmentReader,
              readAllIfUncompressed = blockIO(block.blockStatus).cacheOnAccess
            ).map(Some(_))
        } getOrElse IO.none
    }

  def buildBlockReaderCache[B <: Block](blockIO: BlockStatus => BlockIO)(implicit blockUpdater: BlockUpdater[B]) =
    Cache.blockIO[(B, DecompressedBlockReader[SegmentBlock]), DecompressedBlockReader[B]](
      blockIO = blockAndReader => blockIO(blockAndReader._1.blockStatus),
      reserveError = IO.Error.ReservedValue(Reserve())
    ) {
      case (block, segmentReader) =>
        Block.decompress(
          childBlock = block,
          parentBlock = segmentReader,
          readAllIfUncompressed = blockIO(block.blockStatus).cacheOnAccess
        )
    }

  private[block] val hashIndexReaderCache =
    buildBlockReaderCacheOptional[HashIndexBlock](hashIndexBlockIO)

  private[block] val bloomFilterReaderCache =
    buildBlockReaderCacheOptional[BloomFilterBlock](bloomFilterBlockIO)

  private[block] val binarySearchIndexReaderCache =
    buildBlockReaderCacheOptional[BinarySearchIndexBlock](binarySearchIndexBlockIO)

  private[block] val sortedIndexReaderCache =
    buildBlockReaderCache[SortedIndexBlock](sortedIndexBlockIO)

  private[block] val valuesReaderCache =
    buildBlockReaderCacheOptional[ValuesBlock](valuesBlockIO)

  private[block] val allCaches =
    Seq(
      segmentBlockCache, footerBlockCache, hashIndexBlockCache, bloomFilterBlockCache, binarySearchIndexBlockCache, sortedIndexBlockCache, valuesBlockCache,
      segmentBlockReaderCache, hashIndexReaderCache, bloomFilterReaderCache, binarySearchIndexReaderCache, sortedIndexReaderCache, valuesReaderCache
    )

  private[block] def getSegmentBlock(): IO[SegmentBlock] =
    segmentBlockCache getOrElse {
      segmentBlockCache.value(segmentBlockInfo)
    }

  private[block] def createSegmentBlockReader(): IO[DecompressedBlockReader[SegmentBlock]] =
    segmentBlockReaderCache getOrElse {
      getSegmentBlock() flatMap {
        segmentBlock =>
          segmentBlockReaderCache.value(segmentBlock)
      }
    }.map(_.copy())

  def getFooter(): IO[SegmentFooterBlock] =
    footerBlockCache getOrElse {
      createSegmentBlockReader() flatMap {
        segmentReader =>
          footerBlockCache.value(segmentReader)
      }
    }

  def getHashIndex(): IO[Option[HashIndexBlock]] =
    hashIndexBlockCache getOrElse {
      getFooter() flatMap {
        footer =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              hashIndexBlockCache.value(footer.hashIndexOffset, segmentReader)
          }
      }
    }

  def createHashIndexReader(): IO[Option[DecompressedBlockReader[HashIndexBlock]]] =
    hashIndexReaderCache getOrElse {
      getHashIndex() flatMap {
        hashIndex =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              hashIndexReaderCache.value(hashIndex, segmentReader)
          }
      }
    }.map(_.map(_.copy()))

  def getBloomFilter(): IO[Option[BloomFilterBlock]] =
    bloomFilterBlockCache getOrElse {
      getFooter() flatMap {
        footer =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              bloomFilterBlockCache.value(footer.bloomFilterOffset, segmentReader)
          }
      }
    }

  def createBloomFilterReader(): IO[Option[DecompressedBlockReader[BloomFilterBlock]]] =
    bloomFilterReaderCache getOrElse {
      getBloomFilter() flatMap {
        bloomFilter =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              bloomFilterReaderCache.value(bloomFilter, segmentReader)
          }
      }
    }.map(_.map(_.copy()))

  def getBinarySearchIndex(): IO[Option[BinarySearchIndexBlock]] =
    binarySearchIndexBlockCache getOrElse {
      getFooter() flatMap {
        footer =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              binarySearchIndexBlockCache.value(footer.binarySearchIndexOffset, segmentReader)
          }
      }
    }

  def createBinarySearchIndexReader(): IO[Option[DecompressedBlockReader[BinarySearchIndexBlock]]] =
    binarySearchIndexReaderCache getOrElse {
      getBinarySearchIndex() flatMap {
        binarySearchIndex =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              binarySearchIndexReaderCache.value(binarySearchIndex, segmentReader)
          }
      }
    }.map(_.map(_.copy()))

  def getSortedIndex(): IO[SortedIndexBlock] =
    sortedIndexBlockCache getOrElse {
      getFooter() flatMap {
        footer =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              sortedIndexBlockCache.value(footer.sortedIndexOffset, segmentReader)
          }
      }
    }

  def createSortedIndexReader(): IO[DecompressedBlockReader[SortedIndexBlock]] =
    sortedIndexReaderCache getOrElse {
      getSortedIndex() flatMap {
        sortedIndex =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              sortedIndexReaderCache.value(sortedIndex, segmentReader)
          }
      }
    }.map(_.copy())

  def getValues(): IO[Option[ValuesBlock]] =
    valuesBlockCache getOrElse {
      getFooter() flatMap {
        footer =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              valuesBlockCache.value(footer.valuesOffset, segmentReader)
          }
      }
    }

  def createValuesReader(): IO[Option[DecompressedBlockReader[ValuesBlock]]] =
    valuesReaderCache getOrElse {
      getValues() flatMap {
        values =>
          createSegmentBlockReader() flatMap {
            segmentReader =>
              valuesReaderCache.value(values, segmentReader)
          }
      }
    }.map(_.map(_.copy()))

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
    footerBlockCache.isCached

  def isBloomFilterDefined =
    bloomFilterBlockCache.isCached
}
