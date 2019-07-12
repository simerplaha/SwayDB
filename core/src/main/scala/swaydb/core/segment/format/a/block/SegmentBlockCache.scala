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

  def createBlockReaderCache[B <: Block](segmentBlockReader: => IO[BlockReader[SegmentBlock]]) =
    Cache.delayedIO[B, BlockReader[B]](synchronised = _.compressionInfo.isDefined, reserved = _ => false, stored = _.compressionInfo.isDefined) {
      block =>
        segmentBlockReader flatMap {
          segmentBlockReader =>
            Block.createDecompressedBlockReader(
              block = block,
              segmentReader = segmentBlockReader
            )
        }
    }
}

protected class SegmentBlockInfo(val segmentBlockOffset: SegmentBlock.Offset,
                                 val rawSegmentReader: () => Reader)
class SegmentBlockCache(id: String,
                        segmentBlockInfo: SegmentBlockInfo) {

  private val segmentBlockCache: Cache[SegmentBlockInfo, SegmentBlock] =
    Cache.io[SegmentBlockInfo, SegmentBlock](synchronised = true, reserved = false, stored = false) {
      blockInfo =>
        SegmentBlock.read(
          offset = blockInfo.segmentBlockOffset,
          segmentReader = blockInfo.rawSegmentReader()
        )
    }

  private val footer: Cache[BlockReader[SegmentBlock], SegmentBlock.Footer] =
    Cache.io[BlockReader[SegmentBlock], SegmentBlock.Footer](synchronised = true, reserved = false, stored = true)(
      fetch = SegmentBlock.readFooter
    )

  private val hashIndex: Cache[BlockReader[SegmentBlock], Option[HashIndex]] =
    Cache.io[BlockReader[SegmentBlock], Option[HashIndex]](synchronised = true, reserved = false, stored = true) {
      blockReader =>
        footer.value(blockReader) flatMap {
          footer =>
            footer.hashIndexOffset map {
              hashIndexOffset =>
                HashIndex
                  .read(hashIndexOffset, blockReader)
                  .map(Some(_))
            } getOrElse IO.none
        }
    }

  private val bloomFilter: Cache[BlockReader[SegmentBlock], Option[BloomFilter]] =
    Cache.io[BlockReader[SegmentBlock], Option[BloomFilter]](synchronised = true, reserved = false, stored = true) {
      blockReader =>
        footer.value(blockReader) flatMap {
          footer =>
            footer.bloomFilterOffset map {
              bloomFilterOffset =>
                BloomFilter
                  .read(bloomFilterOffset, blockReader)
                  .map(Some(_))
            } getOrElse IO.none
        }
    }

  private val binarySearchIndex: Cache[BlockReader[SegmentBlock], Option[BinarySearchIndex]] =
    Cache.io[BlockReader[SegmentBlock], Option[BinarySearchIndex]](synchronised = true, reserved = false, stored = true) {
      blockReader =>
        footer.value(blockReader) flatMap {
          footer =>
            footer.binarySearchIndexOffset map {
              binarySearchIndexOffset =>
                BinarySearchIndex
                  .read(binarySearchIndexOffset, blockReader)
                  .map(Some(_))
            } getOrElse IO.none
        }
    }

  private val sortedIndex: Cache[BlockReader[SegmentBlock], SortedIndex] =
    Cache.io[BlockReader[SegmentBlock], SortedIndex](synchronised = true, reserved = false, stored = true) {
      blockReader =>
        footer.value(blockReader) flatMap {
          footer =>
            SortedIndex
              .read(
                offset = footer.sortedIndexOffset,
                segmentReader = blockReader
              )
        }
    }

  private val values: Cache[BlockReader[SegmentBlock], Option[Values]] =
    Cache.io[BlockReader[SegmentBlock], Option[Values]](synchronised = true, reserved = false, stored = true) {
      blockReader =>
        footer.value(blockReader) flatMap {
          footer =>
            footer.valuesOffset map {
              valuesOffset =>
                Values
                  .read(valuesOffset, blockReader)
                  .map(Some(_))
            } getOrElse IO.none
        }
    }

  private val segmentBlockReader: Cache[SegmentBlock, BlockReader[SegmentBlock]] =
    SegmentBlockCache.createBlockReaderCache[SegmentBlock](
      segmentBlockReader = SegmentBlock.createUnblockedReader(segmentBlockInfo.rawSegmentReader())
    )

  private val hashIndexReader: Cache[HashIndex, BlockReader[HashIndex]] =
    SegmentBlockCache.createBlockReaderCache[HashIndex](
      segmentBlockReader = createSegmentBlockReader()
    )

  private val bloomFilterReader: Cache[BloomFilter, BlockReader[BloomFilter]] =
    SegmentBlockCache.createBlockReaderCache[BloomFilter](
      segmentBlockReader = createSegmentBlockReader()
    )

  private val binarySearchIndexReader: Cache[BinarySearchIndex, BlockReader[BinarySearchIndex]] =
    SegmentBlockCache.createBlockReaderCache[BinarySearchIndex](
      segmentBlockReader = createSegmentBlockReader()
    )

  private val sortedIndexReader: Cache[SortedIndex, BlockReader[SortedIndex]] =
    SegmentBlockCache.createBlockReaderCache[SortedIndex](
      segmentBlockReader = createSegmentBlockReader()
    )

  private val valuesReader: Cache[Values, BlockReader[Values]] =
    SegmentBlockCache.createBlockReaderCache[Values](
      segmentBlockReader = createSegmentBlockReader()
    )

  private val allCaches =
    Seq(
      segmentBlockCache, footer, hashIndex, bloomFilter, binarySearchIndex, sortedIndex, values,
      segmentBlockReader, hashIndexReader, bloomFilterReader, binarySearchIndexReader, sortedIndexReader, valuesReader
    )

  private def getSegmentBlock(): IO[SegmentBlock] =
    segmentBlockCache.value(segmentBlockInfo)

  private def createSegmentBlockReader(): IO[BlockReader[SegmentBlock]] =
    getSegmentBlock() flatMap {
      segmentBlock =>
        segmentBlockReader.value(segmentBlock)
    } map (_.copy())

  def getFooter(): IO[SegmentBlock.Footer] =
    createSegmentBlockReader() flatMap {
      segmentBlock =>
        footer.value(segmentBlock)
    }

  def getHashIndex(): IO[Option[HashIndex]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        hashIndex.value(segmentBlockReader)
    }

  def createHashIndexReader(): IO[Option[BlockReader[HashIndex]]] =
    getHashIndex()
      .flatMap {
        block =>
          block map {
            hashIndex =>
              hashIndexReader
                .value(hashIndex)
                .map(Some(_))
          } getOrElse IO.none
      }

  def getBloomFilter(): IO[Option[BloomFilter]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        bloomFilter.value(segmentBlockReader)
    }

  def createBloomFilterReader(): IO[Option[BlockReader[BloomFilter]]] =
    getBloomFilter()
      .flatMap {
        block =>
          block map {
            block =>
              bloomFilterReader
                .value(block)
                .map(Some(_))
          } getOrElse IO.none
      }

  def getBinarySearchIndex(): IO[Option[BinarySearchIndex]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        binarySearchIndex.value(segmentBlockReader)
    }

  def createBinarySearchIndexReader(): IO[Option[BlockReader[BinarySearchIndex]]] =
    getBinarySearchIndex()
      .flatMap {
        block =>
          block map {
            block =>
              binarySearchIndexReader
                .value(block)
                .map(Some(_))
          } getOrElse IO.none
      }

  def getSortedIndex(): IO[SortedIndex] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        sortedIndex.value(segmentBlockReader)
    }

  def createSortedIndexReader(): IO[BlockReader[SortedIndex]] =
    getSortedIndex()
      .flatMap {
        block =>
          sortedIndexReader.value(block)
      }

  def getValues(): IO[Option[Values]] =
    createSegmentBlockReader() flatMap {
      segmentBlockReader =>
        values.value(segmentBlockReader)
    }

  def createValuesReader(): IO[Option[BlockReader[Values]]] =
    getValues()
      .flatMap {
        block =>
          block map {
            block =>
              valuesReader
                .value(block)
                .map(Some(_))
          } getOrElse IO.none
      }

  def clear(): Unit =
    allCaches.foreach(_.clear())

  def isOpen: Boolean =
    allCaches.exists(_.isCached)

  def isFooterDefined =
    footer.isCached

  def isBloomFilterDefined =
    bloomFilter.isCached
}
