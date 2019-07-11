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
}

protected class SegmentBlockInfo(val segmentBlockOffset: SegmentBlock.Offset,
                                 val rawSegmentReader: () => Reader)

class SegmentBlockCache(id: String,
                        segmentBlockInfo: SegmentBlockInfo) {

  val segmentBlockCache =
    Cache.io[SegmentBlockInfo, SegmentBlock](synchronised = true, reserved = false, stored = true) {
      blockInfo =>
        SegmentBlock.read(
          offset = blockInfo.segmentBlockOffset,
          segmentReader = blockInfo.rawSegmentReader()
        )
    }

  val segmentBlockByteCache =
    Cache.io[SegmentBlock, BlockReader[SegmentBlock]](synchronised = true, reserved = false, stored = true) {
      blockInfo =>
        IO(blockInfo.createBlockReader(segmentBlockInfo.rawSegmentReader()))
    }

  val segmentBlockReader =
    segmentBlockCache.map(segmentBlockInfo)(_.createBlockReader(segmentBlockInfo.rawSegmentReader()))

  def getSegmentBlockReader(): IO[BlockReader[SegmentBlock]] =
    segmentBlockReader

  val segmentBlockReaderCache =
    Cache.io[SegmentBlock, BlockReader[SegmentBlock]](synchronised = true, reserved = false, stored = false) {
      segmentBlock =>
        //        IO(segmentBlock createBlockReader rawSegmentBlockReader())
        ???
    }

  val footerCache =
    Cache.io[BlockReader[SegmentBlock], SegmentBlock.Footer](synchronised = true, reserved = false, stored = true)(SegmentBlock.readFooter)

  val hashIndexBlockCache =
    Cache.io[BlockReader[SegmentBlock], Option[HashIndex]](synchronised = true, reserved = false, stored = true) {
      blockReader =>
        footerCache.value(blockReader) flatMap {
          footer =>
            footer.hashIndexOffset map {
              hashIndexOffset =>
                HashIndex
                  .read(hashIndexOffset, blockReader)
                  .map(Some(_))
            } getOrElse IO.none
        }
    }

  val bloomFilterBlockCache = Cache.io[BlockReader[SegmentBlock], Option[BloomFilter]](synchronised = true, reserved = false, stored = true) {
    blockReader =>
      footerCache.value(blockReader) flatMap {
        footer =>
          footer.bloomFilterOffset map {
            bloomFilterOffset =>
              BloomFilter
                .read(bloomFilterOffset, blockReader)
                .map(Some(_))
          } getOrElse IO.none
      }
  }

  val binarySearchIndexBlockCache = Cache.io[BlockReader[SegmentBlock], Option[BinarySearchIndex]](synchronised = true, reserved = false, stored = true) {
    blockReader =>
      footerCache.value(blockReader) flatMap {
        footer =>
          footer.binarySearchIndexOffset map {
            binarySearchIndexOffset =>
              BinarySearchIndex
                .read(binarySearchIndexOffset, blockReader)
                .map(Some(_))
          } getOrElse IO.none
      }
  }

  val sortedIndexBlockCache = Cache.io[BlockReader[SegmentBlock], SortedIndex](synchronised = true, reserved = false, stored = true) {
    blockReader =>
      footerCache.value(blockReader) flatMap {
        footer =>
          SortedIndex
            .read(
              offset = footer.sortedIndexOffset,
              segmentReader = blockReader
            )
      }
  }

  val valuesBlockCache = Cache.io[BlockReader[SegmentBlock], Option[Values]](synchronised = true, reserved = false, stored = true) {
    blockReader =>
      footerCache.value(blockReader) flatMap {
        footer =>
          footer.valuesOffset map {
            valuesOffset =>
              Values
                .read(valuesOffset, blockReader)
                .map(Some(_))
          } getOrElse IO.none
      }
  }

  def clear(): Unit = {
    segmentBlockCache.clear()
    valuesBlockCache.clear()
    sortedIndexBlockCache.clear()
    binarySearchIndexBlockCache.clear()
    hashIndexBlockCache.clear()
    bloomFilterBlockCache.clear()
    footerCache.clear()
  }

  def isOpen: Boolean =
    segmentBlockCache.isCached ||
      footerCache.isCached ||
      hashIndexBlockCache.isCached ||
      bloomFilterBlockCache.isCached ||
      binarySearchIndexBlockCache.isCached ||
      sortedIndexBlockCache.isCached ||
      valuesBlockCache.isCached

  def isFooterDefined =
    footerCache.isCached

  def isBloomFilterDefined =
    bloomFilterBlockCache.isCached

  //  def createSortedIndexReader(): IO[BlockReader[SortedIndex]] =
  //    sortedIndexCache.value flatMap createBlockReader
  //
  //  def createHashIndexReader(): IO[Option[BlockReader[HashIndex]]] =
  //    hashIndexCache.value flatMap createOptionalBlockReader
  //
  //  def createBloomFilterReader(): IO[Option[BlockReader[BloomFilter]]] =
  //    bloomFilterCache.value flatMap createOptionalBlockReader
  //
  //  def createBinarySearchReader(): IO[Option[BlockReader[BinarySearchIndex]]] =
  //    binarySearchIndexCache.value flatMap createOptionalBlockReader
  //
  //  def createValuesReader(): IO[Option[BlockReader[Values]]] =
  //    valuesCache.value flatMap createOptionalBlockReader
  //
  //  private def createOptionalBlockReader[B <: Block](block: Option[B]): IO[Option[BlockReader[B]]] =
  //    block map {
  //      block =>
  //        createBlockReader(block).map(Some(_))
  //    } getOrElse IO.none
  //
  //  private def createBlockReader[B <: Block](block: B): IO[BlockReader[B]] =
  //    segmentBlockReader() map {
  //      reader =>
  //        block.createBlockReader(reader).asInstanceOf[BlockReader[B]]
  //    }
  //
  //  private def getFooterAndSegmentBlockReader(): IO[(SegmentBlock.Footer, BlockReader[SegmentBlock])] =
  //    for {
  //      footer <- footerCache.value
  //      reader <- segmentBlockReader()
  //    } yield {
  //      (footer, reader)
  //    }
  //
  //  /**
  //    * INFOS
  //    */
  //  private def getFooterInfo(segmentBlock: BlockReader[SegmentBlock]): IO[SegmentBlock.Footer] =
  //    segmentBlockReader() flatMap SegmentBlock.readFooter
  //
  //  private def getHashIndexInfo(): IO[Option[HashIndex]] =
  //    getFooterAndSegmentBlockReader() flatMap {
  //      case (footer, segmentReader) =>
  //        footer.hashIndexOffset map {
  //          offset =>
  //            HashIndex.read(
  //              offset = offset,
  //              reader = segmentReader
  //            ).map(Some(_))
  //        } getOrElse {
  //          IO.none
  //        }
  //    }
  //
  //  private def getBloomFilterInfo(): IO[Option[BloomFilter]] =
  //    getFooterAndSegmentBlockReader() flatMap {
  //      case (footer, segmentReader) =>
  //        footer.bloomFilterOffset map {
  //          offset =>
  //            BloomFilter.read(
  //              offset = offset,
  //              segmentReader = segmentReader
  //            ).map(Some(_))
  //        } getOrElse {
  //          IO.none
  //        }
  //    }
  //
  //  private def getBinarySearchIndexInfo(): IO[Option[BinarySearchIndex]] =
  //    getFooterAndSegmentBlockReader() flatMap {
  //      case (footer, segmentReader) =>
  //        footer.binarySearchIndexOffset map {
  //          offset =>
  //            BinarySearchIndex
  //              .read(
  //                offset = offset,
  //                reader = segmentReader
  //              ).map(Some(_))
  //        } getOrElse {
  //          IO.none
  //        }
  //    }
  //
  //  private def getSortedIndexInfo(): IO[SortedIndex] =
  //    getFooterAndSegmentBlockReader() flatMap {
  //      case (footer, segmentReader) =>
  //        SortedIndex.read(
  //          offset = footer.sortedIndexOffset,
  //          segmentReader = segmentReader
  //        )
  //    }
  //
  //  private def getValuesInfo(): IO[Option[Values]] =
  //    getFooterAndSegmentBlockReader() flatMap {
  //      case (footer, segmentReader) =>
  //        footer.valuesOffset map {
  //          valuesOffset =>
  //            Values.read(
  //              offset = valuesOffset,
  //              segmentReader = segmentReader
  //            ).map(Some(_))
  //        } getOrElse {
  //          IO.none
  //        }
  //    }
}
