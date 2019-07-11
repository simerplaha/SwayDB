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

package swaydb.core.segment

import swaydb.core.io.reader.BlockReader
import swaydb.core.segment.format.a.block._
import swaydb.core.util.cache.Cache
import swaydb.data.IO

object SegmentBlockCache {
  def apply(id: String,
            segmentBlockReader: () => IO[BlockReader[SegmentBlock]]): SegmentBlockCache =
    new SegmentBlockCache(
      id = id,
      segmentBlockReader = segmentBlockReader
    )
}

class SegmentBlockCache(id: String,
                        segmentBlockReader: () => IO[BlockReader[SegmentBlock]]) {

  private val footerCache = Cache.io[SegmentBlock.Footer](synchronised = true, stored = true)(getFooterInfo())
  private val hashIndexCache = Cache.io[Option[HashIndex]](synchronised = true, stored = true)(getHashIndexInfo())
  private val bloomFilterCache = Cache.io[Option[BloomFilter]](synchronised = true, stored = true)(getBloomFilterInfo())
  private val binarySearchIndexCache = Cache.io[Option[BinarySearchIndex]](synchronised = true, stored = true)(getBinarySearchIndexInfo())
  private val sortedIndexCache = Cache.io[SortedIndex](synchronised = true, stored = true)(getSortedIndexInfo())
  private val valuesCache = Cache.io[Option[Values]](synchronised = true, stored = true)(getValuesInfo())

  def clear(): Unit = {
    footerCache.clear()
    hashIndexCache.clear()
    bloomFilterCache.clear()
    binarySearchIndexCache.clear()
    sortedIndexCache.clear()
    valuesCache.clear()
  }

  def isOpen: Boolean =
    footerCache.isCached ||
      hashIndexCache.isCached ||
      bloomFilterCache.isCached ||
      binarySearchIndexCache.isCached ||
      sortedIndexCache.isCached ||
      valuesCache.isCached

  def footer: IO[SegmentBlock.Footer] =
    footerCache.value

  def isFooterDefined =
    footerCache.isCached

  def isBloomFilterDefined =
    bloomFilterCache.isCached

  def createSortedIndexReader(): IO[BlockReader[SortedIndex]] =
    sortedIndexCache.value flatMap createBlockReader

  def createHashIndexReader(): IO[Option[BlockReader[HashIndex]]] =
    hashIndexCache.value flatMap createOptionalBlockReader

  def createBloomFilterReader(): IO[Option[BlockReader[BloomFilter]]] =
    bloomFilterCache.value flatMap createOptionalBlockReader

  def createBinarySearchReader(): IO[Option[BlockReader[BinarySearchIndex]]] =
    binarySearchIndexCache.value flatMap createOptionalBlockReader

  def createValuesReader(): IO[Option[BlockReader[Values]]] =
    valuesCache.value flatMap createOptionalBlockReader

  private def createOptionalBlockReader[B <: Block](block: Option[B]): IO[Option[BlockReader[B]]] =
    block map {
      block =>
        createBlockReader(block).map(Some(_))
    } getOrElse IO.none

  private def createBlockReader[B <: Block](block: B): IO[BlockReader[B]] =
    segmentBlockReader() map {
      reader =>
        block.createBlockReader(reader).asInstanceOf[BlockReader[B]]
    }

  private def getFooterAndSegmentReader(): IO[(SegmentBlock.Footer, BlockReader[SegmentBlock])] =
    for {
      footer <- footerCache.value
      reader <- segmentBlockReader()
    } yield {
      (footer, reader)
    }

  /**
    * INFOS
    */
  private def getFooterInfo(): IO[SegmentBlock.Footer] =
    segmentBlockReader() flatMap SegmentBlock.readFooter

  private def getHashIndexInfo(): IO[Option[HashIndex]] =
    getFooterAndSegmentReader() flatMap {
      case (footer, reader) =>
        footer.hashIndexOffset map {
          offset =>
            HashIndex.read(
              offset = offset,
              reader = reader
            ).map(Some(_))
        } getOrElse {
          IO.none
        }
    }

  private def getBloomFilterInfo(): IO[Option[BloomFilter]] =
    getFooterAndSegmentReader() flatMap {
      case (footer, reader) =>
        footer.bloomFilterOffset map {
          offset =>
            BloomFilter.read(
              offset = offset,
              segmentReader = reader
            ).map(Some(_))
        } getOrElse {
          IO.none
        }
    }

  private def getBinarySearchIndexInfo(): IO[Option[BinarySearchIndex]] =
    getFooterAndSegmentReader() flatMap {
      case (footer, reader) =>
        footer.binarySearchIndexOffset map {
          offset =>
            BinarySearchIndex
              .read(
                offset = offset,
                reader = reader
              ).map(Some(_))
        } getOrElse {
          IO.none
        }
    }

  private def getSortedIndexInfo(): IO[SortedIndex] =
    getFooterAndSegmentReader() flatMap {
      case (footer, reader) =>
        SortedIndex.read(
          offset = footer.sortedIndexOffset,
          segmentReader = reader
        )
    }

  private def getValuesInfo(): IO[Option[Values]] =
    getFooterAndSegmentReader() flatMap {
      case (footer, reader) =>
        footer.valuesOffset map {
          valuesOffset =>
            Values.read(
              offset = valuesOffset,
              segmentReader = reader
            ).map(Some(_))
        } getOrElse {
          IO.none
        }
    }
}
