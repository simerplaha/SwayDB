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

import com.typesafe.scalalogging.LazyLogging
import swaydb.Compression
import swaydb.compression.CompressionInternal
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.merge.MergeStats
import swaydb.core.util.MinMax
import swaydb.data.MaxKey
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline
import scala.util.Try

private[core] object SegmentBlock extends LazyLogging {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  val formatId: Byte = 1.toByte

  val crcBytes: Int = 13

  object Config {

    def default =
      new Config(
        ioStrategy = {
          case IOAction.OpenResource =>
            //cache so that files are kept in-memory
            IOStrategy.ConcurrentIO(cacheOnAccess = true)

          case IOAction.ReadDataOverview =>
            //cache so that block overview like footer and blockInfos are kept in memory.
            IOStrategy.ConcurrentIO(cacheOnAccess = true)

          case data: IOAction.DataAction =>
            //cache only if the data is compressed.
            IOStrategy.ConcurrentIO(cacheOnAccess = data.isCompressed)
        },
        compressions = _ => Seq.empty
      )

    def apply(ioStrategy: IOAction => IOStrategy,
              compressions: UncompressedBlockInfo => Iterable[Compression]): Config =
      new Config(
        ioStrategy = ioStrategy,
        compressions =
          uncompressedBlockInfo =>
            Try(compressions(uncompressedBlockInfo))
              .getOrElse(Seq.empty)
              .map(CompressionInternal.apply)
              .toSeq
      )
  }

  class Config(val ioStrategy: IOAction => IOStrategy,
               val compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  object Offset {
    def empty =
      SegmentBlock.Offset(0, 0)
  }

  case class Offset(start: Int, size: Int) extends BlockOffset

  object Closed {

    def apply(openSegment: Open): Closed =
      Closed(
        minKey = openSegment.minKey,
        maxKey = openSegment.maxKey,
        segmentBytes = openSegment.segmentBytes,
        minMaxFunctionId = openSegment.functionMinMax,
        nearestDeadline = openSegment.nearestDeadline
      )
  }

  case class Closed(minKey: Slice[Byte],
                    maxKey: MaxKey[Slice[Byte]],
                    segmentBytes: Slice[Slice[Byte]],
                    minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                    nearestDeadline: Option[Deadline]) {

    def isEmpty: Boolean =
      segmentBytes.exists(_.isEmpty)

    def segmentSize =
      segmentBytes.foldLeft(0)(_ + _.size)

    def flattenSegmentBytes: Slice[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.size)
      val slice = Slice.create[Byte](size)
      segmentBytes foreach (slice addAll _)
      assert(slice.isFull)
      slice
    }

    def flattenSegment: (Slice[Byte], Option[Deadline]) =
      (flattenSegmentBytes, nearestDeadline)

    override def toString: String =
      s"Closed Segment. Size: ${segmentSize}"
  }

  class Open(val minKey: Slice[Byte],
             val maxKey: MaxKey[Slice[Byte]],
             val valuesBlockHeader: Option[Slice[Byte]],
             val valuesBlock: Option[Slice[Byte]],
             val sortedIndexBlockHeader: Slice[Byte],
             val sortedIndexBlock: Slice[Byte],
             val hashIndexBlockHeader: Option[Slice[Byte]],
             val hashIndexBlock: Option[Slice[Byte]],
             val binarySearchIndexBlockHeader: Option[Slice[Byte]],
             val binarySearchIndexBlock: Option[Slice[Byte]],
             val bloomFilterBlockHeader: Option[Slice[Byte]],
             val bloomFilterBlock: Option[Slice[Byte]],
             val footerBlock: Slice[Byte],
             val functionMinMax: Option[MinMax[Slice[Byte]]],
             val nearestDeadline: Option[Deadline]) {

    val segmentHeader: Slice[Byte] = Slice.create[Byte](Byte.MaxValue)

    val segmentBytes: Slice[Slice[Byte]] = {
      val allBytes = Slice.create[Slice[Byte]](13)
      allBytes add segmentHeader

      valuesBlockHeader foreach (allBytes add _)
      valuesBlock foreach (allBytes add _)

      allBytes add sortedIndexBlockHeader
      allBytes add sortedIndexBlock

      hashIndexBlockHeader foreach (allBytes add _)
      hashIndexBlock foreach (allBytes add _)

      binarySearchIndexBlockHeader foreach (allBytes add _)
      binarySearchIndexBlock foreach (allBytes add _)

      bloomFilterBlockHeader foreach (allBytes add _)
      bloomFilterBlock foreach (allBytes add _)

      allBytes add footerBlock
    }

    def isEmpty: Boolean =
      segmentBytes.exists(_.isEmpty)

    def segmentSize =
      segmentBytes.foldLeft(0)(_ + _.size)

    def flattenSegmentBytes: Slice[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.size)
      val slice = Slice.create[Byte](size)
      segmentBytes foreach (slice addAll _)
      assert(slice.isFull)
      slice
    }
  }

  private[block] case class ClosedBlocks(sortedIndex: SortedIndexBlock.State,
                                         values: Option[ValuesBlock.State],
                                         hashIndex: Option[HashIndexBlock.State],
                                         binarySearchIndex: Option[BinarySearchIndexBlock.State],
                                         bloomFilter: Option[BloomFilterBlock.State],
                                         minMaxFunction: Option[MinMax[Slice[Byte]]]) {
    def nearestDeadline: Option[Deadline] = sortedIndex.nearestDeadline
  }

  def read(header: Block.Header[Offset]): SegmentBlock =
    SegmentBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )

  def writeClosed(keyValues: MergeStats.Persistent[_, Iterable],
                  createdInLevel: Int,
                  segmentSize: Int,
                  bloomFilterConfig: BloomFilterBlock.Config,
                  hashIndexConfig: HashIndexBlock.Config,
                  binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                  sortedIndexConfig: SortedIndexBlock.Config,
                  valuesConfig: ValuesBlock.Config,
                  segmentConfig: SegmentBlock.Config): Iterable[SegmentBlock.Closed] =
    if (keyValues.isEmpty)
      Seq.empty
    else
      writeOpen(
        keyValues = keyValues,
        createdInLevel = createdInLevel,
        minSegmentSize = segmentSize,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      ) map {
        openSegment =>
          Block.block(
            openSegment = openSegment,
            compressions = segmentConfig.compressions(UncompressedBlockInfo(openSegment.segmentSize)),
            blockName = blockName
          )
      }

  def writeOpen(keyValues: MergeStats.Persistent[_, Iterable],
                createdInLevel: Int,
                minSegmentSize: Int,
                bloomFilterConfig: BloomFilterBlock.Config,
                hashIndexConfig: HashIndexBlock.Config,
                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                sortedIndexConfig: SortedIndexBlock.Config,
                valuesConfig: ValuesBlock.Config,
                segmentConfig: SegmentBlock.Config): Iterable[SegmentBlock.Open] =
    if (keyValues.isEmpty) {
      Seq.empty
    } else {
      //IMPORTANT! - The following is critical for compaction performance!

      val keyValuesCount = keyValues.size
      val segments = ListBuffer.empty[SegmentBlock.Open]

      //start sortedIndex for a new Segment.
      var sortedIndex =
        SortedIndexBlock.init(
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig
        )

      //start valuesBlock for a new Segment.
      var values =
        ValuesBlock.init(
          keyValues = keyValues,
          valuesConfig = valuesConfig,
          builder = sortedIndex.builder
        )

      //keys to write to bloomFilter.
      val bloomFilterKeys = ListBuffer.empty[Slice[Byte]]

      var processedCount = 0 //numbers of key-values written
      var closed = false //true if the following iterator exited after closing the Segment.

      //start building the segment.
      keyValues.keyValues foreach {
        keyValue =>
          closed = false
          processedCount += 1
          bloomFilterKeys += keyValue.key

          SortedIndexBlock.write(keyValue = keyValue, state = sortedIndex)
          values foreach (ValuesBlock.write(keyValue, _))

          var currentSegmentSize = sortedIndex.bytes.size + SegmentFooterBlock.optimalBytesRequired
          values foreach (currentSegmentSize += _.bytes.size)

          //check and close segment if segment size limit is reached.
          //to do - maybe check if compression is defined and increase the segmentSize.
          if (currentSegmentSize >= minSegmentSize) {

            logger.debug(s"Creating segment of size: $currentSegmentSize.bytes")

            val (closedSegment, nextSortedIndex, nextValues) =
              writeOpenSegment(
                createdInLevel = createdInLevel,
                hasMoreKeyValues = processedCount < keyValuesCount,
                bloomFilterKeys = bloomFilterKeys,
                sortedIndex = sortedIndex,
                values = values,
                bloomFilterConfig = bloomFilterConfig,
                hashIndexConfig = hashIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                sortedIndexConfig = sortedIndexConfig,
                valuesConfig = valuesConfig
              )

            segments += closedSegment

            //segment's closed. Prepare for next Segment.
            bloomFilterKeys.clear() //clear bloomFilter keys.

            nextSortedIndex foreach { //set the newSortedIndex if it was created.
              newSortedIndex =>
                sortedIndex = newSortedIndex
            }

            values = nextValues
            closed = true
          }
      }

      //if the segment was closed and all key-values were written then close Segment.
      if (closed) {
        segments
      } else {
        val (closedSegment, nextSortedIndex, nextValuesBlock) =
          writeOpenSegment(
            createdInLevel = createdInLevel,
            hasMoreKeyValues = false,
            bloomFilterKeys = bloomFilterKeys,
            sortedIndex = sortedIndex,
            values = values,
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig
          )

        //temporary check.
        assert(nextSortedIndex.isEmpty && nextValuesBlock.isEmpty, s"${nextSortedIndex.isEmpty} && ${nextValuesBlock.isEmpty} is not empty.")

        segments += closedSegment
      }
    }

  private def writeOpenSegment(createdInLevel: Int,
                               hasMoreKeyValues: Boolean,
                               bloomFilterKeys: ListBuffer[Slice[Byte]],
                               sortedIndex: SortedIndexBlock.State,
                               values: Option[ValuesBlock.State],
                               bloomFilterConfig: BloomFilterBlock.Config,
                               hashIndexConfig: HashIndexBlock.Config,
                               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                               sortedIndexConfig: SortedIndexBlock.Config,
                               valuesConfig: ValuesBlock.Config): (SegmentBlock.Open, Option[SortedIndexBlock.State], Option[ValuesBlock.State]) = {
    //tail bytes before closing and compression is applied.
    val unwrittenTailSortedIndexBytes = sortedIndex.bytes.unwrittenTail()
    val unwrittenTailValueBytes = values.map(_.bytes.unwrittenTail())

    val closedBlocks =
      closeBlocks(
        sortedIndex = sortedIndex,
        values = values,
        bloomFilterKeys = bloomFilterKeys,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig
      )

    val footer =
      SegmentFooterBlock.init(
        keyValuesCount = closedBlocks.sortedIndex.entriesCount,
        rangesCount = closedBlocks.sortedIndex.rangeCount,
        hasPut = closedBlocks.sortedIndex.hasPut,
        createdInLevel = createdInLevel
      )

    val closedFooter =
      SegmentFooterBlock.writeAndClose(
        state = footer,
        closedBlocks = closedBlocks
      )

    val open =
      new Open(
        minKey = closedBlocks.sortedIndex.minKey,
        maxKey = closedBlocks.sortedIndex.maxKey,
        footerBlock = closedFooter.bytes.close(),
        valuesBlockHeader = closedBlocks.values.map(_.header.close()),
        valuesBlock = closedBlocks.values.map(_.bytes.close()),
        sortedIndexBlockHeader = closedBlocks.sortedIndex.header.close(),
        sortedIndexBlock = closedBlocks.sortedIndex.bytes.close(),
        hashIndexBlockHeader = closedBlocks.hashIndex map (_.header.close()),
        hashIndexBlock = closedBlocks.hashIndex map (_.bytes.close()),
        binarySearchIndexBlockHeader = closedBlocks.binarySearchIndex map (_.header.close()),
        binarySearchIndexBlock = closedBlocks.binarySearchIndex map (_.bytes.close()),
        bloomFilterBlockHeader = closedBlocks.bloomFilter map (_.header.close()),
        bloomFilterBlock = closedBlocks.bloomFilter map (_.bytes.close()),
        functionMinMax = closedBlocks.minMaxFunction,
        nearestDeadline = closedBlocks.nearestDeadline
      )

    //start new sortedIndex block only if there are more key-values to process
    val newSortedIndex =
      if (hasMoreKeyValues)
        Some(
          SortedIndexBlock.init(
            bytes = unwrittenTailSortedIndexBytes,
            compressDuplicateValues = valuesConfig.compressDuplicateValues,
            compressDuplicateRangeValues = valuesConfig.compressDuplicateRangeValues,
            sortedIndexConfig = sortedIndexConfig
          )
        )
      else
        None

    //start new values block only if there are more key-values to process
    val newValues =
      if (hasMoreKeyValues)
        newSortedIndex flatMap {
          newSortedIndex =>
            unwrittenTailValueBytes map {
              tailValueBytes =>
                ValuesBlock.init(
                  bytes = tailValueBytes,
                  valuesConfig = valuesConfig,
                  builder = newSortedIndex.builder
                )
            }
        }
      else
        None

    (open, newSortedIndex, newValues)
  }

  private def closeBlocks(sortedIndex: SortedIndexBlock.State,
                          values: Option[ValuesBlock.State],
                          bloomFilterKeys: ListBuffer[Slice[Byte]],
                          bloomFilterConfig: BloomFilterBlock.Config,
                          hashIndexConfig: HashIndexBlock.Config,
                          binarySearchIndexConfig: BinarySearchIndexBlock.Config): ClosedBlocks = {
    val sortedIndexState = SortedIndexBlock.close(sortedIndex)
    val valuesState = values map ValuesBlock.close

    val bloomFilter =
      if (sortedIndexState.hasRemoveRange || bloomFilterKeys.size < bloomFilterConfig.minimumNumberOfKeys)
        None
      else
        BloomFilterBlock.init(
          numberOfKeys = bloomFilterKeys.size,
          falsePositiveRate = bloomFilterConfig.falsePositiveRate,
          updateMaxProbe = bloomFilterConfig.optimalMaxProbe,
          compressions = bloomFilterConfig.compressions
        )

    val hashIndex =
      HashIndexBlock.init(
        sortedIndexState = sortedIndexState,
        hashIndexConfig = hashIndexConfig
      )

    val binarySearchIndex =
      BinarySearchIndexBlock.init(
        sortedIndexState = sortedIndexState,
        binarySearchConfig = binarySearchIndexConfig
      )

    if (hashIndex.isDefined || binarySearchIndex.isDefined)
      sortedIndexState.secondaryIndexEntries foreach {
        indexEntry =>
          val hit =
            if (hashIndex.isDefined)
              HashIndexBlock.write(
                entry = indexEntry,
                state = hashIndex.get
              )
            else
              false

          //if it's a hit and binary search is not configured to be full.
          //no need to check if the value was previously written to binary search here since BinarySearchIndexBlock itself performs this check.
          if (binarySearchIndex.isDefined && (binarySearchIndexConfig.fullIndex || !hit))
            BinarySearchIndexBlock.write(
              entry = indexEntry,
              state = binarySearchIndex.get
            )
      }

    bloomFilter foreach {
      bloomFilter =>
        bloomFilterKeys foreach {
          key =>
            BloomFilterBlock.add(key, bloomFilter)
        }
    }

    ClosedBlocks(
      sortedIndex = sortedIndexState,
      values = valuesState,
      hashIndex = hashIndex.flatMap(HashIndexBlock.close),
      binarySearchIndex = binarySearchIndex.flatMap(BinarySearchIndexBlock.close),
      bloomFilter = bloomFilter.flatMap(BloomFilterBlock.close),
      minMaxFunction = sortedIndexState.minMaxFunctionId
    )
  }

  implicit object SegmentBlockOps extends BlockOps[SegmentBlock.Offset, SegmentBlock] {
    override def updateBlockOffset(block: SegmentBlock, start: Int, size: Int): SegmentBlock =
      block.copy(offset = SegmentBlock.Offset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      SegmentBlock.Offset(start, size)

    override def readBlock(header: Block.Header[SegmentBlock.Offset]): SegmentBlock =
      SegmentBlock.read(header)
  }

}

private[core] case class SegmentBlock(offset: SegmentBlock.Offset,
                                      headerSize: Int,
                                      compressionInfo: Option[Block.CompressionInfo]) extends Block[SegmentBlock.Offset]


