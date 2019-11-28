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

import swaydb.Error.Segment.ExceptionHandler
import swaydb.compression.CompressionInternal
import swaydb.core.data.{Memory, Transient}
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.{DeadlineAndFunctionId, Segment}
import swaydb.core.util.{Bytes, MinMax, SkipList}
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.slice.Slice
import swaydb.{Compression, IO}

import scala.annotation.tailrec
import scala.concurrent.duration.Deadline
import scala.util.Try

private[core] object SegmentBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  val formatId: Byte = 1.toByte

  val crcBytes: Int = 13

  object Config {

    def default =
      new Config(
        blockIO = _ => IOStrategy.ConcurrentIO(false),
        compressions = _ => Seq.empty
      )

    def apply(segmentIO: IOAction => IOStrategy,
              compressions: UncompressedBlockInfo => Iterable[Compression]): Config =
      new Config(
        blockIO = segmentIO,
        compressions =
          uncompressedBlockInfo =>
            Try(compressions(uncompressedBlockInfo))
              .getOrElse(Seq.empty)
              .map(CompressionInternal.apply)
              .toSeq
      )
  }

  class Config(val blockIO: IOAction => IOStrategy,
               val compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  object Offset {
    def empty =
      SegmentBlock.Offset(0, 0)
  }

  case class Offset(start: Int, size: Int) extends BlockOffset

  object Closed {

    def empty =
      apply(Open.empty)

    def emptyIO =
      IO(empty)

    def apply(openSegment: Open): Closed =
      Closed(
        segmentBytes = openSegment.segmentBytes,
        minMaxFunctionId = openSegment.functionMinMax,
        nearestDeadline = openSegment.nearestDeadline
      )
  }

  case class Closed(segmentBytes: Slice[Slice[Byte]],
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

  object Open {
    def empty =
      new Open(
        headerBytes = Slice.emptyBytes,
        valuesBlock = None,
        sortedIndexBlock = Slice.emptyBytes,
        hashIndexBlock = None,
        binarySearchIndexBlock = None,
        bloomFilterBlock = None,
        footerBlock = Slice.emptyBytes,
        functionMinMax = None,
        nearestDeadline = None
      )

    def emptyIO = IO.Right(empty)

    def apply(headerBytes: Slice[Byte],
              valuesBlock: Option[Slice[Byte]],
              sortedIndexBlock: Slice[Byte],
              hashIndexBlock: Option[Slice[Byte]],
              binarySearchIndexBlock: Option[Slice[Byte]],
              bloomFilterBlock: Option[Slice[Byte]],
              footerBlock: Slice[Byte],
              functionMinMax: Option[MinMax[Slice[Byte]]],
              nearestDeadline: Option[Deadline]): Open =
      new Open(
        headerBytes = headerBytes,
        valuesBlock = valuesBlock,
        sortedIndexBlock = sortedIndexBlock,
        hashIndexBlock = hashIndexBlock,
        binarySearchIndexBlock = binarySearchIndexBlock,
        bloomFilterBlock = bloomFilterBlock,
        footerBlock = footerBlock,
        functionMinMax = functionMinMax,
        nearestDeadline = nearestDeadline
      )
  }

  class Open(val headerBytes: Slice[Byte],
             val valuesBlock: Option[Slice[Byte]],
             val sortedIndexBlock: Slice[Byte],
             val hashIndexBlock: Option[Slice[Byte]],
             val binarySearchIndexBlock: Option[Slice[Byte]],
             val bloomFilterBlock: Option[Slice[Byte]],
             val footerBlock: Slice[Byte],
             val functionMinMax: Option[MinMax[Slice[Byte]]],
             val nearestDeadline: Option[Deadline]) {

    headerBytes moveWritePosition headerBytes.allocatedSize

    val segmentBytes: Slice[Slice[Byte]] = {
      val allBytes = Slice.create[Slice[Byte]](8)
      allBytes add headerBytes.close()
      valuesBlock foreach (allBytes add _)
      allBytes add sortedIndexBlock
      hashIndexBlock foreach (allBytes add _)
      binarySearchIndexBlock foreach (allBytes add _)
      bloomFilterBlock foreach (allBytes add _)
      allBytes add footerBlock
      allBytes.filter(_.nonEmpty).close()
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
                                         minMaxFunction: Option[MinMax[Slice[Byte]]],
                                         nearestDeadline: Option[Deadline])

  def read(header: Block.Header[Offset]): SegmentBlock =
    SegmentBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )

  val noCompressionHeaderSize = {
    val size = Block.headerSize(false)
    Bytes.sizeOfUnsignedInt(size) + size
  }

  val hasCompressionHeaderSize = {
    val size = Block.headerSize(true)
    Bytes.sizeOfUnsignedInt(size) + size
  }

  def headerSize(hasCompression: Boolean): Int =
    if (hasCompression)
      hasCompressionHeaderSize
    else
      noCompressionHeaderSize

  def writeIndexBlocks(keyValue: Transient,
                       skipList: Option[SkipList[Slice[Byte], Memory]],
                       hashIndex: Option[HashIndexBlock.State],
                       binarySearchIndex: Option[BinarySearchIndexBlock.State],
                       bloomFilter: Option[BloomFilterBlock.State],
                       currentMinMaxFunction: Option[MinMax[Slice[Byte]]],
                       currentNearestDeadline: Option[Deadline]): DeadlineAndFunctionId = {

    def writeOne(keyValue: Transient): Unit =
      keyValue match {

        case keyValue @ (_: Transient.Range | _: Transient.Fixed) =>
          //always write to the rootGroup's accessIndexOffset. Nested group's key-values are just opened and indexed againsts the rootGroup's key-values.
          val thisKeyValuesAccessOffset = keyValue.stats.thisKeyValuesAccessIndexOffset

          bloomFilter foreach (BloomFilterBlock.add(keyValue.key, _))

          hashIndex map {
            hashIndexState: HashIndexBlock.State =>
              if (keyValue.isPrefixCompressed) {
                //fix me - this should be managed by HashIndex itself.
                hashIndexState.miss += 1
                false
              } else if (hashIndexState.copyIndex) {
                HashIndexBlock.writeCopied(
                  key = keyValue.key,
                  thisKeyValuesAccessOffset = thisKeyValuesAccessOffset,
                  value = keyValue.indexEntryBytes,
                  state = hashIndexState
                )
              } else { //else build a reference hashIndex only.
                HashIndexBlock.write(
                  key = keyValue.key,
                  value = thisKeyValuesAccessOffset,
                  state = hashIndexState
                )
              }
          } match {
            //if it's a hit and binary search is not configured to be full.
            //no need to check if the value was previously written to binary search here since BinarySearchIndexBlock itself performs this check.
            case Some(hit) if hit && !keyValue.isRange && binarySearchIndex.forall(!_.isFullIndex) =>
              ()

            case None | Some(_) =>
              if (!keyValue.isPrefixCompressed)
                binarySearchIndex foreach {
                  state =>
                    BinarySearchIndexBlock.write(
                      indexOffset = thisKeyValuesAccessOffset,
                      keyOffset = keyValue.stats.thisKeyValuesKeyOffset,
                      mergedKey = keyValue.mergedKey,
                      keyType = keyValue.id,
                      state = state
                    )
                }
          }
      }

    @tailrec
    def writeRoot(keyValues: Slice[Transient],
                  currentMinMaxFunction: Option[MinMax[Slice[Byte]]],
                  currentNearestDeadline: Option[Deadline]): DeadlineAndFunctionId =
      keyValues.headOption match {
        case Some(keyValue) =>
          val nextNearestDeadline = Segment.getNearestDeadline(currentNearestDeadline, keyValue)
          var nextMinMaxFunctionId = currentMinMaxFunction

          keyValue match {
            case range: Transient.Range =>
              nextMinMaxFunctionId = MinMax.minMaxFunction(range, currentMinMaxFunction)

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(keyValue = range)

              skipList foreach {
                skipList =>
                  val fromKeyUnsliced = range.fromKey.unslice()
                  skipList.put(
                    fromKeyUnsliced,
                    Memory.Range(
                      fromKey = fromKeyUnsliced,
                      toKey = range.toKey.unslice(),
                      fromValue = range.fromValue.map(_.unslice),
                      rangeValue = range.rangeValue.unslice
                    )
                  )
              }

            case function: Transient.Function =>
              nextMinMaxFunctionId = Some(MinMax.minMaxFunction(function, currentMinMaxFunction))

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(keyValue = function)

              skipList foreach {
                skipList =>
                  val keyUnsliced = function.key.unslice()
                  skipList.put(
                    keyUnsliced,
                    Memory.Function(
                      key = keyUnsliced,
                      function = function.function.unslice(),
                      time = function.time.unslice()
                    )
                  )
              }

            case pendingApply: Transient.PendingApply =>
              nextMinMaxFunctionId = MinMax.minMaxFunction(pendingApply.applies, currentMinMaxFunction)

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(keyValue = pendingApply)

              skipList foreach {
                skipList =>
                  val keyUnsliced = pendingApply.key.unslice()
                  skipList.put(
                    keyUnsliced,
                    Memory.PendingApply(
                      key = keyUnsliced,
                      applies = pendingApply.applies.map(_.unslice)
                    )
                  )
              }

            case put: Transient.Put =>
              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(keyValue = put)

              skipList foreach {
                skipList =>
                  val keyUnsliced = put.key.unslice()
                  val unslicedValue = put.value flatMap (_.toOptionUnsliced())

                  skipList.put(
                    keyUnsliced,
                    Memory.Put(
                      key = keyUnsliced,
                      value = unslicedValue,
                      deadline = put.deadline,
                      time = put.time.unslice()
                    )
                  )
              }

            case remove: Transient.Remove =>
              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(keyValue = remove)

              skipList foreach {
                skipList =>
                  val keyUnsliced = remove.key.unslice()

                  skipList.put(
                    keyUnsliced,
                    Memory.Remove(
                      key = keyUnsliced,
                      deadline = remove.deadline,
                      time = remove.time.unslice()
                    )
                  )
              }

            case update: Transient.Update =>
              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(keyValue = update)

              skipList foreach {
                skipList =>
                  val keyUnsliced = update.key.unslice()
                  val unslicedValue = update.value flatMap (_.toOptionUnsliced())

                  skipList.put(
                    keyUnsliced,
                    Memory.Update(
                      key = keyUnsliced,
                      value = unslicedValue,
                      deadline = update.deadline,
                      time = update.time.unslice()
                    )
                  )
              }
          }

          writeRoot(keyValues.drop(1), nextMinMaxFunctionId, nextNearestDeadline)

        case None =>
          DeadlineAndFunctionId(currentNearestDeadline, currentMinMaxFunction)
      }

    writeRoot(
      keyValues = Slice(keyValue),
      currentMinMaxFunction = currentMinMaxFunction,
      currentNearestDeadline = currentNearestDeadline
    )
  }

  private def writeBlocks(keyValue: Transient,
                          sortedIndex: SortedIndexBlock.State,
                          values: Option[ValuesBlock.State],
                          hashIndex: Option[HashIndexBlock.State],
                          binarySearchIndex: Option[BinarySearchIndexBlock.State],
                          bloomFilter: Option[BloomFilterBlock.State],
                          currentMinMaxFunction: Option[MinMax[Slice[Byte]]],
                          currentNearestDeadline: Option[Deadline]): DeadlineAndFunctionId = {
    SortedIndexBlock.write(keyValue = keyValue, state = sortedIndex)
    values.foreach(ValuesBlock.write(keyValue, _))
    writeIndexBlocks(
      keyValue = keyValue,
      skipList = None,
      hashIndex = hashIndex,
      bloomFilter = bloomFilter,
      binarySearchIndex = binarySearchIndex,
      currentMinMaxFunction = currentMinMaxFunction,
      currentNearestDeadline = currentNearestDeadline
    )
  }

  private def closeBlocks(sortedIndex: SortedIndexBlock.State,
                          values: Option[ValuesBlock.State],
                          hashIndex: Option[HashIndexBlock.State],
                          binarySearchIndex: Option[BinarySearchIndexBlock.State],
                          bloomFilter: Option[BloomFilterBlock.State],
                          minMaxFunction: Option[MinMax[Slice[Byte]]],
                          nearestDeadline: Option[Deadline]): ClosedBlocks = {
    val sortedIndexClosed = SortedIndexBlock.close(sortedIndex)
    val valuesClosed = values.map(ValuesBlock.close)
    val hashIndexClosed = hashIndex.flatMap(HashIndexBlock.close)
    val binarySearchIndexClosed = binarySearchIndex.flatMap(BinarySearchIndexBlock.close)
    val bloomFilterClosed = bloomFilter.flatMap(BloomFilterBlock.close)

    ClosedBlocks(
      sortedIndex = sortedIndexClosed,
      values = valuesClosed,
      hashIndex = hashIndexClosed,
      binarySearchIndex = binarySearchIndexClosed,
      bloomFilter = bloomFilterClosed,
      minMaxFunction = minMaxFunction,
      nearestDeadline = nearestDeadline
    )
  }

  private def write(keyValues: Iterable[Transient],
                    sortedIndexBlock: SortedIndexBlock.State,
                    valuesBlock: Option[ValuesBlock.State],
                    hashIndexBlock: Option[HashIndexBlock.State],
                    binarySearchIndexBlock: Option[BinarySearchIndexBlock.State],
                    bloomFilterBlock: Option[BloomFilterBlock.State]): ClosedBlocks = {
    val nearestDeadlineMinMaxFunctionId =
      keyValues.foldLeft(DeadlineAndFunctionId(None, None)) {
        case (nearestDeadlineMinMaxFunctionId, keyValue) =>
          writeBlocks(
            keyValue = keyValue,
            sortedIndex = sortedIndexBlock,
            values = valuesBlock,
            hashIndex = hashIndexBlock,
            bloomFilter = bloomFilterBlock,
            binarySearchIndex = binarySearchIndexBlock,
            currentMinMaxFunction = nearestDeadlineMinMaxFunctionId.minMaxFunctionId,
            currentNearestDeadline = nearestDeadlineMinMaxFunctionId.nearestDeadline
          )
      }

    val closedBlocks =
      closeBlocks(
        sortedIndex = sortedIndexBlock,
        values = valuesBlock,
        hashIndex = hashIndexBlock,
        bloomFilter = bloomFilterBlock,
        binarySearchIndex = binarySearchIndexBlock,
        minMaxFunction = nearestDeadlineMinMaxFunctionId.minMaxFunctionId,
        nearestDeadline = nearestDeadlineMinMaxFunctionId.nearestDeadline
      )

    //ensure that all the slices are full.
    if (!sortedIndexBlock.bytes.isFull)
      throw IO.throwable(s"indexSlice is not full actual: ${sortedIndexBlock.bytes.size} - expected: ${sortedIndexBlock.bytes.allocatedSize}")
    else if (valuesBlock.exists(!_.bytes.isFull))
      throw IO.throwable(s"valuesSlice is not full actual: ${valuesBlock.get.bytes.size} - expected: ${valuesBlock.get.bytes.allocatedSize}")
    else
      closedBlocks
  }

  def writeClosed(keyValues: Iterable[Transient],
                  createdInLevel: Int,
                  segmentConfig: SegmentBlock.Config): SegmentBlock.Closed =
    if (keyValues.isEmpty) {
      SegmentBlock.Closed.empty
    } else {
      val openSegment =
        writeOpen(
          keyValues = keyValues,
          createdInLevel = createdInLevel,
          segmentConfig = segmentConfig
        )

      Block.block(
        openSegment = openSegment,
        compressions = segmentConfig.compressions(UncompressedBlockInfo(openSegment.segmentSize)),
        blockName = blockName
      )
    }

  def writeOpen(keyValues: Iterable[Transient],
                createdInLevel: Int,
                segmentConfig: SegmentBlock.Config): SegmentBlock.Open =
    if (keyValues.isEmpty)
      Open.empty
    else {
      val (sortedIndexBlock, normalisedKeyValues) = SortedIndexBlock.init(keyValues = keyValues)
      val footerBlock = SegmentFooterBlock.init(keyValues = normalisedKeyValues, createdInLevel = createdInLevel)
      val valuesBlock = ValuesBlock.init(keyValues = normalisedKeyValues)
      val hashIndexBlock = HashIndexBlock.init(keyValues = normalisedKeyValues)
      val binarySearchIndexBlock = BinarySearchIndexBlock.init(normalisedKeyValues = normalisedKeyValues, originalKeyValues = keyValues)
      val bloomFilterBlock = BloomFilterBlock.init(keyValues = keyValues)

      //      bloomFilterBlock foreach {
      //        bloomFilter =>
      //          //temporary check.
      //          val lastStats: Stats = keyValues.last.stats
      //          assert(
      //            bloomFilter.bytes.allocatedSize == lastStats.segmentBloomFilterSize,
      //            s"BloomFilter size calculation were incorrect. Actual: ${bloomFilter.bytes.allocatedSize}. Expected: ${lastStats.segmentBloomFilterSize}"
      //          )
      //      }

      val closedBlocks =
        write(
          keyValues = normalisedKeyValues,
          sortedIndexBlock = sortedIndexBlock,
          valuesBlock = valuesBlock,
          hashIndexBlock = hashIndexBlock,
          binarySearchIndexBlock = binarySearchIndexBlock,
          bloomFilterBlock = bloomFilterBlock
        )

      val closedFooterBlock = SegmentFooterBlock.writeAndClose(footerBlock, closedBlocks)

      close(closedFooterBlock, closedBlocks)
    }

  private def close(footerBlock: SegmentFooterBlock.State,
                    closedBlocks: ClosedBlocks): SegmentBlock.Open = {
    val headerSize = SegmentBlock.headerSize(true)
    val headerBytes = Slice.create[Byte](headerSize)
    //set header bytes to be fully written so that it does not closed when compression.
    headerBytes moveWritePosition headerBytes.allocatedSize

    val open =
      Open(
        headerBytes = headerBytes,
        footerBlock = footerBlock.bytes.close(),
        valuesBlock = closedBlocks.values.map(_.bytes.close()),
        sortedIndexBlock = closedBlocks.sortedIndex.bytes.close(),
        hashIndexBlock = closedBlocks.hashIndex map (_.bytes.close()),
        binarySearchIndexBlock = closedBlocks.binarySearchIndex map (_.bytes.close()),
        bloomFilterBlock = closedBlocks.bloomFilter map (_.bytes.close()),
        functionMinMax = closedBlocks.minMaxFunction,
        nearestDeadline = closedBlocks.nearestDeadline
      )

    Block.unblock(
      headerSize = open.headerBytes.size,
      bytes = open.headerBytes,
      blockName = blockName
    )

    open
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


