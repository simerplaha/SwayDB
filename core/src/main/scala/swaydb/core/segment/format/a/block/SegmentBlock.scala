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

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.compression.CompressionInternal
import swaydb.core.data.{Memory, Transient}
import swaydb.core.function.FunctionStore
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.Block.CompressionInfo
import swaydb.core.segment.format.a.block.reader.{CompressedBlockReader, DecompressedBlockReader}
import swaydb.core.segment.{DeadlineAndFunctionId, Segment}
import swaydb.core.util.{Bytes, MinMax}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.api.grouping.Compression
import swaydb.data.config.{BlockIO, BlockStatus, UncompressedBlockInfo}
import swaydb.data.slice.{Reader, Slice}

import scala.annotation.tailrec
import scala.concurrent.duration.Deadline
import scala.util.Try

private[core] object SegmentBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  val formatId: Byte = 1.toByte

  val crcBytes: Int = 7

  def emptyDecompressedBlock: DecompressedBlockReader[SegmentBlock] =
    DecompressedBlockReader.empty(
      SegmentBlock(
        offset = SegmentBlock.Offset.empty,
        headerSize = 0,
        compressionInfo = None
      )
    )

  def decompressed(bytes: Slice[Byte])(implicit updater: BlockUpdater[SegmentBlock]): DecompressedBlockReader[SegmentBlock] =
    DecompressedBlockReader.decompressed(
      decompressedBytes = bytes,
      block =
        SegmentBlock(
          offset = SegmentBlock.Offset(
            start = 0,
            size = bytes.size
          ),
          headerSize = 0,
          compressionInfo = None
        )
    )

  def compressed(bytes: Slice[Byte], compressionInfo: Block.CompressionInfo)(implicit updater: BlockUpdater[SegmentBlock]): CompressedBlockReader[SegmentBlock] =
    CompressedBlockReader.compressed(
      bytes = bytes,
      block =
        SegmentBlock(
          offset = SegmentBlock.Offset(
            start = 0,
            size = bytes.size
          ),
          headerSize = compressionInfo.headerSize,
          compressionInfo = Some(compressionInfo)
        )
    )

  object Config {

    def default =
      new Config(
        blockIO = _ => BlockIO.ConcurrentIO(false),
        compressions = _ => Seq.empty
      )

    def apply(segmentIO: BlockStatus => BlockIO,
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

  class Config(val blockIO: BlockStatus => BlockIO,
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

    def emptyIO = IO.Success(empty)

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

  def read(offset: SegmentBlock.Offset,
           segmentReader: Reader): IO[SegmentBlock] =
    Block.readHeader(
      offset = offset,
      reader = segmentReader
    ) map {
      header =>
        SegmentBlock(
          offset = offset,
          headerSize = header.headerSize,
          compressionInfo = header.compressionInfo
        )
    }

  //  def createDecompressedBlockReader(segmentReader: Reader): IO[DecompressedBlockReader[SegmentBlock]] =
  //    segmentReader.size map {
  //      size =>
  //        new DecompressedBlockReader(
  //          reader = segmentReader,
  //          block = SegmentBlock(
  //            offset = SegmentBlock.Offset(0, size.toInt),
  //            headerSize = 0,
  //            compressionInfo = None
  //          )
  //        )
  //    }

  val noCompressionHeaderSize = {
    val size = Block.headerSize(false)
    Bytes.sizeOf(size) + size
  }

  val hasCompressionHeaderSize = {
    val size = Block.headerSize(true)
    Bytes.sizeOf(size) + size
  }

  def headerSize(hasCompression: Boolean): Int =
    if (hasCompression)
      hasCompressionHeaderSize
    else
      noCompressionHeaderSize

  def writeIndexBlocks(keyValue: Transient,
                       memoryMap: Option[ConcurrentSkipListMap[Slice[Byte], Memory]],
                       hashIndex: Option[HashIndexBlock.State],
                       binarySearchIndex: Option[BinarySearchIndexBlock.State],
                       bloomFilter: Option[BloomFilterBlock.State],
                       currentMinMaxFunction: Option[MinMax[Slice[Byte]]],
                       currentNearestDeadline: Option[Deadline]): IO[DeadlineAndFunctionId] = {

    def writeOne(rootGroup: Option[Transient.Group],
                 keyValue: Transient): IO[Unit] =
      keyValue match {
        case childGroup: Transient.Group =>
          writeMany(
            rootGroup = rootGroup,
            keyValues = childGroup.keyValues
          )

        case keyValue @ (_: Transient.Range | _: Transient.Fixed) =>
          val thisKeyValuesAccessOffset =
            rootGroup
              .map(_.stats.thisKeyValuesAccessIndexOffset)
              .getOrElse(keyValue.stats.thisKeyValuesAccessIndexOffset)

          bloomFilter foreach (BloomFilterBlock.add(keyValue.minKey, _))

          hashIndex map {
            hashIndexState =>
              HashIndexBlock.write(
                key = keyValue.minKey,
                value = thisKeyValuesAccessOffset,
                state = hashIndexState
              )
          } match {
            //if it's a hit and binary search is not configured to be full OR the key-value has same offset as previous then skip writing to binary search.
            case Some(IO.Success(hit)) if (!keyValue.isRange && (hit && binarySearchIndex.forall(!_.isFullIndex))) || keyValue.previous.exists(_.stats.thisKeyValuesAccessIndexOffset == thisKeyValuesAccessOffset) =>
              IO.unit

            case None | Some(IO.Success(_)) =>
              binarySearchIndex map {
                state =>
                  BinarySearchIndexBlock.write(
                    value = thisKeyValuesAccessOffset,
                    state = state
                  )
              } getOrElse IO.unit

            case Some(IO.Failure(error)) =>
              IO.Failure(error)
          }
      }

    @tailrec
    def writeMany(rootGroup: Option[Transient.Group],
                  keyValues: Slice[Transient]): IO[Unit] =
      keyValues.headOption match {
        case Some(keyValue) =>
          writeOne(rootGroup, keyValue)
          writeMany(rootGroup, keyValues.drop(1))

        case None =>
          IO.unit
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
            case rootGroup: Transient.Group =>
              nextMinMaxFunctionId = MinMax.minMax(currentMinMaxFunction, rootGroup.minMaxFunctionId)(FunctionStore.order)

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeMany(
                  rootGroup = Some(rootGroup),
                  keyValues = rootGroup.keyValues
                ).get

              memoryMap foreach {
                skipList =>
                  val minKeyUnsliced = rootGroup.minKey.unslice()
                  skipList.put(
                    minKeyUnsliced,
                    Memory.Group(
                      minKey = minKeyUnsliced,
                      maxKey = rootGroup.maxKey.unslice(),
                      blockedSegment = rootGroup.blockedSegment
                    )
                  )
              }

            case range: Transient.Range =>
              nextMinMaxFunctionId = MinMax.minMaxFunction(range, currentMinMaxFunction)

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(
                  rootGroup = None,
                  keyValue = range
                ).get

              memoryMap foreach {
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
                writeOne(
                  rootGroup = None,
                  keyValue = function
                ).get

              memoryMap foreach {
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
                writeOne(
                  rootGroup = None,
                  keyValue = pendingApply
                ).get

              memoryMap foreach {
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
                writeOne(
                  rootGroup = None,
                  keyValue = put
                ).get

              memoryMap foreach {
                skipList =>
                  val keyUnsliced = put.key.unslice()
                  val unslicedValue = put.value flatMap (_.unsliceNonEmpty())

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
                writeOne(
                  rootGroup = None,
                  keyValue = remove
                ).get

              memoryMap foreach {
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
                writeOne(
                  rootGroup = None,
                  keyValue = update
                ).get

              memoryMap foreach {
                skipList =>
                  val keyUnsliced = update.key.unslice()
                  val unslicedValue = update.value flatMap (_.unsliceNonEmpty())

                  skipList.put(
                    keyUnsliced,
                    Memory.Put(
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

    IO {
      writeRoot(
        keyValues = Slice(keyValue),
        currentMinMaxFunction = currentMinMaxFunction,
        currentNearestDeadline = currentNearestDeadline
      )
    }
  }

  private def writeBlocks(keyValue: Transient,
                          sortedIndex: SortedIndexBlock.State,
                          values: Option[ValuesBlock.State],
                          hashIndex: Option[HashIndexBlock.State],
                          binarySearchIndex: Option[BinarySearchIndexBlock.State],
                          bloomFilter: Option[BloomFilterBlock.State],
                          currentMinMaxFunction: Option[MinMax[Slice[Byte]]],
                          currentNearestDeadline: Option[Deadline]): IO[DeadlineAndFunctionId] =
    SortedIndexBlock
      .write(keyValue = keyValue, state = sortedIndex)
      .flatMap(_ => values.map(ValuesBlock.write(keyValue, _)) getOrElse IO.unit)
      .flatMap {
        _ =>
          writeIndexBlocks(
            keyValue = keyValue,
            memoryMap = None,
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
                          nearestDeadline: Option[Deadline]): IO[ClosedBlocks] =
    for {
      sortedIndexClosed <- SortedIndexBlock.close(sortedIndex)
      valuesClosed <- values.map(values => ValuesBlock.close(values).map(Some(_))) getOrElse IO.none
      hashIndexClosed <- hashIndex.map(HashIndexBlock.close) getOrElse IO.none
      binarySearchIndexClosed <- binarySearchIndex.map(BinarySearchIndexBlock.close) getOrElse IO.none
      bloomFilterClosed <- bloomFilter.map(BloomFilterBlock.close) getOrElse IO.none
    } yield
      ClosedBlocks(
        sortedIndex = sortedIndexClosed,
        values = valuesClosed,
        hashIndex = hashIndexClosed,
        binarySearchIndex = binarySearchIndexClosed,
        bloomFilter = bloomFilterClosed,
        minMaxFunction = minMaxFunction,
        nearestDeadline = nearestDeadline
      )

  private def write(keyValues: Iterable[Transient],
                    sortedIndexBlock: SortedIndexBlock.State,
                    valuesBlock: Option[ValuesBlock.State],
                    hashIndexBlock: Option[HashIndexBlock.State],
                    binarySearchIndexBlock: Option[BinarySearchIndexBlock.State],
                    bloomFilterBlock: Option[BloomFilterBlock.State]): IO[ClosedBlocks] =
    keyValues.foldLeftIO(DeadlineAndFunctionId(None, None)) {
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
    } flatMap {
      nearestDeadlineMinMaxFunctionId =>
        closeBlocks(
          sortedIndex = sortedIndexBlock,
          values = valuesBlock,
          hashIndex = hashIndexBlock,
          bloomFilter = bloomFilterBlock,
          binarySearchIndex = binarySearchIndexBlock,
          minMaxFunction = nearestDeadlineMinMaxFunctionId.minMaxFunctionId,
          nearestDeadline = nearestDeadlineMinMaxFunctionId.nearestDeadline
        )
    } flatMap {
      result =>
        //ensure that all the slices are full.
        if (!sortedIndexBlock.bytes.isFull)
          IO.Failure(new Exception(s"indexSlice is not full actual: ${sortedIndexBlock.bytes.size} - expected: ${sortedIndexBlock.bytes.allocatedSize}"))
        else if (valuesBlock.exists(!_.bytes.isFull))
          IO.Failure(new Exception(s"valuesSlice is not full actual: ${valuesBlock.get.bytes.size} - expected: ${valuesBlock.get.bytes.allocatedSize}"))
        else
          IO.Success(result)
    }

  def writeClosed(keyValues: Iterable[Transient],
                  createdInLevel: Int,
                  segmentConfig: SegmentBlock.Config): IO[SegmentBlock.Closed] =
    if (keyValues.isEmpty)
      SegmentBlock.Closed.emptyIO
    else
      writeOpen(
        keyValues = keyValues,
        createdInLevel = createdInLevel,
        segmentConfig = segmentConfig
      ) flatMap {
        openSegment =>
          Block.compress(
            openSegment = openSegment,
            compressions = segmentConfig.compressions(UncompressedBlockInfo(openSegment.segmentSize)),
            blockName = blockName
          )
      }

  def writeOpen(keyValues: Iterable[Transient],
                createdInLevel: Int,
                segmentConfig: SegmentBlock.Config): IO[SegmentBlock.Open] =
    if (keyValues.isEmpty)
      Open.emptyIO
    else {
      val footerBlock = SegmentFooterBlock.init(keyValues = keyValues, createdInLevel = createdInLevel)
      val sortedIndexBlock = SortedIndexBlock.init(keyValues = keyValues)
      val valuesBlock = ValuesBlock.init(keyValues = keyValues)
      val hashIndexBlock = HashIndexBlock.init(keyValues = keyValues)
      val binarySearchIndexBlock = BinarySearchIndexBlock.init(keyValues = keyValues)
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

      write(
        keyValues = keyValues,
        sortedIndexBlock = sortedIndexBlock,
        valuesBlock = valuesBlock,
        hashIndexBlock = hashIndexBlock,
        binarySearchIndexBlock = binarySearchIndexBlock,
        bloomFilterBlock = bloomFilterBlock
      ) flatMap {
        closedBlocks =>
          SegmentFooterBlock
            .writeAndClose(footerBlock, closedBlocks)
            .flatMap(close(_, closedBlocks))
      }
    }

  private def close(footerBlock: SegmentFooterBlock.State,
                    closedBlocks: ClosedBlocks): IO[SegmentBlock.Open] =
    IO {
      val headerSize = SegmentBlock.headerSize(true)
      val headerBytes = Slice.create[Byte](headerSize)
      //set header bytes to be fully written so that it does not closed when compression.
      headerBytes moveWritePosition headerBytes.allocatedSize

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
    } flatMap {
      open =>
        Block.uncompressed(
          headerSize = open.headerBytes.size,
          bytes = open.headerBytes,
          blockName = blockName
        ) map {
          _ =>
            open
        }
    }

  implicit object SegmentBlockUpdater extends BlockUpdater[SegmentBlock] {
    override def updateOffset(block: SegmentBlock, start: Int, size: Int): SegmentBlock =
      block.copy(offset = SegmentBlock.Offset(start = start, size = size))
  }
}

private[core] case class SegmentBlock(offset: SegmentBlock.Offset,
                                      headerSize: Int,
                                      compressionInfo: Option[Block.CompressionInfo]) extends Block

