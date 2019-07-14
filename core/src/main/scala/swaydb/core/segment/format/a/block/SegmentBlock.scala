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
import swaydb.core.data.{KeyValue, Memory, Stats, Transient}
import swaydb.core.function.FunctionStore
import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.{DeadlineAndFunctionId, Segment}
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.util.{Bytes, CRC32, MinMax}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.api.grouping.Compression
import swaydb.data.config.{BlockIO, BlockStatus, UncompressedBlockInfo}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec
import scala.concurrent.duration.Deadline
import scala.util.Try

private[core] object SegmentBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  val formatId: Byte = 1.toByte

  val crcBytes: Int = 7

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

  case class Offset(start: Int, size: Int) extends BlockOffset

  case class Footer(valuesOffset: Option[ValuesBlock.Offset],
                    sortedIndexOffset: SortedIndexBlock.Offset,
                    hashIndexOffset: Option[HashIndexBlock.Offset],
                    binarySearchIndexOffset: Option[BinarySearchIndexBlock.Offset],
                    bloomFilterOffset: Option[BloomFilterBlock.Offset],
                    keyValueCount: Int,
                    createdInLevel: Int,
                    bloomFilterItemsCount: Int,
                    hasRange: Boolean,
                    hasGroup: Boolean,
                    hasPut: Boolean)

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
      segmentBytes foreach (slice addAll _.unslice())
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
        values = None,
        sortedIndex = Slice.emptyBytes,
        hashIndex = None,
        binarySearchIndex = None,
        bloomFilter = None,
        footer = Slice.emptyBytes,
        functionMinMax = None,
        nearestDeadline = None
      )

    def emptyIO = IO.Success(empty)

    def apply(headerBytes: Slice[Byte],
              values: Option[Slice[Byte]],
              sortedIndex: Slice[Byte],
              hashIndex: Option[Slice[Byte]],
              binarySearchIndex: Option[Slice[Byte]],
              bloomFilter: Option[Slice[Byte]],
              footer: Slice[Byte],
              functionMinMax: Option[MinMax[Slice[Byte]]],
              nearestDeadline: Option[Deadline]): Open =
      new Open(
        headerBytes = headerBytes,
        values = values,
        sortedIndex = sortedIndex,
        hashIndex = hashIndex,
        binarySearchIndex = binarySearchIndex,
        bloomFilter = bloomFilter,
        footer = footer,
        functionMinMax = functionMinMax,
        nearestDeadline = nearestDeadline
      )
  }

  class Open(val headerBytes: Slice[Byte],
             val values: Option[Slice[Byte]],
             val sortedIndex: Slice[Byte],
             val hashIndex: Option[Slice[Byte]],
             val binarySearchIndex: Option[Slice[Byte]],
             val bloomFilter: Option[Slice[Byte]],
             val footer: Slice[Byte],
             val functionMinMax: Option[MinMax[Slice[Byte]]],
             val nearestDeadline: Option[Deadline]) {

    headerBytes moveWritePosition headerBytes.allocatedSize

    val segmentBytes: Slice[Slice[Byte]] = {
      val allBytes = Slice.create[Slice[Byte]](8)
      allBytes add headerBytes.close()
      values foreach (allBytes add _)
      allBytes add sortedIndex
      hashIndex foreach (allBytes add _)
      binarySearchIndex foreach (allBytes add _)
      bloomFilter foreach (allBytes add _)
      allBytes add footer
      allBytes.filter(_.nonEmpty).close()
    }

    def isEmpty: Boolean =
      segmentBytes.exists(_.isEmpty)

    def segmentSize =
      segmentBytes.foldLeft(0)(_ + _.size)

    def flattenSegmentBytes: Slice[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.size)
      val slice = Slice.create[Byte](size)
      segmentBytes foreach (slice addAll _.unslice())
      assert(slice.isFull)
      slice
    }

    def flattenSegment: (Slice[Byte], Option[Deadline]) =
      (flattenSegmentBytes, nearestDeadline)
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

  def createUnblockedReader(bytes: Slice[Byte]): IO[BlockReader[SegmentBlock]] =
    createUnblockedReader(Reader(bytes))

  def createUnblockedReader(segmentReader: Reader): IO[BlockReader[SegmentBlock]] =
    segmentReader.size map {
      size =>
        BlockReader(
          reader = segmentReader,
          block = SegmentBlock(
            offset = SegmentBlock.Offset(0, size.toInt),
            headerSize = 0,
            compressionInfo = None
          )
        )
    }

  //all these functions are wrapper with a try catch block with value only to make it easier to read.
  def readFooter(reader: BlockReader[SegmentBlock]): IO[Footer] =
    try {
      val segmentBlockSize = reader.size.get.toInt
      val footerStartOffset = reader.moveTo(segmentBlockSize - ByteSizeOf.int).readInt().get
      val footerSize = segmentBlockSize - footerStartOffset
      val footerBytes = reader.moveTo(footerStartOffset).read(footerSize - ByteSizeOf.int).get
      val footerReader = Reader(footerBytes)
      val formatId = footerReader.readIntUnsigned().get
      if (formatId != SegmentBlock.formatId) {
        val message = s"Invalid Segment formatId: $formatId. Expected: ${SegmentBlock.formatId}"
        return IO.Failure(IO.Error.Fatal(SegmentCorruptionException(message = message, cause = new Exception(message))))
      }
      assert(formatId == SegmentBlock.formatId, s"Invalid Segment formatId: $formatId. Expected: ${SegmentBlock.formatId}")
      val createdInLevel = footerReader.readIntUnsigned().get
      val hasGroup = footerReader.readBoolean().get
      val hasRange = footerReader.readBoolean().get
      val hasPut = footerReader.readBoolean().get
      val keyValueCount = footerReader.readIntUnsigned().get
      val bloomFilterItemsCount = footerReader.readIntUnsigned().get
      val expectedCRC = footerReader.readLong().get
      val crcBytes = footerBytes.take(SegmentBlock.crcBytes)
      val crc = CRC32.forBytes(crcBytes)
      if (expectedCRC != crc) {
        IO.Failure(SegmentCorruptionException(s"Corrupted Segment: CRC Check failed. $expectedCRC != $crc", new Exception("CRC check failed.")))
      } else {
        val sortedIndexOffset =
          SortedIndexBlock.Offset(
            size = footerReader.readIntUnsigned().get,
            start = footerReader.readIntUnsigned().get
          )

        val hashIndexSize = footerReader.readIntUnsigned().get
        val hashIndexOffset =
          if (hashIndexSize == 0)
            None
          else
            Some(
              HashIndexBlock.Offset(
                start = footerReader.readIntUnsigned().get,
                size = hashIndexSize
              )
            )

        val binarySearchIndexSize = footerReader.readIntUnsigned().get
        val binarySearchIndexOffset =
          if (binarySearchIndexSize == 0)
            None
          else
            Some(
              BinarySearchIndexBlock.Offset(
                start = footerReader.readIntUnsigned().get,
                size = binarySearchIndexSize
              )
            )

        val bloomFilterSize = footerReader.readIntUnsigned().get
        val bloomFilterOffset =
          if (bloomFilterSize == 0)
            None
          else
            Some(
              BloomFilterBlock.Offset(
                start = footerReader.readIntUnsigned().get,
                size = bloomFilterSize
              )
            )

        val valuesOffset =
          if (sortedIndexOffset.start == 0)
            None
          else
            Some(ValuesBlock.Offset(0, sortedIndexOffset.start))

        IO.Success(
          Footer(
            valuesOffset = valuesOffset,
            sortedIndexOffset = sortedIndexOffset,
            hashIndexOffset = hashIndexOffset,
            binarySearchIndexOffset = binarySearchIndexOffset,
            bloomFilterOffset = bloomFilterOffset,
            keyValueCount = keyValueCount,
            createdInLevel = createdInLevel,
            bloomFilterItemsCount = bloomFilterItemsCount,
            hasRange = hasRange,
            hasGroup = hasGroup,
            hasPut = hasPut
          )
        )
      }
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            IO.Failure(
              IO.Error.Fatal(
                SegmentCorruptionException(
                  message = "Corrupted Segment: Failed to read footer bytes",
                  cause = exception
                )
              )
            )

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

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

  def writeIndexBlocks(keyValue: KeyValue.WriteOnly,
                       memoryMap: Option[ConcurrentSkipListMap[Slice[Byte], Memory]],
                       hashIndex: Option[HashIndexBlock.State],
                       binarySearchIndex: Option[BinarySearchIndexBlock.State],
                       bloomFilter: Option[BloomFilterBlock.State],
                       currentMinMaxFunction: Option[MinMax[Slice[Byte]]],
                       currentNearestDeadline: Option[Deadline]): IO[DeadlineAndFunctionId] = {

    def writeOne(rootGroup: Option[KeyValue.WriteOnly.Group],
                 keyValue: KeyValue.WriteOnly): IO[Unit] =
      keyValue match {
        case childGroup: KeyValue.WriteOnly.Group =>
          writeMany(
            rootGroup = rootGroup,
            keyValues = childGroup.keyValues
          )

        case keyValue @ (_: KeyValue.WriteOnly.Range | _: KeyValue.WriteOnly.Fixed) =>
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
    def writeMany(rootGroup: Option[KeyValue.WriteOnly.Group],
                  keyValues: Slice[KeyValue.WriteOnly]): IO[Unit] =
      keyValues.headOption match {
        case Some(keyValue) =>
          writeOne(rootGroup, keyValue)
          writeMany(rootGroup, keyValues.drop(1))

        case None =>
          IO.unit
      }

    @tailrec
    def writeRoot(keyValues: Slice[KeyValue.WriteOnly],
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

  private def writeBlocks(keyValue: KeyValue.WriteOnly,
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

  private def write(keyValues: Iterable[KeyValue.WriteOnly],
                    sortedIndex: SortedIndexBlock.State,
                    values: Option[ValuesBlock.State],
                    hashIndex: Option[HashIndexBlock.State],
                    binarySearchIndex: Option[BinarySearchIndexBlock.State],
                    bloomFilter: Option[BloomFilterBlock.State]): IO[ClosedBlocks] =
    keyValues.foldLeftIO(DeadlineAndFunctionId(None, None)) {
      case (nearestDeadlineMinMaxFunctionId, keyValue) =>
        writeBlocks(
          keyValue = keyValue,
          sortedIndex = sortedIndex,
          values = values,
          hashIndex = hashIndex,
          bloomFilter = bloomFilter,
          binarySearchIndex = binarySearchIndex,
          currentMinMaxFunction = nearestDeadlineMinMaxFunctionId.minMaxFunctionId,
          currentNearestDeadline = nearestDeadlineMinMaxFunctionId.nearestDeadline
        )
    } flatMap {
      nearestDeadlineMinMaxFunctionId =>
        closeBlocks(
          sortedIndex = sortedIndex,
          values = values,
          hashIndex = hashIndex,
          bloomFilter = bloomFilter,
          binarySearchIndex = binarySearchIndex,
          minMaxFunction = nearestDeadlineMinMaxFunctionId.minMaxFunctionId,
          nearestDeadline = nearestDeadlineMinMaxFunctionId.nearestDeadline
        )
    } flatMap {
      result =>
        //ensure that all the slices are full.
        if (!sortedIndex.bytes.isFull)
          IO.Failure(new Exception(s"indexSlice is not full actual: ${sortedIndex.bytes.size} - expected: ${sortedIndex.bytes.allocatedSize}"))
        else if (values.exists(!_.bytes.isFull))
          IO.Failure(new Exception(s"valuesSlice is not full actual: ${values.get.bytes.size} - expected: ${values.get.bytes.allocatedSize}"))
        else
          IO.Success(result)
    }

  def writeClosed(keyValues: Iterable[KeyValue.WriteOnly],
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
          Block.create(
            openSegment = openSegment,
            compressions = segmentConfig.compressions(UncompressedBlockInfo(openSegment.segmentSize)),
            blockName = blockName
          )
      }

  def writeOpen(keyValues: Iterable[KeyValue.WriteOnly],
                createdInLevel: Int,
                segmentConfig: SegmentBlock.Config): IO[SegmentBlock.Open] =
    if (keyValues.isEmpty)
      Open.emptyIO
    else {
      val sortedIndex = SortedIndexBlock.init(keyValues = keyValues)
      val values = ValuesBlock.init(keyValues = keyValues)
      val hashIndex = HashIndexBlock.init(keyValues = keyValues)
      val binarySearchIndex = BinarySearchIndexBlock.init(keyValues = keyValues)
      val bloomFilter = BloomFilterBlock.init(keyValues = keyValues)

      bloomFilter foreach {
        bloomFilter =>
          //temporary check.
          val lastStats: Stats = keyValues.last.stats
          assert(
            bloomFilter.bytes.allocatedSize == lastStats.segmentBloomFilterSize,
            s"BloomFilter size calculation were incorrect. Actual: ${bloomFilter.bytes.allocatedSize}. Expected: ${lastStats.segmentBloomFilterSize}"
          )
      }

      write(
        keyValues = keyValues,
        sortedIndex = sortedIndex,
        values = values,
        hashIndex = hashIndex,
        binarySearchIndex = binarySearchIndex,
        bloomFilter = bloomFilter
      ) flatMap {
        closedBlocks =>
          writeFooter(
            closedBlocks = closedBlocks,
            keyValues = keyValues,
            createdInLevel = createdInLevel,
            segmentConfig = segmentConfig
          )
      }
    }

  private def writeFooter(closedBlocks: ClosedBlocks,
                          keyValues: Iterable[KeyValue.WriteOnly],
                          createdInLevel: Int,
                          segmentConfig: SegmentBlock.Config): IO[SegmentBlock.Open] =
    IO {
      val lastStats: Stats = keyValues.last.stats

      val values = closedBlocks.values
      val sortedIndex = closedBlocks.sortedIndex
      val hashIndex = closedBlocks.hashIndex
      val binarySearchIndex = closedBlocks.binarySearchIndex
      val bloomFilter = closedBlocks.bloomFilter

      val segmentFooterSlice = Slice.create[Byte](Stats.segmentFooterSize)
      //this is a placeholder to store the format type of the Segment file written.
      //currently there is only one format. So this is hardcoded but if there are a new file format then
      //SegmentWriter and SegmentReader should be changed to be type classes with unique format types ids.
      //the following group of bytes are also used for CRC check.
      segmentFooterSlice addIntUnsigned SegmentBlock.formatId
      segmentFooterSlice addIntUnsigned createdInLevel
      segmentFooterSlice addBoolean lastStats.segmentHasGroup
      segmentFooterSlice addBoolean lastStats.segmentHasRange
      segmentFooterSlice addBoolean lastStats.segmentHasPut
      //here the top Level key-values are used instead of Group's internal key-values because Group's internal key-values
      //are read when the Group key-value is read.
      segmentFooterSlice addIntUnsigned keyValues.size
      //total number of actual key-values grouped or un-grouped
      segmentFooterSlice addIntUnsigned lastStats.segmentUniqueKeysCount

      //do CRC
      val indexBytesToCRC = segmentFooterSlice.take(SegmentBlock.crcBytes)
      assert(indexBytesToCRC.size == SegmentBlock.crcBytes, s"Invalid CRC bytes size: ${indexBytesToCRC.size}. Required: ${SegmentBlock.crcBytes}")
      segmentFooterSlice addLong CRC32.forBytes(indexBytesToCRC)

      var segmentOffset = values.map(_.bytes.size) getOrElse 0

      segmentFooterSlice addIntUnsigned sortedIndex.bytes.size
      segmentFooterSlice addIntUnsigned segmentOffset
      segmentOffset = segmentOffset + sortedIndex.bytes.size

      hashIndex map {
        hashIndex =>
          segmentFooterSlice addIntUnsigned hashIndex.bytes.size
          segmentFooterSlice addIntUnsigned segmentOffset
          segmentOffset = segmentOffset + hashIndex.bytes.size
      } getOrElse {
        segmentFooterSlice addIntUnsigned 0
      }

      binarySearchIndex map {
        binarySearchIndex =>
          segmentFooterSlice addIntUnsigned binarySearchIndex.bytes.size
          segmentFooterSlice addIntUnsigned segmentOffset
          segmentOffset = segmentOffset + binarySearchIndex.bytes.size
      } getOrElse {
        segmentFooterSlice addIntUnsigned 0
      }

      bloomFilter map {
        bloomFilter =>
          segmentFooterSlice addIntUnsigned bloomFilter.bytes.size
          segmentFooterSlice addIntUnsigned segmentOffset
          segmentOffset = segmentOffset + bloomFilter.bytes.size
      } getOrElse {
        segmentFooterSlice addIntUnsigned 0
      }

      val footerOffset =
        values.map(_.bytes.size).getOrElse(0) +
          sortedIndex.bytes.size +
          hashIndex.map(_.bytes.size).getOrElse(0) +
          binarySearchIndex.map(_.bytes.size).getOrElse(0) +
          bloomFilter.map(_.bytes.size).getOrElse(0)

      segmentFooterSlice addInt footerOffset

      val headerSize = SegmentBlock.headerSize(true)
      val headerBytes = Slice.create[Byte](headerSize)
      //set header bytes to be fully written so that it does not closed when compression.
      headerBytes moveWritePosition headerBytes.allocatedSize

      Open(
        headerBytes = headerBytes,
        values = values.map(_.bytes.close()),
        sortedIndex = sortedIndex.bytes.close(),
        hashIndex = hashIndex map (_.bytes.close()),
        binarySearchIndex = binarySearchIndex map (_.bytes.close()),
        bloomFilter = bloomFilter map (_.bytes.close()),
        footer = segmentFooterSlice.close(),
        functionMinMax = closedBlocks.minMaxFunction,
        nearestDeadline = closedBlocks.nearestDeadline
      )
    } flatMap {
      segmentBlock =>
        Block.createUncompressedBlock(
          headerSize = segmentBlock.headerBytes.size,
          bytes = segmentBlock.headerBytes
        ) map {
          _ =>
            segmentBlock
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

