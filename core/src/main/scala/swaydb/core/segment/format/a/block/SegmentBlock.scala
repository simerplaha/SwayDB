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

import swaydb.compression.CompressionInternal
import swaydb.core.data.{KeyValue, Stats, Transient}
import swaydb.core.function.FunctionStore
import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.Segment
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.{NearestDeadlineMinMaxFunctionId, OffsetBase}
import swaydb.core.util.{Bytes, CRC32, MinMax}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec
import scala.concurrent.duration.Deadline

private[core] object SegmentBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  val formatId: Byte = 1.toByte

  val crcBytes: Int = 7

  case class Offset(start: Int, size: Int) extends OffsetBase

  case class Footer(valuesOffset: Option[Values.Offset],
                    sortedIndexOffset: SortedIndex.Offset,
                    hashIndexOffset: Option[HashIndex.Offset],
                    binarySearchIndexOffset: Option[BinarySearchIndex.Offset],
                    bloomFilterOffset: Option[BloomFilter.Offset],
                    keyValueCount: Int,
                    createdInLevel: Int,
                    bloomFilterItemsCount: Int,
                    hasRange: Boolean,
                    hasGroup: Boolean,
                    hasPut: Boolean)

  object ClosedSegment {
    val empty =
      ClosedSegment(
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

    val emptyIO = IO.Success(empty)

    def apply(headerBytes: Slice[Byte],
              values: Option[Slice[Byte]],
              sortedIndex: Slice[Byte],
              hashIndex: Option[Slice[Byte]],
              binarySearchIndex: Option[Slice[Byte]],
              bloomFilter: Option[Slice[Byte]],
              footer: Slice[Byte],
              functionMinMax: Option[MinMax[Slice[Byte]]],
              nearestDeadline: Option[Deadline]): ClosedSegment = {
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

      new ClosedSegment(
        segmentBytes = segmentBytes,
        minMaxFunctionId = functionMinMax,
        nearestDeadline = nearestDeadline
      )
    }
  }

  case class ClosedSegment(segmentBytes: Slice[Slice[Byte]],
                           minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                           nearestDeadline: Option[Deadline]) {

    def isEmpty: Boolean =
      segmentBytes.exists(_.isEmpty)

    def segmentSize =
      segmentBytes.foldLeft(0)(_ + _.size)

    def flattenSegmentBytes: Slice[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.size)
      val slice = Slice.create[Byte](size)
      segmentBytes.map(_.unslice()) foreach slice.addAll
      assert(slice.isFull)
      slice
    }

    def flattenSegment: (Slice[Byte], Option[Deadline]) =
      (flattenSegmentBytes, nearestDeadline)
  }

  private case class ClosedBlocks(sortedIndex: SortedIndex.State,
                                  values: Option[Values.State],
                                  hashIndex: Option[HashIndex.State],
                                  binarySearchIndex: Option[BinarySearchIndex.State],
                                  bloomFilter: Option[BloomFilter.State],
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
          SortedIndex.Offset(
            size = footerReader.readIntUnsigned().get,
            start = footerReader.readIntUnsigned().get
          )

        val hashIndexSize = footerReader.readIntUnsigned().get
        val hashIndexOffset =
          if (hashIndexSize == 0)
            None
          else
            Some(
              HashIndex.Offset(
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
              BinarySearchIndex.Offset(
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
              BloomFilter.Offset(
                start = footerReader.readIntUnsigned().get,
                size = bloomFilterSize
              )
            )

        val valuesOffset =
          if (sortedIndexOffset.start == 0)
            None
          else
            Some(Values.Offset(0, sortedIndexOffset.start))

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

  private def writeBlocks(keyValue: KeyValue.WriteOnly,
                          sortedIndex: SortedIndex.State,
                          values: Option[Values.State],
                          hashIndex: Option[HashIndex.State],
                          binarySearchIndex: Option[BinarySearchIndex.State],
                          bloomFilter: Option[BloomFilter.State],
                          currentMinMaxFunction: Option[MinMax[Slice[Byte]]],
                          currentNearestDeadline: Option[Deadline]): IO[NearestDeadlineMinMaxFunctionId] = {

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

          bloomFilter foreach (BloomFilter.add(keyValue.minKey, _))

          hashIndex map {
            hashIndexState =>
              HashIndex.write(
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
                  BinarySearchIndex.write(
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
    def write(keyValues: Slice[KeyValue.WriteOnly],
              currentMinMaxFunction: Option[MinMax[Slice[Byte]]],
              nearestDeadline: Option[Deadline]): NearestDeadlineMinMaxFunctionId =
      keyValues.headOption match {
        case Some(keyValue) =>
          val nextNearestDeadline = Segment.getNearestDeadline(nearestDeadline, keyValue)
          var nextMinMaxFunctionId = currentMinMaxFunction

          keyValue match {
            case rootGroup: Transient.Group =>
              nextMinMaxFunctionId = MinMax.minMax(currentMinMaxFunction, rootGroup.minMaxFunctionId)(FunctionStore.order)

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeMany(
                  rootGroup = Some(rootGroup),
                  keyValues = rootGroup.keyValues
                ).get

            case range: Transient.Range =>
              nextMinMaxFunctionId = MinMax.minMaxFunction(range.fromValue, currentMinMaxFunction)(FunctionStore.order)
              nextMinMaxFunctionId = MinMax.minMaxFunction(range.rangeValue, currentMinMaxFunction)(FunctionStore.order)

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(
                  rootGroup = None,
                  keyValue = range
                ).get

            case function: Transient.Function =>
              nextMinMaxFunctionId = Some(MinMax.minMaxFunction(function, currentMinMaxFunction)(FunctionStore.order))

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(
                  rootGroup = None,
                  keyValue = function
                ).get

            case pendingApply: Transient.PendingApply =>
              nextMinMaxFunctionId = MinMax.minMaxFunction(pendingApply.applies, currentMinMaxFunction)(FunctionStore.order)

              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(
                  rootGroup = None,
                  keyValue = pendingApply
                ).get

            case others @ (_: Transient.Put | _: Transient.Remove | _: Transient.Update) =>
              if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
                writeOne(
                  rootGroup = None,
                  keyValue = others
                ).get
          }

          write(keyValues.drop(1), nextMinMaxFunctionId, nextNearestDeadline)

        case None =>
          NearestDeadlineMinMaxFunctionId(nearestDeadline, currentMinMaxFunction)
      }

    if (keyValue.valueEntryBytes.nonEmpty && values.isEmpty)
      Values.valueSliceNotInitialised
    else
      SortedIndex
        .write(keyValue = keyValue, state = sortedIndex)
        .flatMap(_ => values.map(Values.write(keyValue, _)) getOrElse IO.unit)
        .map {
          _ =>
            write(
              keyValues = Slice(keyValue),
              currentMinMaxFunction = currentMinMaxFunction,
              nearestDeadline = currentNearestDeadline
            )
        }
  }

  private def closeBlocks(sortedIndex: SortedIndex.State,
                          values: Option[Values.State],
                          hashIndex: Option[HashIndex.State],
                          binarySearchIndex: Option[BinarySearchIndex.State],
                          bloomFilter: Option[BloomFilter.State],
                          minMaxFunction: Option[MinMax[Slice[Byte]]],
                          nearestDeadline: Option[Deadline]): IO[ClosedBlocks] =
    for {
      sortedIndexClosed <- SortedIndex.close(sortedIndex)
      valuesClosed <- values.map(values => Values.close(values).map(Some(_))) getOrElse IO.none
      hashIndexClosed <- hashIndex.map(HashIndex.close) getOrElse IO.none
      binarySearchIndexClosed <- binarySearchIndex.map(BinarySearchIndex.close) getOrElse IO.none
      bloomFilterClosed <- bloomFilter.map(BloomFilter.close) getOrElse IO.none
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
                    sortedIndex: SortedIndex.State,
                    values: Option[Values.State],
                    hashIndex: Option[HashIndex.State],
                    binarySearchIndex: Option[BinarySearchIndex.State],
                    bloomFilter: Option[BloomFilter.State]): IO[ClosedBlocks] =
    keyValues.foldLeftIO(NearestDeadlineMinMaxFunctionId(None, None)) {
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
          IO.Failure(new Exception(s"indexSlice is not full actual: ${sortedIndex.bytes.size} - expected: ${sortedIndex.bytes.size}"))
        else if (values.exists(!_.bytes.isFull))
          IO.Failure(new Exception(s"valuesSlice is not full actual: ${values.get.bytes.size} - expected: ${values.get.bytes.size}"))
        else
          IO.Success(result)
    }

  /**
    * Rules for creating bloom filters
    *
    * If key-values contains:
    * 1. A Remove range - bloom filters are not created because 'mightContain' checks bloomFilters only and bloomFilters
    * do not have range scans. BloomFilters are still created for Update ranges because even if boomFilter returns false,
    * 'mightContain' will continue looking for the key in lower Levels but a remove Range should always return false.
    *
    * 2. Any other Range - a flag is added to Appendix indicating that the Segment contains a Range key-value so that
    * Segment reads can take appropriate steps to fetch the right range key-value.
    */
  def write(keyValues: Iterable[KeyValue.WriteOnly],
            createdInLevel: Int,
            segmentCompressions: Seq[CompressionInternal]): IO[ClosedSegment] =
    if (keyValues.isEmpty)
      ClosedSegment.emptyIO
    else {
      val sortedIndex = SortedIndex.init(keyValues = keyValues)
      val values = Values.init(keyValues = keyValues)
      val hashIndex = HashIndex.init(keyValues = keyValues)
      val binarySearchIndex = BinarySearchIndex.init(keyValues = keyValues)
      val bloomFilter = BloomFilter.init(keyValues = keyValues)

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
        closeResult =>
          close(
            closeResult = closeResult,
            keyValues = keyValues,
            createdInLevel = createdInLevel,
            segmentCompressions = segmentCompressions
          )
      }
    }

  def close(closeResult: ClosedBlocks,
            keyValues: Iterable[KeyValue.WriteOnly],
            createdInLevel: Int,
            segmentCompressions: Seq[CompressionInternal]): IO[ClosedSegment] = {
    IO {
      val lastStats: Stats = keyValues.last.stats

      val values = closeResult.values
      val sortedIndex = closeResult.sortedIndex
      val hashIndex = closeResult.hashIndex
      val binarySearchIndex = closeResult.binarySearchIndex
      val bloomFilter = closeResult.bloomFilter

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

      val headerSize = SegmentBlock.headerSize(segmentCompressions.nonEmpty)
      val headerBytes = Slice.create[Byte](headerSize)
      //set header bytes to be fully written so that it does not closed when compression.
      headerBytes moveWritePosition headerBytes.allocatedSize

      ClosedSegment(
        headerBytes = headerBytes,
        values = values.map(_.bytes.close()),
        sortedIndex = sortedIndex.bytes.close(),
        hashIndex = hashIndex map (_.bytes.close()),
        binarySearchIndex = binarySearchIndex map (_.bytes.close()),
        bloomFilter = bloomFilter map (_.bytes.close()),
        footer = segmentFooterSlice.close(),
        functionMinMax = closeResult.minMaxFunction,
        nearestDeadline = closeResult.nearestDeadline
      )
    } flatMap {
      closedSegment =>
        val closed =
          Block.create(
            headerSize = closedSegment.segmentBytes.head.size,
            closedSegment = closedSegment,
            compressions = segmentCompressions,
            blockName = blockName
          )
        closed
    }
  }
}

private[core] case class SegmentBlock(offset: SegmentBlock.Offset,
                                      headerSize: Int,
                                      compressionInfo: Option[Block.CompressionInfo]) extends Block {

  override def createBlockReader(segmentReader: BlockReader[SegmentBlock]): BlockReader[SegmentBlock] =
    segmentReader

  def createBlockReader(segmentReader: Reader): BlockReader[SegmentBlock] =
    BlockReader(
      reader = segmentReader,
      block = this
    )

  def clear(): SegmentBlock =
    copy(compressionInfo = compressionInfo.map(_.clear()))

  override def updateOffset(start: Int, size: Int): Block =
    copy(offset = SegmentBlock.Offset(start = start, size = size))
}
