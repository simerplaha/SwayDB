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

import swaydb.IO
import swaydb.core.data.Transient
import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.block
import swaydb.core.segment.format.a.block.Block.CompressionInfo
import swaydb.core.segment.format.a.block.SegmentBlock.ClosedBlocks
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.io.Core
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.data.io.Core.Error.ErrorHandler

object SegmentFooterBlock {
  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {
    def default =
      Config(
        blockIO = IOStrategy.defaultSynchronisedStoredIfCompressed
      )
  }

  case class Config(blockIO: IOAction => IOStrategy)

  case class Offset(start: Int, size: Int) extends BlockOffset

  val optimalBytesRequired =
    Block.headerSize(false) +
      Bytes.sizeOf(Block.headerSize(false)) +
      ByteSizeOf.byte + //1 byte for format
      ByteSizeOf.varInt + //created in level
      ByteSizeOf.varInt + //numberOfGroups
      ByteSizeOf.varInt + //numberOfRanges
      ByteSizeOf.boolean + //hasPut
      ByteSizeOf.varInt + //key-values count
      ByteSizeOf.varInt + //uniqueKeysCount
      ByteSizeOf.long + //CRC. This cannot be unsignedLong because the size of the crc long bytes is not fixed.
      ByteSizeOf.varInt + //sorted index offset.
      ByteSizeOf.varInt + //sorted index size.
      ByteSizeOf.varInt + //hash index offset. HashIndex offset will never be 0 since that's reserved for index is values are none..
      ByteSizeOf.varInt + //hash index size.
      ByteSizeOf.varInt + //binarySearch index
      ByteSizeOf.varInt + //binary index size.
      ByteSizeOf.varInt + //bloomFilter
      ByteSizeOf.varInt + //bloomFilter
      ByteSizeOf.varInt //footer offset.

  case class State(footerSize: Int,
                   createdInLevel: Int,
                   bytes: Slice[Byte],
                   topLevelKeyValuesCount: Int,
                   uniqueKeyValuesCount: Int,
                   numberOfGroups: Int,
                   numberOfRanges: Int,
                   hasPut: Boolean)

  def init(keyValues: Iterable[Transient],
           createdInLevel: Int): SegmentFooterBlock.State =
    SegmentFooterBlock.State(
      footerSize = Block.headerSize(false),
      createdInLevel = createdInLevel,
      topLevelKeyValuesCount = keyValues.size,
      uniqueKeyValuesCount = keyValues.last.stats.segmentUniqueKeysCount,
      bytes = Slice.create[Byte](optimalBytesRequired),
      numberOfGroups = keyValues.last.stats.groupsCount,
      numberOfRanges = keyValues.last.stats.segmentTotalNumberOfRanges,
      hasPut = keyValues.last.stats.segmentHasPut
    )

  def writeAndClose(state: State, closedBlocks: ClosedBlocks): IO[Core.Error, State] =
    IO {
      val values = closedBlocks.values
      val sortedIndex = closedBlocks.sortedIndex
      val hashIndex = closedBlocks.hashIndex
      val binarySearchIndex = closedBlocks.binarySearchIndex
      val bloomFilter = closedBlocks.bloomFilter

      val footerBytes = state.bytes
      //this is a placeholder to store the format type of the Segment file written.
      //currently there is only one format. So this is hardcoded but if there are a new file format then
      //SegmentWriter and SegmentReader should be changed to be type classes with unique format types ids.
      //the following group of bytes are also used for CRC check.
      footerBytes addIntUnsigned SegmentBlock.formatId
      footerBytes addIntUnsigned state.createdInLevel
      footerBytes addIntUnsigned state.numberOfGroups
      footerBytes addIntUnsigned state.numberOfRanges
      footerBytes addBoolean state.hasPut
      //here the top Level key-values are used instead of Group's internal key-values because Group's internal key-values
      //are read when the Group key-value is read.
      footerBytes addIntUnsigned state.topLevelKeyValuesCount
      //total number of actual key-values grouped or un-grouped
      footerBytes addIntUnsigned state.uniqueKeyValuesCount

      //do CRC
      val indexBytesToCRC = footerBytes.take(SegmentBlock.crcBytes)
      assert(indexBytesToCRC.size == SegmentBlock.crcBytes, s"Invalid CRC bytes size: ${indexBytesToCRC.size}. Required: ${SegmentBlock.crcBytes}")
      footerBytes addLong CRC32.forBytes(indexBytesToCRC)

      var currentBlockOffset = values.map(_.bytes.size) getOrElse 0

      footerBytes addIntUnsigned sortedIndex.bytes.size
      footerBytes addIntUnsigned currentBlockOffset
      currentBlockOffset = currentBlockOffset + sortedIndex.bytes.size

      hashIndex map {
        hashIndex =>
          footerBytes addIntUnsigned hashIndex.bytes.size
          footerBytes addIntUnsigned currentBlockOffset
          currentBlockOffset = currentBlockOffset + hashIndex.bytes.size
      } getOrElse {
        footerBytes addIntUnsigned 0
      }

      binarySearchIndex map {
        binarySearchIndex =>
          footerBytes addIntUnsigned binarySearchIndex.bytes.size
          footerBytes addIntUnsigned currentBlockOffset
          currentBlockOffset = currentBlockOffset + binarySearchIndex.bytes.size
      } getOrElse {
        footerBytes addIntUnsigned 0
      }

      bloomFilter map {
        bloomFilter =>
          footerBytes addIntUnsigned bloomFilter.bytes.size
          footerBytes addIntUnsigned currentBlockOffset
          currentBlockOffset = currentBlockOffset + bloomFilter.bytes.size
      } getOrElse {
        footerBytes addIntUnsigned 0
      }

      val footerOffset =
        values.map(_.bytes.size).getOrElse(0) +
          sortedIndex.bytes.size +
          hashIndex.map(_.bytes.size).getOrElse(0) +
          binarySearchIndex.map(_.bytes.size).getOrElse(0) +
          bloomFilter.map(_.bytes.size).getOrElse(0)

      footerBytes addInt footerOffset

      state.bytes.close()
      state
    }

  //all these functions are wrapper with a try catch block with value only to make it easier to read.
  def read(reader: UnblockedReader[SegmentBlock.Offset, SegmentBlock]): IO[Core.Error, SegmentFooterBlock] =
    try {
      val segmentBlockSize = reader.size.get.toInt
      val footerStartOffset = reader.moveTo(segmentBlockSize - ByteSizeOf.int).readInt().get
      val footerSize = segmentBlockSize - footerStartOffset
      val footerBytes = reader.moveTo(footerStartOffset).read(footerSize - ByteSizeOf.int).get
      val footerReader = Reader(footerBytes)
      val formatId = footerReader.readIntUnsigned().get
      if (formatId != SegmentBlock.formatId) {
        val message = s"Invalid Segment formatId: $formatId. Expected: ${SegmentBlock.formatId}"
        return IO.Failure(Core.Error.Fatal(SegmentCorruptionException(message = message, cause = new Exception(message))))
      }
      assert(formatId == SegmentBlock.formatId, s"Invalid Segment formatId: $formatId. Expected: ${SegmentBlock.formatId}")
      val createdInLevel = footerReader.readIntUnsigned().get
      val numberOfGroups = footerReader.readIntUnsigned().get
      val numberOfRanges = footerReader.readIntUnsigned().get
      val hasPut = footerReader.readBoolean().get
      val keyValueCount = footerReader.readIntUnsigned().get
      val bloomFilterItemsCount = footerReader.readIntUnsigned().get
      val expectedCRC = footerReader.readLong().get
      val crcBytes = footerBytes.take(SegmentBlock.crcBytes)
      val crc = CRC32.forBytes(crcBytes)
      if (expectedCRC != crc) {
        IO.failed(SegmentCorruptionException(s"Corrupted Segment: CRC Check failed. $expectedCRC != $crc", new Exception("CRC check failed.")))
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
          SegmentFooterBlock(
            SegmentFooterBlock.Offset(footerStartOffset, footerSize),
            headerSize = 0,
            compressionInfo = None,
            valuesOffset = valuesOffset,
            sortedIndexOffset = sortedIndexOffset,
            hashIndexOffset = hashIndexOffset,
            binarySearchIndexOffset = binarySearchIndexOffset,
            bloomFilterOffset = bloomFilterOffset,
            keyValueCount = keyValueCount,
            createdInLevel = createdInLevel,
            bloomFilterItemsCount = bloomFilterItemsCount,
            numberOfRanges = numberOfRanges,
            numberOfGroups = numberOfGroups,
            hasPut = hasPut
          )
        )
      }
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            IO.Failure(
              Core.Error.Fatal(
                SegmentCorruptionException(
                  message = "Corrupted Segment: Failed to read footer bytes",
                  cause = exception
                )
              )
            )

          case ex: Exception =>
            IO.failed(ex)
        }
    }

  implicit object SegmentFooterBlockUpdated extends BlockOps[SegmentFooterBlock.Offset, SegmentFooterBlock] {
    override def updateBlockOffset(block: SegmentFooterBlock, start: Int, size: Int): SegmentFooterBlock =
      block.copy(offset = createOffset(start, size))

    override def createOffset(start: Int, size: Int): Offset =
      SegmentFooterBlock.Offset(start, size)

    override def readBlock(header: Block.Header[Offset]): IO[Core.Error, SegmentFooterBlock] =
      IO.Failure(Core.Error.Fatal("Footers do not have block header readers."))
  }
}

case class SegmentFooterBlock(offset: SegmentFooterBlock.Offset,
                              headerSize: Int,
                              compressionInfo: Option[CompressionInfo],
                              valuesOffset: Option[ValuesBlock.Offset],
                              sortedIndexOffset: SortedIndexBlock.Offset,
                              hashIndexOffset: Option[HashIndexBlock.Offset],
                              binarySearchIndexOffset: Option[BinarySearchIndexBlock.Offset],
                              bloomFilterOffset: Option[BloomFilterBlock.Offset],
                              keyValueCount: Int,
                              createdInLevel: Int,
                              bloomFilterItemsCount: Int,
                              numberOfRanges: Int,
                              numberOfGroups: Int,
                              hasPut: Boolean) extends Block[block.SegmentFooterBlock.Offset] {
  def hasGroup =
    numberOfGroups > 0

  def hasRange =
    numberOfRanges > 0
}
