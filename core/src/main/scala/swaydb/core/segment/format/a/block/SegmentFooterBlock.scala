/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block
import swaydb.core.segment.format.a.block.Block.CompressionInfo
import swaydb.core.segment.format.a.block.SegmentBlock.ClosedBlocks
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.merge.MergeStats
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

object SegmentFooterBlock {
  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {
    def default =
      Config(
        blockIO = IOStrategy.synchronisedStoredIfCompressed
      )
  }

  case class Config(blockIO: IOAction => IOStrategy)

  case class Offset(start: Int, size: Int) extends BlockOffset

  val optimalBytesRequired =
    Block.minimumHeaderSize(false) +
      Bytes.sizeOfUnsignedInt(Block.minimumHeaderSize(false)) +
      ByteSizeOf.byte + //1 byte for format
      ByteSizeOf.varInt + //created in level
      ByteSizeOf.varInt + //numberOfRanges
      ByteSizeOf.boolean + //hasPut
      ByteSizeOf.varInt + //key-values count
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
                   var bytes: Slice[Byte],
                   keyValuesCount: Int,
                   numberOfRanges: Int,
                   hasPut: Boolean)

  def init(keyValuesCount: Int,
           rangesCount: Int,
           hasPut: Boolean,
           createdInLevel: Int): SegmentFooterBlock.State =
    SegmentFooterBlock.State(
      footerSize = Block.minimumHeaderSize(false),
      createdInLevel = createdInLevel,
      keyValuesCount = keyValuesCount,
      bytes = Slice.create[Byte](optimalBytesRequired),
      numberOfRanges = rangesCount,
      hasPut = hasPut
    )

  def writeAndClose(state: State, closedBlocks: ClosedBlocks): State = {
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
    footerBytes addUnsignedInt SegmentBlock.formatId
    footerBytes addUnsignedInt state.createdInLevel
    footerBytes addUnsignedInt state.numberOfRanges
    footerBytes addBoolean state.hasPut
    //here the top Level key-values are used instead of Group's internal key-values because Group's internal key-values
    //are read when the Group key-value is read.
    footerBytes addUnsignedInt state.keyValuesCount

    val valuesSize =
      if (values.isDefined)
        values.get.header.size + values.get.compressibleBytes.size
      else
        0

    var currentBlockOffset = valuesSize

    val sortedIndexSize = sortedIndex.blockSize

    footerBytes addUnsignedInt sortedIndexSize
    footerBytes addUnsignedInt currentBlockOffset
    currentBlockOffset = currentBlockOffset + sortedIndexSize

    val hashIndexSize: Int =
      hashIndex match {
        case Some(hashIndex) =>
          val hashIndexSize = hashIndex.blockSize
          footerBytes addUnsignedInt hashIndexSize
          footerBytes addUnsignedInt currentBlockOffset
          currentBlockOffset = currentBlockOffset + hashIndexSize
          hashIndexSize

        case None =>
          footerBytes addUnsignedInt 0
          0
      }

    val binarySearchIndexSize =
      binarySearchIndex match {
        case Some(binarySearchIndex) =>
          val binarySearchIndexSize = binarySearchIndex.blockSize
          footerBytes addUnsignedInt binarySearchIndexSize
          footerBytes addUnsignedInt currentBlockOffset
          currentBlockOffset = currentBlockOffset + binarySearchIndexSize
          binarySearchIndexSize

        case None =>
          footerBytes addUnsignedInt 0
          0
      }

    val bloomFilterSize =
      bloomFilter match {
        case Some(bloomFilter) =>
          val bloomFilterSize = bloomFilter.blockSize
          footerBytes addUnsignedInt bloomFilterSize
          footerBytes addUnsignedInt currentBlockOffset
          currentBlockOffset = currentBlockOffset + bloomFilterSize
          bloomFilterSize

        case None =>
          footerBytes addUnsignedInt 0
          0
      }

    val footerOffset =
      valuesSize +
        sortedIndexSize +
        hashIndexSize +
        binarySearchIndexSize +
        bloomFilterSize

    footerBytes addInt footerOffset
    footerBytes addLong CRC32.forBytes(footerBytes)

    state.bytes = state.bytes.close()
    state
  }

  //all these functions are wrapper with a try catch block with value only to make it easier to read.
  def read(reader: UnblockedReader[SegmentBlock.Offset, SegmentBlock]): SegmentFooterBlock = {
    val segmentBlockSize = reader.size.toInt
    val approximateFooterOffset = (segmentBlockSize - SegmentFooterBlock.optimalBytesRequired) max 0
    val fullFooterBytes = Reader(reader.moveTo(approximateFooterOffset).readRemaining())
    val footerOffsetAndCrc = fullFooterBytes.moveTo(fullFooterBytes.size - (ByteSizeOf.int + ByteSizeOf.long))
    val footerStartOffset = footerOffsetAndCrc.readInt()
    val expectedCRC = footerOffsetAndCrc.readLong()
    val footerSize = segmentBlockSize - footerStartOffset
    val footerBytes = fullFooterBytes.moveTo(fullFooterBytes.size - footerSize).readRemaining()
    val actualCRC = CRC32.forBytes(footerBytes dropRight ByteSizeOf.long) //drop crc bytes.
    if (expectedCRC != actualCRC)
      throw IO.throwable(s"Corrupted Segment: CRC Check failed. $expectedCRC != $actualCRC")
    else
      readCRCPassed(
        footerStartOffset = footerStartOffset,
        footerSize = footerSize,
        footerBytes = footerBytes
      )
  }

  def readCRCPassed(footerStartOffset: Int, footerSize: Int, footerBytes: Slice[Byte]) = {
    val footerReader = Reader(footerBytes)
    val formatId = footerReader.readUnsignedInt()
    if (formatId != SegmentBlock.formatId) {
      throw IO.throwable(message = s"Invalid Segment formatId: $formatId. Expected: ${SegmentBlock.formatId}")
    } else {
      val createdInLevel = footerReader.readUnsignedInt()
      val numberOfRanges = footerReader.readUnsignedInt()
      val hasPut = footerReader.readBoolean()
      val keyValueCount = footerReader.readUnsignedInt()

      val sortedIndexOffset =
        SortedIndexBlock.Offset(
          size = footerReader.readUnsignedInt(),
          start = footerReader.readUnsignedInt()
        )

      val hashIndexSize = footerReader.readUnsignedInt()
      val hashIndexOffset =
        if (hashIndexSize == 0)
          None
        else
          Some(
            HashIndexBlock.Offset(
              start = footerReader.readUnsignedInt(),
              size = hashIndexSize
            )
          )

      val binarySearchIndexSize = footerReader.readUnsignedInt()
      val binarySearchIndexOffset =
        if (binarySearchIndexSize == 0)
          None
        else
          Some(
            BinarySearchIndexBlock.Offset(
              start = footerReader.readUnsignedInt(),
              size = binarySearchIndexSize
            )
          )

      val bloomFilterSize = footerReader.readUnsignedInt()
      val bloomFilterOffset =
        if (bloomFilterSize == 0)
          None
        else
          Some(
            BloomFilterBlock.Offset(
              start = footerReader.readUnsignedInt(),
              size = bloomFilterSize
            )
          )

      val valuesOffset =
        if (sortedIndexOffset.start == 0)
          None
        else
          Some(ValuesBlock.Offset(0, sortedIndexOffset.start))

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
        numberOfRanges = numberOfRanges,
        hasPut = hasPut
      )
    }
  }
  implicit object SegmentFooterBlockOps extends BlockOps[SegmentFooterBlock.Offset, SegmentFooterBlock] {
    override def updateBlockOffset(block: SegmentFooterBlock, start: Int, size: Int): SegmentFooterBlock =
      block.copy(offset = createOffset(start, size))

    override def createOffset(start: Int, size: Int): Offset =
      SegmentFooterBlock.Offset(start, size)

    override def readBlock(header: Block.Header[Offset]): SegmentFooterBlock =
      throw IO.throwable("Footers do not have block header readers.")
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
                              numberOfRanges: Int,
                              hasPut: Boolean) extends Block[block.SegmentFooterBlock.Offset] {
  def hasRange =
    numberOfRanges > 0
}
