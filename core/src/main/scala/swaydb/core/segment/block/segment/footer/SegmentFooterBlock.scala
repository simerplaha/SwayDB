/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.block.segment.footer

import swaydb.core.io.reader.Reader
import swaydb.core.segment.block._
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockOffset
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockOffset
import swaydb.core.segment.block.hashindex.HashIndexBlockOffset
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.segment.data.ClosedBlocks
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockOffset}
import swaydb.core.segment.block.sortedindex.SortedIndexBlockOffset
import swaydb.core.segment.block.values.ValuesBlockOffset
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.slice.Slice
import swaydb.utils.ByteSizeOf

private[core] case object SegmentFooterBlock {

  val blockName = this.productPrefix

  val optimalBytesRequired =
    Block.minimumHeaderSize(false) +
      Bytes.sizeOfUnsignedInt(Block.minimumHeaderSize(false)) +
      ByteSizeOf.byte + //1 byte for format
      ByteSizeOf.varInt + //created in level
      ByteSizeOf.varInt + //rangeCount
      ByteSizeOf.varInt + //updateCount
      ByteSizeOf.varInt + //putCount
      ByteSizeOf.varInt + //putDeadlineCount
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

  def init(keyValuesCount: Int,
           rangesCount: Int,
           updateCount: Int,
           putCount: Int,
           putDeadlineCount: Int,
           createdInLevel: Int): SegmentFooterBlockState =
    SegmentFooterBlockState(
      footerSize = Block.minimumHeaderSize(false),
      createdInLevel = createdInLevel,
      bytes = Slice.of[Byte](optimalBytesRequired),
      keyValuesCount = keyValuesCount,
      rangeCount = rangesCount,
      updateCount = updateCount,
      putCount = putCount,
      putDeadlineCount = putDeadlineCount
    )

  def writeAndClose(state: SegmentFooterBlockState, closedBlocks: ClosedBlocks): SegmentFooterBlockState = {
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
    footerBytes addUnsignedInt state.rangeCount
    footerBytes addUnsignedInt state.updateCount
    footerBytes addUnsignedInt state.putCount
    footerBytes addUnsignedInt state.putDeadlineCount
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
  def read(reader: UnblockedReader[SegmentBlockOffset, SegmentBlock]): SegmentFooterBlock = {
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
      throw new Exception(s"Corrupted Segment: CRC Check failed. $expectedCRC != $actualCRC")
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
      throw new Exception(s"Invalid Segment formatId: $formatId. Expected: ${SegmentBlock.formatId}")
    } else {
      val createdInLevel = footerReader.readUnsignedInt()
      val rangeCount = footerReader.readUnsignedInt()
      val updateCount = footerReader.readUnsignedInt()
      val putCount = footerReader.readUnsignedInt()
      val putDeadlineCount = footerReader.readUnsignedInt()
      val keyValueCount = footerReader.readUnsignedInt()

      val sortedIndexOffset =
        SortedIndexBlockOffset(
          size = footerReader.readUnsignedInt(),
          start = footerReader.readUnsignedInt()
        )

      val hashIndexSize = footerReader.readUnsignedInt()
      val hashIndexOffset =
        if (hashIndexSize == 0)
          None
        else
          Some(
            HashIndexBlockOffset(
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
            BinarySearchIndexBlockOffset(
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
            BloomFilterBlockOffset(
              start = footerReader.readUnsignedInt(),
              size = bloomFilterSize
            )
          )

      val valuesOffset =
        if (sortedIndexOffset.start == 0)
          None
        else
          Some(ValuesBlockOffset(0, sortedIndexOffset.start))

      SegmentFooterBlock(
        offset = SegmentFooterBlockOffset(footerStartOffset, footerSize),
        headerSize = 0,
        compressionInfo = None,
        valuesOffset = valuesOffset,
        sortedIndexOffset = sortedIndexOffset,
        hashIndexOffset = hashIndexOffset,
        binarySearchIndexOffset = binarySearchIndexOffset,
        bloomFilterOffset = bloomFilterOffset,
        keyValueCount = keyValueCount,
        createdInLevel = createdInLevel,
        rangeCount = rangeCount,
        updateCount = updateCount,
        putCount = putCount,
        putDeadlineCount = putDeadlineCount
      )
    }
  }

}

case class SegmentFooterBlock(offset: SegmentFooterBlockOffset,
                              headerSize: Int,
                              compressionInfo: Option[BlockCompressionInfo],
                              valuesOffset: Option[ValuesBlockOffset],
                              sortedIndexOffset: SortedIndexBlockOffset,
                              hashIndexOffset: Option[HashIndexBlockOffset],
                              binarySearchIndexOffset: Option[BinarySearchIndexBlockOffset],
                              bloomFilterOffset: Option[BloomFilterBlockOffset],
                              keyValueCount: Int,
                              createdInLevel: Int,
                              rangeCount: Int,
                              updateCount: Int,
                              putCount: Int,
                              putDeadlineCount: Int) extends Block[SegmentFooterBlockOffset] {
  def hasRange =
    rangeCount > 0
}
