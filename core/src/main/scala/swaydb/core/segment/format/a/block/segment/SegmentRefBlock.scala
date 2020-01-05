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

package swaydb.core.segment.format.a.block.segment

import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{Block, BlockOffset}
import swaydb.core.segment.{SegmentRef, SegmentRefOptional}
import swaydb.core.util.{Bytes, SkipList}
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.data.util.ByteSizeOf

object SegmentRefBlock {
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

  def writeAndClose(closedBlocks: Iterable[TransientSegment]): Unit = {

    //    val values = closedBlocks.values
    //    val sortedIndex = closedBlocks.sortedIndex
    //    val hashIndex = closedBlocks.hashIndex
    //    val binarySearchIndex = closedBlocks.binarySearchIndex
    //    val bloomFilter = closedBlocks.bloomFilter
    //
    //    val footerBytes = state.bytes
    //    //this is a placeholder to store the format type of the Segment file written.
    //    //currently there is only one format. So this is hardcoded but if there are a new file format then
    //    //SegmentWriter and SegmentReader should be changed to be type classes with unique format types ids.
    //    //the following group of bytes are also used for CRC check.
    //    footerBytes addUnsignedInt SegmentBlock.formatId
    //    footerBytes addUnsignedInt state.createdInLevel
    //    footerBytes addUnsignedInt state.numberOfRanges
    //    footerBytes addBoolean state.hasPut
    //    //here the top Level key-values are used instead of Group's internal key-values because Group's internal key-values
    //    //are read when the Group key-value is read.
    //    footerBytes addUnsignedInt state.keyValuesCount
    //
    //    val valuesSize =
    //      if (values.isDefined)
    //        values.get.header.size + values.get.compressibleBytes.size
    //      else
    //        0
    //
    //    var currentBlockOffset = valuesSize
    //
    //    val sortedIndexSize = sortedIndex.blockSize
    //
    //    footerBytes addUnsignedInt sortedIndexSize
    //    footerBytes addUnsignedInt currentBlockOffset
    //    currentBlockOffset = currentBlockOffset + sortedIndexSize
    //
    //    val hashIndexSize: Int =
    //      hashIndex match {
    //        case Some(hashIndex) =>
    //          val hashIndexSize = hashIndex.blockSize
    //          footerBytes addUnsignedInt hashIndexSize
    //          footerBytes addUnsignedInt currentBlockOffset
    //          currentBlockOffset = currentBlockOffset + hashIndexSize
    //          hashIndexSize
    //
    //        case None =>
    //          footerBytes addUnsignedInt 0
    //          0
    //      }
    //
    //    val binarySearchIndexSize =
    //      binarySearchIndex match {
    //        case Some(binarySearchIndex) =>
    //          val binarySearchIndexSize = binarySearchIndex.blockSize
    //          footerBytes addUnsignedInt binarySearchIndexSize
    //          footerBytes addUnsignedInt currentBlockOffset
    //          currentBlockOffset = currentBlockOffset + binarySearchIndexSize
    //          binarySearchIndexSize
    //
    //        case None =>
    //          footerBytes addUnsignedInt 0
    //          0
    //      }
    //
    //    val bloomFilterSize =
    //      bloomFilter match {
    //        case Some(bloomFilter) =>
    //          val bloomFilterSize = bloomFilter.blockSize
    //          footerBytes addUnsignedInt bloomFilterSize
    //          footerBytes addUnsignedInt currentBlockOffset
    //          currentBlockOffset = currentBlockOffset + bloomFilterSize
    //          bloomFilterSize
    //
    //        case None =>
    //          footerBytes addUnsignedInt 0
    //          0
    //      }
    //
    //    val footerOffset =
    //      valuesSize +
    //        sortedIndexSize +
    //        hashIndexSize +
    //        binarySearchIndexSize +
    //        bloomFilterSize
    //
    //    footerBytes addInt footerOffset
    //    footerBytes addLong CRC32.forBytes(footerBytes)
    //
    //    state.bytes = state.bytes.close()
    //    state
    ???
  }

  //all these functions are wrapper with a try catch block with value only to make it easier to read.
  def read(reader: UnblockedReader[SegmentBlock.Offset, SegmentBlock]): SkipList.Immutable[SliceOptional[Byte], SegmentRefOptional, Slice[Byte], SegmentRef] = {
    ???
  }
}
