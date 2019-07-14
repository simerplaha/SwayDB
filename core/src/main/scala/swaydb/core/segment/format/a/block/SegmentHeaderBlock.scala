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

import swaydb.core.data.{KeyValue, Stats}
import swaydb.core.segment.format.a.block.SegmentBlock.ClosedBlocks
import swaydb.core.util.{CRC32, FunctionUtil}
import swaydb.data.IO
import swaydb.data.config.{BlockIO, BlockStatus}
import swaydb.data.slice.Slice

object SegmentHeaderBlock {
  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {
    val disabled =
      Config(
        blockIO = BlockIO.default
      )

    def apply(config: swaydb.data.config.SortedKeyIndex): Config =
      config match {
        case config: swaydb.data.config.SortedKeyIndex.Enable =>
          apply(config)
      }

    def apply(enable: swaydb.data.config.SortedKeyIndex.Enable): Config =
      Config(
        blockIO = FunctionUtil.safe(BlockIO.default, enable.blockIO)
      )
  }

  case class Config(blockIO: BlockStatus => BlockIO)

  case class Offset(start: Int, size: Int) extends BlockOffset

  val optimalBytesRequired =
    Stats.segmentFooterSize

  case class State(headerSize: Int,
                   createdInLevel: Int,
                   bytes: Slice[Byte],
                   topLevelKeyValuesCount: Int,
                   uniqueKeyValuesCount: Int,
                   hasGroup: Boolean,
                   hasRange: Boolean,
                   hasPut: Boolean,
                   closedBlocks: SegmentBlock.ClosedBlocks)

  def init(keyValues: Iterable[KeyValue.WriteOnly],
           createdInLevel: Int,
           closedBlocks: ClosedBlocks): SegmentHeaderBlock.State =
    SegmentHeaderBlock.State(
      headerSize = Block.headerSize(false),
      createdInLevel = createdInLevel,
      topLevelKeyValuesCount = keyValues.size,
      uniqueKeyValuesCount = keyValues.last.stats.segmentUniqueKeysCount,
      bytes = Slice.create[Byte](optimalBytesRequired),
      hasGroup = keyValues.last.stats.segmentHasGroup,
      hasRange = keyValues.last.stats.segmentHasRange,
      hasPut = keyValues.last.stats.segmentHasPut,
      closedBlocks = closedBlocks
    )

  def close(state: State): IO[Slice[Byte]] =
    Block.createUncompressedBlock(
      headerSize = state.headerSize,
      bytes = state.bytes
    )

  def write(state: State): IO[State] =
    IO {
      val closedBlocks = state.closedBlocks

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
      segmentFooterSlice addIntUnsigned state.createdInLevel
      segmentFooterSlice addBoolean state.hasGroup
      segmentFooterSlice addBoolean state.hasRange
      segmentFooterSlice addBoolean state.hasPut
      //here the top Level key-values are used instead of Group's internal key-values because Group's internal key-values
      //are read when the Group key-value is read.
      segmentFooterSlice addIntUnsigned state.topLevelKeyValuesCount
      //total number of actual key-values grouped or un-grouped
      segmentFooterSlice addIntUnsigned state.uniqueKeyValuesCount

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
      state
    }
}
