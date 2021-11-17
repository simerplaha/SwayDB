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

package swaydb.core.segment.block.values

import swaydb.IO
import swaydb.core.segment.data.Memory
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.block._
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.config.UncompressedBlockInfo
import swaydb.slice.{Slice, SliceMut}

private[core] case object ValuesBlock {

  val blockName = this.productPrefix

  def valuesBlockNotInitialised: IO.Left[swaydb.Error.Segment, Nothing] =
    IO.Left(swaydb.Error.Fatal("Value block not initialised."))

  //  val headerSize = {
  //    val size = Block.headerSize(true)
  //    Bytes.sizeOfUnsignedInt(size) +
  //      size
  //  }

  def init(stats: MergeStats.Persistent.ClosedStatsOnly,
           valuesConfig: ValuesBlockConfig,
           //the builder created by SortedIndex.
           builder: EntryWriter.Builder): Option[ValuesBlockState] = {
    val totalValuesSize = stats.totalValuesSize
    if (totalValuesSize > 0) {
      val bytes = Slice.of[Byte](totalValuesSize)
      val state =
        new ValuesBlockState(
          compressibleBytes = bytes,
          cacheableBytes = bytes,
          header = null,
          compressions = valuesConfig.compressions,
          builder = builder
        )
      Some(state)
    }
    else
      None
  }

  def init(bytes: SliceMut[Byte],
           valuesConfig: ValuesBlockConfig,
           //the builder created by SortedIndex.
           builder: EntryWriter.Builder): ValuesBlockState =
    new ValuesBlockState(
      compressibleBytes = bytes,
      cacheableBytes = bytes,
      header = null,
      compressions = valuesConfig.compressions,
      builder = builder
    )

  def write(keyValue: Memory, state: ValuesBlockState) =
    if (state.builder.isValueFullyCompressed)
      state.builder.isValueFullyCompressed = false
    else
      keyValue.value foreachC state.compressibleBytes.addAll

  def close(state: ValuesBlockState): ValuesBlockState = {
    val compressionResult =
      Block.compress(
        bytes = state.compressibleBytes,
        dataBlocksHeaderByteSize = 0,
        compressions = state.compressions(UncompressedBlockInfo(state.compressibleBytes.size)),
        blockName = blockName
      )

    compressionResult.compressedBytes foreachC (slice => state.compressibleBytes = slice.asMut())

    assert(compressionResult.headerBytes.isOriginalFullSlice)
    state.header = compressionResult.headerBytes

    //    if (state.bytes.currentWritePosition > state.bytes.fromOffset + headerSize)
    //      throw new Exception(s"Calculated header size was incorrect. Expected: $headerSize. Used: ${state.bytes.currentWritePosition - 1}")

    state
  }

  def unblockedReader(closedState: ValuesBlockState): UnblockedReader[ValuesBlockOffset, ValuesBlock] = {
    val block =
      ValuesBlock(
        offset = ValuesBlockOffset(0, closedState.cacheableBytes.size),
        headerSize = 0,
        compressionInfo = BlockCompressionInfo.Null
      )

    UnblockedReader(
      block = block,
      bytes = closedState.cacheableBytes.close()
    )
  }

  def read(header: BlockHeader[ValuesBlockOffset]): ValuesBlock =
    ValuesBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )

  def read(fromOffset: Int, length: Int, reader: UnblockedReader[ValuesBlockOffset, ValuesBlock]): Option[Slice[Byte]] =
  //    reader
  //      .moveTo(fromOffset)
  //      .read(length)
  //      .map(_.getOrNone())
  //TODO - replace this with above. This code does error check which is used for testing only and not required when deployed.
    if (length == 0)
      None
    else if (fromOffset < 0) {
      throw new Exception(s"Cannot read from negative offset '$fromOffset'.")
    } else {
      val slice =
        reader
          .moveTo(fromOffset)
          .read(length)

      if (slice.size != length)
        throw new Exception(s"Read value bytes != expected. ${slice.size} != $length.")
      else
        Some(slice)
    }
}

private[core] case class ValuesBlock(offset: ValuesBlockOffset,
                                     headerSize: Int,
                                     compressionInfo: BlockCompressionInfoOption) extends Block[ValuesBlockOffset]

