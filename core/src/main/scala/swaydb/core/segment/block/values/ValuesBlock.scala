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

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.compression.CompressionInternal
import swaydb.core.data.Memory
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.{Block, BlockCompressionInfo, BlockHeader, BlockOffset, BlockOps}
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.effect.IOStrategy
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.slice.Slice
import swaydb.effect.{IOAction, IOStrategy}
import swaydb.utils.FunctionSafe

private[core] case object ValuesBlock {

  val blockName = this.productPrefix

  val empty =
    ValuesBlock(ValuesBlock.Offset.zero, 0, None)

  val emptyUnblocked: UnblockedReader[ValuesBlock.Offset, ValuesBlock] =
    UnblockedReader.empty[ValuesBlock.Offset, ValuesBlock](ValuesBlock.empty)(ValuesBlockOps)

  val emptyUnblockedIO: IO[swaydb.Error.Segment, UnblockedReader[Offset, ValuesBlock]] =
    IO.Right(emptyUnblocked)

  object Config {

    val disabled =
      ValuesBlock.Config(
        compressDuplicateValues = false,
        compressDuplicateRangeValues = false,
        ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(enable: swaydb.data.config.ValuesConfig): Config =
      Config(
        compressDuplicateValues = enable.compressDuplicateValues,
        compressDuplicateRangeValues = enable.compressDuplicateRangeValues,
        ioStrategy = FunctionSafe.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
        compressions =
          FunctionSafe.safe(
            default = _ => Seq.empty[CompressionInternal],
            function = enable.compression(_) map CompressionInternal.apply
          )
      )
  }

  case class Config(compressDuplicateValues: Boolean,
                    compressDuplicateRangeValues: Boolean,
                    ioStrategy: IOAction => IOStrategy,
                    compressions: UncompressedBlockInfo => Iterable[CompressionInternal])

  def valuesBlockNotInitialised: IO.Left[swaydb.Error.Segment, Nothing] =
    IO.Left(swaydb.Error.Fatal("Value block not initialised."))

  class State(var compressibleBytes: Slice[Byte],
              val cacheableBytes: Slice[Byte],
              var header: Slice[Byte],
              val compressions: UncompressedBlockInfo => Iterable[CompressionInternal],
              val builder: EntryWriter.Builder) {
    def blockSize: Int =
      header.size + compressibleBytes.size

    def blockBytes =
      header ++ compressibleBytes
  }

  object Offset {
    val zero = Offset(0, 0)
  }

  case class Offset(start: Int, size: Int) extends BlockOffset

  //  val headerSize = {
  //    val size = Block.headerSize(true)
  //    Bytes.sizeOfUnsignedInt(size) +
  //      size
  //  }

  def init(stats: MergeStats.Persistent.ClosedStatsOnly,
           valuesConfig: ValuesBlock.Config,
           //the builder created by SortedIndex.
           builder: EntryWriter.Builder): Option[ValuesBlock.State] = {
    val totalValuesSize = stats.totalValuesSize
    if (totalValuesSize > 0) {
      val bytes = Slice.of[Byte](totalValuesSize)
      val state =
        new ValuesBlock.State(
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

  def init(bytes: Slice[Byte],
           valuesConfig: ValuesBlock.Config,
           //the builder created by SortedIndex.
           builder: EntryWriter.Builder): ValuesBlock.State =
    new ValuesBlock.State(
      compressibleBytes = bytes,
      cacheableBytes = bytes,
      header = null,
      compressions = valuesConfig.compressions,
      builder = builder
    )

  def write(keyValue: Memory, state: ValuesBlock.State) =
    if (state.builder.isValueFullyCompressed)
      state.builder.isValueFullyCompressed = false
    else
      keyValue.value foreachC state.compressibleBytes.addAll

  def close(state: State): State = {
    val compressionResult =
      Block.compress(
        bytes = state.compressibleBytes,
        compressions = state.compressions(UncompressedBlockInfo(state.compressibleBytes.size)),
        blockName = blockName
      )

    compressionResult.compressedBytes foreach (state.compressibleBytes = _)

    compressionResult.fixHeaderSize()

    state.header = compressionResult.headerBytes

    //    if (state.bytes.currentWritePosition > state.bytes.fromOffset + headerSize)
    //      throw IO.throwable(s"Calculated header size was incorrect. Expected: $headerSize. Used: ${state.bytes.currentWritePosition - 1}")

    state
  }

  def unblockedReader(closedState: ValuesBlock.State): UnblockedReader[ValuesBlock.Offset, ValuesBlock] = {
    val block =
      ValuesBlock(
        offset = ValuesBlock.Offset(0, closedState.cacheableBytes.size),
        headerSize = 0,
        compressionInfo = None
      )

    UnblockedReader(
      block = block,
      bytes = closedState.cacheableBytes.close()
    )
  }

  def read(header: BlockHeader[ValuesBlock.Offset]): ValuesBlock =
    ValuesBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )

  def read(fromOffset: Int, length: Int, reader: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Option[Slice[Byte]] =
  //    reader
  //      .moveTo(fromOffset)
  //      .read(length)
  //      .map(_.getOrNone())
  //TODO - replace this with above. This code does error check which is used for testing only and not required when deployed.
    if (length == 0)
      None
    else if (fromOffset < 0) {
      throw IO.throwable(s"Cannot read from negative offset '$fromOffset'.")
    } else {
      val slice =
        reader
          .moveTo(fromOffset)
          .read(length)

      if (slice.size != length)
        throw IO.throwable(s"Read value bytes != expected. ${slice.size} != $length.")
      else
        Some(slice)
    }

  implicit object ValuesBlockOps extends BlockOps[ValuesBlock.Offset, ValuesBlock] {
    override def updateBlockOffset(block: ValuesBlock, start: Int, size: Int): ValuesBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      ValuesBlock.Offset(start = start, size = size)

    override def readBlock(header: BlockHeader[Offset]): ValuesBlock =
      ValuesBlock.read(header)
  }
}

private[core] case class ValuesBlock(offset: ValuesBlock.Offset,
                                     headerSize: Int,
                                     compressionInfo: Option[BlockCompressionInfo]) extends Block[ValuesBlock.Offset]

