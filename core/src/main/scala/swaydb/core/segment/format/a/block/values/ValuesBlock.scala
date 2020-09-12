/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.values

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.compression.CompressionInternal
import swaydb.core.data.Memory
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{Block, BlockOffset, BlockOps}
import swaydb.core.segment.format.a.entry.writer.EntryWriter
import swaydb.core.segment.merge.MergeStats
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.{Sliced, _}
import swaydb.data.util.Functions

private[core] object ValuesBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

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
        ioStrategy = Functions.safe(IOStrategy.defaultSynchronised, enable.blockIOStrategy),
        compressions =
          Functions.safe(
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

  class State(var compressibleBytes: Sliced[Byte],
              val cacheableBytes: Sliced[Byte],
              var header: Sliced[Byte],
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

  def init(keyValues: MergeStats.Persistent.Closed[Iterable],
           valuesConfig: ValuesBlock.Config,
           //the builder created by SortedIndex.
           builder: EntryWriter.Builder): Option[ValuesBlock.State] = {
    val totalValuesSize = keyValues.totalValuesSize
    if (totalValuesSize > 0) {
      val bytes = Slice.create[Byte](totalValuesSize)
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

  def init(bytes: Sliced[Byte],
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

  def read(header: Block.Header[ValuesBlock.Offset]): ValuesBlock =
    ValuesBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )

  def read(fromOffset: Int, length: Int, reader: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Option[Sliced[Byte]] =
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

    override def readBlock(header: Block.Header[Offset]): ValuesBlock =
      ValuesBlock.read(header)
  }
}

private[core] case class ValuesBlock(offset: ValuesBlock.Offset,
                                     headerSize: Int,
                                     compressionInfo: Option[Block.CompressionInfo]) extends Block[ValuesBlock.Offset]

