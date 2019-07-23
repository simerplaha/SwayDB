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
import swaydb.compression.CompressionInternal
import swaydb.core.data.Transient
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.{Bytes, FunctionUtil}
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.slice.Slice
import swaydb.ErrorHandler.CoreErrorHandler

private[core] object ValuesBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  val empty =
    ValuesBlock(ValuesBlock.Offset.zero, 0, None)

  val emptyUnblocked: UnblockedReader[ValuesBlock.Offset, ValuesBlock] =
    UnblockedReader.empty[ValuesBlock.Offset, ValuesBlock](ValuesBlock.empty)(ValuesBlockOps)

  val emptyUnblockedIO: IO[IO.Error, UnblockedReader[Offset, ValuesBlock]] =
    IO(emptyUnblocked)

  def unblocked(bytes: Slice[Byte])(implicit blockOps: BlockOps[ValuesBlock.Offset, ValuesBlock]): UnblockedReader[ValuesBlock.Offset, ValuesBlock] =
    UnblockedReader(
      bytes = bytes,
      block = ValuesBlock(ValuesBlock.Offset(0, bytes.size), 0, None)
    )

  object Config {

    val disabled =
      ValuesBlock.Config(
        compressDuplicateValues = false,
        compressDuplicateRangeValues = false,
        blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(enable: swaydb.data.config.ValuesConfig): Config =
      Config(
        compressDuplicateValues = enable.compressDuplicateValues,
        compressDuplicateRangeValues = enable.compressDuplicateRangeValues,
        blockIO = FunctionUtil.safe(IOStrategy.defaultSynchronisedStoredIfCompressed, enable.ioStrategy),
        compressions =
          FunctionUtil.safe(
            default = _ => Seq.empty[CompressionInternal],
            function = enable.compression(_) map CompressionInternal.apply
          )
      )
  }

  case class Config(compressDuplicateValues: Boolean,
                    compressDuplicateRangeValues: Boolean,
                    blockIO: IOAction => IOStrategy,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  def valuesBlockNotInitialised: IO.Failure[IO.Error, Nothing] =
    IO.Failure(IO.Error.Fatal("Value block not initialised."))

  case class State(var _bytes: Slice[Byte],
                   headerSize: Int,
                   compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {
    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes
  }

  object Offset {
    val zero = Offset(0, 0)
  }

  case class Offset(start: Int, size: Int) extends BlockOffset

  val hasCompressionHeaderSize = {
    val size = Block.headerSize(true)
    Bytes.sizeOf(size) +
      size
  }

  val noCompressionHeaderSize = {
    val size = Block.headerSize(false)
    Bytes.sizeOf(size) +
      size
  }

  def headerSize(hasCompression: Boolean): Int =
    if (hasCompression)
      hasCompressionHeaderSize
    else
      noCompressionHeaderSize

  def init(keyValues: Iterable[Transient]): Option[ValuesBlock.State] =
    if (keyValues.last.stats.segmentValuesSize > 0) {
      val hasCompression = keyValues.last.valuesConfig.compressions(UncompressedBlockInfo(keyValues.last.stats.segmentValuesSize)).nonEmpty
      val headSize = headerSize(hasCompression)
      val bytes =
        if (hasCompression) //stats calculate size with no compression if there is compression add remaining bytes.
          Slice.create[Byte](keyValues.last.stats.segmentValuesSize - noCompressionHeaderSize + headSize)
        else
          Slice.create[Byte](keyValues.last.stats.segmentValuesSize)

      bytes moveWritePosition headSize

      Some(
        ValuesBlock.State(
          _bytes = bytes,
          headerSize = headSize,
          compressions =
            //cannot have no compression to begin with a then have compression because that upsets the total bytes required.
            if (hasCompression)
              keyValues.last.valuesConfig.compressions
            else
              _ => Seq.empty
        )
      )
    }
    else
      None

  def write(keyValue: Transient, state: ValuesBlock.State) =
    IO {
      keyValue.valueEntryBytes foreach state.bytes.addAll
    }

  def close(state: State): IO[IO.Error, State] =
    Block.block(
      headerSize = state.headerSize,
      bytes = state.bytes,
      compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
      blockName = blockName
    ) flatMap {
      compressedOrUncompressedBytes =>
        IO {
          state.bytes = compressedOrUncompressedBytes
          if (state.bytes.currentWritePosition > state.headerSize)
            throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
          state
        }
    }

  def read(header: Block.Header[ValuesBlock.Offset]): ValuesBlock =
    ValuesBlock(
      offset = header.offset,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )

  def read(fromOffset: Int, length: Int, reader: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): IO[IO.Error, Option[Slice[Byte]]] =
    if (length == 0)
      IO.none
    else
      reader
        .moveTo(fromOffset)
        .read(length)
        .map(Some(_))
        .recoverWith[IO.Error, Option[Slice[Byte]]] {
          case error =>
            error.exception match {
              case exception @ (_: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException) =>
                IO.Failure(
                  IO.Error.Fatal(
                    SegmentCorruptionException(
                      message = s"Corrupted Segment: Failed to value bytes of length $length from offset $fromOffset",
                      cause = exception
                    )
                  )
                )

              case ex: Exception =>
                IO.Failure(ex)
            }
        }

  implicit object ValuesBlockOps extends BlockOps[ValuesBlock.Offset, ValuesBlock] {
    override def updateBlockOffset(block: ValuesBlock, start: Int, size: Int): ValuesBlock =
      try {
        block.copy(offset = createOffset(start = start, size = size))
      } catch {
        case exception: Exception =>
          throw exception
      }

    override def createOffset(start: Int, size: Int): Offset =
      ValuesBlock.Offset(start = start, size = size)

    override def readBlock(header: Block.Header[Offset]): IO[IO.Error, ValuesBlock] =
      IO(ValuesBlock.read(header))
  }
}

private[core] case class ValuesBlock(offset: ValuesBlock.Offset,
                                     headerSize: Int,
                                     compressionInfo: Option[Block.CompressionInfo]) extends Block[ValuesBlock.Offset]

