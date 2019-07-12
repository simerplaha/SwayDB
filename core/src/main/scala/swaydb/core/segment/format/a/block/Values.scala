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
import swaydb.core.data.KeyValue
import swaydb.core.io.reader.BlockReader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.block.BinarySearchIndex.Config.defaultBlockIO
import swaydb.core.util.{Bytes, FunctionUtil}
import swaydb.data.IO
import swaydb.data.config.{BlockIO, BlockInfo, UncompressedBlockInfo}
import swaydb.data.slice.Slice

private[core] object Values {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {

    val disabled =
      Values.Config(
        compressDuplicateValues = false,
        compressDuplicateRangeValues = false,
        blockIO = blockInfo => BlockIO.SynchronisedIO(cacheOnAccess = blockInfo.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(enable: swaydb.data.config.ValuesConfig): Config =
      Config(
        compressDuplicateValues = enable.compressDuplicateValues,
        compressDuplicateRangeValues = enable.compressDuplicateRangeValues,
        blockIO = FunctionUtil.safe(defaultBlockIO, enable.blockIO),
        compressions =
          FunctionUtil.safe(
            default = _ => Seq.empty[CompressionInternal],
            function = enable.compression(_) map CompressionInternal.apply
          )
      )
  }

  case class Config(compressDuplicateValues: Boolean,
                    compressDuplicateRangeValues: Boolean,
                    blockIO: BlockInfo => BlockIO,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  def valuesBlockNotInitialised: IO.Failure[Nothing] =
    IO.Failure(IO.Error.Fatal("Value block not initialised."))

  val empty =
    Values(Values.Offset.zero, 0, None)

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

  def init(keyValues: Iterable[KeyValue.WriteOnly]): Option[Values.State] =
    if (keyValues.last.stats.segmentValuesSize > 0) {
      val headSize = headerSize(keyValues.last.valuesConfig.compressions(UncompressedBlockInfo(keyValues.last.stats.segmentValuesSize)).nonEmpty)
      val bytes = Slice.create[Byte](keyValues.last.stats.segmentValuesSize)
      bytes moveWritePosition headSize
      Some(
        Values.State(
          _bytes = bytes,
          headerSize = headSize,
          compressions = keyValues.last.valuesConfig.compressions
        )
      )
    }
    else
      None

  def write(keyValue: KeyValue.WriteOnly, state: Values.State) =
    IO {
      keyValue.valueEntryBytes foreach state.bytes.addAll
    }

  def close(state: State): IO[State] =
    Block.create(
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

  def read(offset: Values.Offset,
           segmentReader: BlockReader[SegmentBlock]): IO[Values] =
    Block.readHeader(offset = offset, reader = segmentReader) map {
      result =>
        Values(
          offset = offset,
          headerSize = result.headerSize,
          compressionInfo = result.compressionInfo
        )
    }

  def read(fromOffset: Int, length: Int, reader: BlockReader[Values]): IO[Option[Slice[Byte]]] =
    if (length == 0)
      IO.none
    else
      reader
        .moveTo(fromOffset)
        .read(length)
        .map(Some(_))
        .recoverWith {
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
}

private[core] case class Values(offset: Values.Offset,
                                headerSize: Int,
                                compressionInfo: Option[Block.CompressionInfo]) extends Block {

  override def updateOffset(start: Int, size: Int): Block =
    copy(offset = Values.Offset(start = start, size = size))
}
