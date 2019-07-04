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
import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.OffsetBase
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

object Values {

  object Config {

    val disabled =
      Values.Config(
        compressDuplicateValues = false,
        compressDuplicateRangeValues = false,
        cacheOnAccess = false,
        hasCompression = false
      )

    def apply(config: swaydb.data.config.ValuesConfig): Config =
      Config(
        compressDuplicateValues = config.compressDuplicateValues,
        compressDuplicateRangeValues = config.compressDuplicateRangeValues,
        cacheOnAccess = config.cacheOnAccess,
        hasCompression = config.compression.nonEmpty
      )
  }

  case class Config(compressDuplicateValues: Boolean,
                    compressDuplicateRangeValues: Boolean,
                    cacheOnAccess: Boolean,
                    hasCompression: Boolean)

  def valueNotFound: IO.Failure[Nothing] =
    IO.Failure(IO.Error.Fatal("Value not found."))

  def valueSliceNotInitialised: IO.Failure[Nothing] =
    IO.Failure(IO.Error.Fatal("Value slice not initialised."))

  val empty =
    Values(Values.Offset.zero, 0, None)

  case class State(var _bytes: Slice[Byte],
                   headerSize: Int,
                   compressions: Seq[CompressionInternal]) {
    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes
  }

  object Offset {
    val zero = Offset(0, 0)
  }

  case class Offset(start: Int, size: Int) extends OffsetBase

  def headerSize(hasCompression: Boolean): Int = {
    val size = Block.headerSize(hasCompression)
    Bytes.sizeOf(size) +
      size
  }

  def init(keyValues: Iterable[KeyValue.WriteOnly],
           compressions: Seq[CompressionInternal]): Option[Values.State] = {
    val headSize = headerSize(compressions.nonEmpty)
    if (keyValues.last.stats.segmentValuesSize > headSize) {
      val bytes = Slice.create[Byte](keyValues.last.stats.segmentValuesSize)
      bytes moveWritePosition headSize
      Some(
        Values.State(
          _bytes = bytes,
          headerSize = headSize,
          compressions = compressions
        )
      )
    }
    else
      None
  }

  def write(keyValue: KeyValue.WriteOnly, state: Values.State) =
    IO {
      keyValue.valueEntryBytes foreach state.bytes.addAll
    }

  def close(state: State): IO[State] =
    Block.create(
      headerSize = state.headerSize,
      bytes = state.bytes,
      compressions = state.compressions
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
           segmentReader: Reader): IO[Values] =
    Block.readHeader(offset = offset, segmentReader = segmentReader) map {
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

case class Values(offset: Values.Offset,
                  headerSize: Int,
                  compressionInfo: Option[Block.CompressionInfo]) extends Block {

  override def createBlockReader(bytes: Slice[Byte]): BlockReader[Values] =
    createBlockReader(Reader(bytes))

  def createBlockReader(segmentReader: Reader): BlockReader[Values] =
    BlockReader(
      reader = segmentReader,
      block = this
    )

  override def updateOffset(start: Int, size: Int): Block =
    copy(offset = Values.Offset(start = start, size = size))
}
