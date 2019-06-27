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
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.OffsetBase
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

object Values {

  val empty =
    Values(Values.Offset.zero, None)

  case class State(var _bytes: Slice[Byte],
                   compressions: Seq[CompressionInternal]) {
    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes
  }
  object Offset {
    val zero = Offset(0, 0)
  }
  case class Offset(start: Int, size: Int) extends OffsetBase

  val optimalHeaderSize =
    Block.blockCompressionOnlyHeaderSize

  def init(keyValues: Iterable[KeyValue.WriteOnly],
           compressions: Seq[CompressionInternal]): Option[Values.State] =
    if (keyValues.last.stats.segmentValuesSize > 0)
      Some(
        Values.State(
          _bytes = Slice.create[Byte](keyValues.last.stats.segmentValuesSize),
          compressions = compressions
        )
      )
    else
      None

  def write(value: Slice[Byte], state: State) =
    state.bytes addAll value

  def close(state: State) =
    Block.compress(
      headerSize = optimalHeaderSize,
      bytes = state.bytes,
      compressions = state.compressions
    ) flatMap {
      compressedOrUncompressedBytes =>
        IO {
          state.bytes = compressedOrUncompressedBytes
          if (state.bytes.currentWritePosition > optimalHeaderSize)
            throw new Exception(s"Calculated header size was incorrect. Expected: $optimalHeaderSize. Used: ${state.bytes.currentWritePosition - 1}")
        }
    }

  def read(offset: Values.Offset,
           reader: Reader): IO[Values] =
    Block.readHeader(offset = offset, compressedBytes = reader) map {
      result =>
        Values(
          offset =
            if (result.compressionInfo.isDefined)
              offset
            else
              Offset(
                start = offset.start + result.headerSize,
                size = offset.size - result.headerSize
              ),
          block =
            result.compressionInfo
        )
    }

  def getDecompressedValues(reader: Reader, values: Values) =
  //    values
  //      .blockCompression
  //      .map {
  //        blockDecompressor =>
  //          BlockCompression.getDecompressedReader(
  //            blockDecompressor = blockDecompressor,
  //            compressedReader = reader,
  //            offset = values.offset
  //          ) map ((0, _)) //decompressed bytes, offsets not required, set to 0.
  //      }
  //      .getOrElse {
  //        IO.Success((values.offset.start, reader)) //no compression used. Set the offset.
  //      }
  //      .flatMap {
  //        case (startOffset, reader) =>
  //          reader
  //            .copy()
  //            .moveTo(startOffset)
  //            .read(length)
  //            .map(Some(_))
  //      }
  //      .recoverWith {
  //        case error =>
  //          error.exception match {
  //            case exception @ (_: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException) =>
  //              IO.Failure(
  //                IO.Error.Fatal(
  //                  SegmentCorruptionException(
  //                    message = s"Corrupted Segment: Failed to get bytes of length $length from offset $fromOffset",
  //                    cause = exception
  //                  )
  //                )
  //              )
  //
  //            case ex: Exception =>
  //              IO.Failure(ex)
  //          }
  //      }
    ???

  def read(fromOffset: Int, length: Int, reader: Reader): IO[Option[Slice[Byte]]] =
  //    if (length == 0)
  //      IO.none
  //    else
  //      values
  //        .blockCompression
  //        .map {
  //          blockDecompressor =>
  //            BlockCompression.getDecompressedReader(
  //              blockDecompressor = blockDecompressor,
  //              compressedReader = reader,
  //              offset = values.offset
  //            ) map ((0, _)) //decompressed bytes, offsets not required, set to 0.
  //        }
  //        .getOrElse {
  //          IO.Success((values.offset.start + fromOffset, reader)) //no compression used. Set the offset.
  //        }
  //        .flatMap {
  //          case (startOffset, reader) =>
  //            reader
  //              .copy()
  //              .moveTo(startOffset)
  //              .read(length)
  //              .map(Some(_))
  //        }
  //        .recoverWith {
  //          case error =>
  //            error.exception match {
  //              case exception @ (_: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException) =>
  //                IO.Failure(
  //                  IO.Error.Fatal(
  //                    SegmentCorruptionException(
  //                      message = s"Corrupted Segment: Failed to get bytes of length $length from offset $fromOffset",
  //                      cause = exception
  //                    )
  //                  )
  //                )
  //
  //              case ex: Exception =>
  //                IO.Failure(ex)
  //            }
  //        }
    ???
}

case class Values(offset: Values.Offset,
                  block: Option[Block.CompressionInfo])
