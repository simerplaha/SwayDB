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

import swaydb.compression.DecompressorInternal
import swaydb.core.segment.format.a.OffsetBase
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.{IO, Reserve}

object BlockDecompressor {

  class State(val decompressor: DecompressorInternal,
              val decompressedLength: Int,
              private[BlockDecompressor] val reserve: Reserve[Unit],
              @volatile private var _decompressedBytes: Option[Slice[Byte]]) {
    def decompressedBytes = _decompressedBytes

    def decompressedBytes_=(bytes: Slice[Byte]) =
      this._decompressedBytes = Some(bytes)

    def isBusy =
      reserve.isBusy
  }

  def apply(decompressor: DecompressorInternal,
            decompressedLength: Int) =
    new State(
      decompressor = decompressor,
      decompressedLength = decompressedLength,
      reserve = Reserve(),
      _decompressedBytes = None
    )

  def decompress(blockDecompressor: State,
                 compressedReader: Reader,
                 uncompressedHeaderBytes: Int,
                 offset: OffsetBase): IO[Slice[Byte]] =
    decompress(
      blockDecompressor = blockDecompressor,
      compressedReader = compressedReader,
      offset = new OffsetBase {
        override def start: Int = offset.start + uncompressedHeaderBytes
        override def size: Int = offset.size - uncompressedHeaderBytes
      }
    )

  def decompress(blockDecompressor: State,
                 compressedReader: Reader,
                 offset: OffsetBase): IO[Slice[Byte]] =
    blockDecompressor.decompressedBytes map {
      decompressedBytes =>
        IO.Success(decompressedBytes)
    } getOrElse {
      if (Reserve.setBusyOrGet((), blockDecompressor.reserve).isEmpty)
        try
          compressedReader.moveTo(offset.start).read(offset.size) flatMap {
            compressedBytes =>
              blockDecompressor.decompressor.decompress(
                slice = compressedBytes,
                decompressLength = blockDecompressor.decompressedLength
              ) map {
                decompressedBytes =>
                  blockDecompressor.decompressedBytes = decompressedBytes
                  decompressedBytes
              }
          }
        finally
          Reserve.setFree(blockDecompressor.reserve)
      else
        IO.Failure(IO.Error.DecompressingValues(blockDecompressor.reserve))
    }
}
