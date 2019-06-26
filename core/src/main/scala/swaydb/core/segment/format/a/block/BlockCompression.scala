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

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.{CompressionInternal, DecompressorInternal}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.OffsetBase
import swaydb.core.util.Bytes
import swaydb.data.IO._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf
import swaydb.data.{IO, Reserve}

object BlockCompression extends LazyLogging {

  val uncompressedFormatId: Byte = 0.toByte
  val compressedFormatID: Byte = 1.toByte

  case class ReadResult(blockCompression: Option[BlockCompression.State],
                        headerReader: Reader,
                        headerSize: Int)

  class State(val decompressor: DecompressorInternal,
              val decompressedLength: Int,
              val headerSize: Int,
              private[BlockCompression] val reserve: Reserve[Unit],
              @volatile private var _decompressedBytes: Option[Slice[Byte]]) {
    def decompressedBytes = _decompressedBytes

    def decompressedBytes_=(bytes: Slice[Byte]) =
      this._decompressedBytes = Some(bytes)

    def isBusy =
      reserve.isBusy
  }

  //header size required to store block compression information.
  val headerSize =
    ByteSizeOf.byte + //formatId
      ByteSizeOf.byte + //decompressor
      ByteSizeOf.int + 1 //decompressed length. +1 for larger varints

  //header size if the header only contains block compression information.
  val blockCompressionOnlyHeaderSize =
    Bytes.sizeOf(headerSize) + headerSize

  val headerSizeNoCompression =
    ByteSizeOf.byte //formatId

  val headerSizeNoCompressionByteSize =
    Bytes.sizeOf(headerSizeNoCompression)

  def apply(decompressor: DecompressorInternal,
            headerSize: Int,
            decompressedLength: Int) =
    new State(
      decompressor = decompressor,
      decompressedLength = decompressedLength,
      reserve = Reserve(),
      headerSize = headerSize,
      _decompressedBytes = None
    )

  /**
    * Compress the bytes and update the header with the compression information.
    * The bytes should make sure it has enough space to write compression information.
    *
    * Mutation is required here because compression is expensive. Instead of copying and merging we
    * ask the compressor to allocate empty header bytes to the compressed array.
    *
    * Others using this function should ensure that [[headerSize]] is accounted for in the byte size calculations.
    * They should also allocate enough bytes to write the total headerSize.
    */
  def compressAndUpdateHeader(headerSize: Int,
                              bytes: Slice[Byte],
                              compressions: Seq[CompressionInternal]): IO[Slice[Byte]] =
    compressions.untilSome(_.compressor.compress(headerSize, bytes.drop(headerSize))) flatMap {
      case Some((compressedBytes, compression)) =>
        IO {
          compressedBytes moveWritePosition 0
          compressedBytes addIntUnsigned headerSize
          compressedBytes add compressedFormatID
          compressedBytes addIntUnsigned compression.decompressor.id
          compressedBytes addIntUnsigned (bytes.written - headerSize) //decompressed bytes
          compressedBytes
        }

      case None =>
        logger.debug {
          if (compressions.isEmpty)
            s"No compression strategies provided. Storing ${bytes.written}.bytes uncompressed."
          else
            s"Unable to satisfy compression requirement from ${compressions.size} compression strategies. Storing ${bytes.written}.bytes uncompressed."
        }
        IO {
          bytes moveWritePosition 0
          bytes addIntUnsigned headerSize
          bytes add uncompressedFormatId
        }
    }

  private def validateFormatId(formatID: Int) =
    if (formatID != BlockCompression.uncompressedFormatId && formatID != BlockCompression.compressedFormatID)
      IO.Failure(
        IO.Error.Fatal(
          new Exception(s"Invalid formatID: $formatID. Expected: ${BlockCompression.uncompressedFormatId} or ${BlockCompression.compressedFormatID}")
        )
      )
    else
      IO.unit

  private def readBlockCompression(formatID: Int,
                                   headerSize: Int,
                                   reader: Reader): IO[Option[BlockCompression.State]] =
    if (formatID == compressedFormatID)
      for {
        decompressor <- reader.readIntUnsigned() flatMap (DecompressorInternal(_))
        decompressedLength <- reader.readIntUnsigned()
      } yield
        Some(
          BlockCompression(
            decompressor = decompressor,
            headerSize = headerSize,
            decompressedLength = decompressedLength
          )
        )
    else
      IO.none

  def readHeader(offset: OffsetBase, reader: Reader): IO[ReadResult] = {
    val movedReader = reader.moveTo(offset.start)
    for {
      headerSize <- movedReader.readIntUnsigned()
      headerReader <- movedReader.read(headerSize).map(Reader(_))
      formatID <- headerReader.get()
      headerReader <- validateFormatId(formatID).map(_ => headerReader)
      blockCompression <- readBlockCompression(formatID, headerSize, headerReader)
    } yield
      ReadResult(
        blockCompression = blockCompression,
        headerReader = headerReader,
        headerSize = headerSize
      )
  }

  /**
    * Decompresses the block skipping the header bytes.
    */
  def decompress(blockCompression: BlockCompression.State,
                 compressedReader: Reader,
                 offset: OffsetBase): IO[Slice[Byte]] =
    blockCompression
      .decompressedBytes
      .map(IO.Success(_))
      .getOrElse {
        if (Reserve.setBusyOrGet((), blockCompression.reserve).isEmpty)
          try
            compressedReader
              .moveTo(offset.start + blockCompression.headerSize)
              .read(offset.size - blockCompression.headerSize)
              .flatMap {
                compressedBytes =>
                  blockCompression.decompressor.decompress(
                    slice = compressedBytes,
                    decompressLength = blockCompression.decompressedLength
                  ) map {
                    decompressedBytes =>
                      blockCompression.decompressedBytes = decompressedBytes
                      decompressedBytes
                  }
              }
          finally
            Reserve.setFree(blockCompression.reserve)
        else
          IO.Failure(IO.Error.DecompressingValues(blockCompression.reserve))
      }

  def getDecompressedReader(blockCompression: BlockCompression.State,
                            compressedReader: Reader,
                            offset: OffsetBase): IO[Reader] =
    BlockCompression.decompress(
      blockCompression = blockCompression,
      compressedReader = compressedReader,
      offset = offset
    ) map (Reader(_))
}
