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
import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.format.a.OffsetBase
import swaydb.core.util.Bytes
import swaydb.data.IO._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf
import swaydb.data.{IO, Reserve}

/**
  * A block is a group of compressed or uncompressed bytes.
  */
object Block extends LazyLogging {

  val uncompressedFormatId: Byte = 0.toByte
  val compressedFormatID: Byte = 1.toByte

  case class Header(block: Option[Block.State],
                    headerReader: Reader,
                    headerSize: Int)

  class State(val decompressor: DecompressorInternal,
              val decompressedLength: Int,
              val headerSize: Int,
              private[Block] val reserve: Reserve[Unit],
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
  def compress(headerSize: Int,
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
    if (formatID != Block.uncompressedFormatId && formatID != Block.compressedFormatID)
      IO.Failure(
        IO.Error.Fatal(
          new Exception(s"Invalid formatID: $formatID. Expected: ${Block.uncompressedFormatId} or ${Block.compressedFormatID}")
        )
      )
    else
      IO.unit

  private def read(formatID: Int,
                   headerSize: Int,
                   segmentReader: Reader): IO[Option[Block.State]] =
    if (formatID == compressedFormatID)
      for {
        decompressor <- segmentReader.readIntUnsigned() flatMap (DecompressorInternal(_))
        decompressedLength <- segmentReader.readIntUnsigned()
      } yield
        Some(
          Block(
            decompressor = decompressor,
            headerSize = headerSize,
            decompressedLength = decompressedLength
          )
        )
    else
      IO.none

  def readHeader(offset: OffsetBase, segmentReader: Reader): IO[Block.Header] = {
    val movedReader = segmentReader.moveTo(offset.start)
    for {
      headerSize <- movedReader.readIntUnsigned()
      headerReader <- movedReader.read(headerSize).map(Reader(_))
      formatID <- headerReader.get()
      headerReader <- Block.validateFormatId(formatID).map(_ => headerReader)
      block <- Block.read(formatID, headerSize, headerReader)
    } yield
      Header(
        block = block,
        headerReader = headerReader,
        headerSize = headerSize
      )
  }

  /**
    * Decompresses the block skipping the header bytes.
    */
  def decompress(block: Block.State,
                 compressedReader: Reader,
                 offset: OffsetBase): IO[Slice[Byte]] =
    block
      .decompressedBytes
      .map(IO.Success(_))
      .getOrElse {
        if (Reserve.setBusyOrGet((), block.reserve).isEmpty)
          try
            compressedReader
              .moveTo(offset.start + block.headerSize)
              .read(offset.size - block.headerSize)
              .flatMap {
                compressedBytes =>
                  block.decompressor.decompress(
                    slice = compressedBytes,
                    decompressLength = block.decompressedLength
                  ) map {
                    decompressedBytes =>
                      block.decompressedBytes = decompressedBytes
                      decompressedBytes
                  }
              }
          finally
            Reserve.setFree(block.reserve)
        else
          IO.Failure(IO.Error.DecompressingValues(block.reserve))
      }

  def blockReader(offset: OffsetBase,
                  segmentReader: Reader,
                  headerSize: Int,
                  block: Option[Block.State]): BlockReader =
    new BlockReader(
      reader = segmentReader,
      offset = offset,
      headerSize = headerSize,
      block = block
    )
}
