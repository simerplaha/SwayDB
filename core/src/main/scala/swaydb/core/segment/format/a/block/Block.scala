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
import swaydb.data.IO._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf
import swaydb.data.{IO, Reserve}

/**
  * A block is a group of compressed or uncompressed bytes.
  */
trait Block {
  def offset: OffsetBase
  def headerSize: Int
  def compressionInfo: Option[Block.CompressionInfo]
  def updateOffset(start: Int, size: Int): Block
  def createBlockReader(segmentReader: BlockReader[SegmentBlock]): BlockReader[_ <: Block]
}

object Block extends LazyLogging {

  val uncompressedBlockId: Byte = 0.toByte
  val compressedBlockID: Byte = 1.toByte

  class CompressionInfo(val decompressor: DecompressorInternal,
                        val decompressedLength: Int,
                        val headerSize: Int,
                        private[Block] val reserve: Reserve[Unit],
                        @volatile private var _decompressedBytes: Option[IO.Success[Slice[Byte]]]) {
    def decompressedBytes = _decompressedBytes

    def decompressedBytes_=(bytes: Slice[Byte]) =
      this._decompressedBytes = Some(IO.Success(bytes))

    def isBusy =
      reserve.isBusy

    def clear() =
      if (_decompressedBytes.isEmpty)
        this
      else
        new CompressionInfo(
          decompressor = decompressor,
          decompressedLength = decompressedLength,
          headerSize = headerSize,
          reserve = reserve,
          _decompressedBytes = None
        )
  }

  case class Header(compressionInfo: Option[CompressionInfo],
                    headerReader: Reader,
                    headerSize: Int)

  private val headerSizeWithCompression =
    ByteSizeOf.byte + //formatId
      ByteSizeOf.byte + //decompressor
      ByteSizeOf.int + 1 //decompressed length. +1 for larger varints

  private val headerSizeNoCompression =
    ByteSizeOf.byte //formatId

  def headerSize(hasCompression: Boolean) =
    if (hasCompression)
      Block.headerSizeWithCompression
    else
      Block.headerSizeNoCompression

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
  def create(headerSize: Int,
             bytes: Slice[Byte],
             compressions: Seq[CompressionInternal]): IO[Slice[Byte]] =
    compressions.untilSome(_.compressor.compress(headerSize, bytes.drop(headerSize))) flatMap {
      case Some((compressedBytes, compression)) =>
        IO {
          compressedBytes moveWritePosition 0
          compressedBytes addIntUnsigned headerSize
          compressedBytes add compressedBlockID
          compressedBytes addIntUnsigned compression.decompressor.id
          compressedBytes addIntUnsigned (bytes.size - headerSize) //decompressed bytes
          compressedBytes
        }

      case None =>
        logger.debug {
          if (compressions.isEmpty)
            s"No compression strategies provided. Storing ${bytes.size}.bytes uncompressed."
          else
            s"Unable to satisfy compression requirement from ${compressions.size} compression strategies. Storing ${bytes.size}.bytes uncompressed."
        }
        IO {
          bytes moveWritePosition 0
          bytes addIntUnsigned headerSize
          bytes add uncompressedBlockId
        }
    }

  def create(headerSize: Int,
             closedSegment: SegmentBlock.ClosedSegment,
             compressions: Seq[CompressionInternal]): IO[SegmentBlock.ClosedSegment] =
    if (compressions.isEmpty) {
      logger.debug(s"No compression strategies provided for Segment level compression. Storing ${closedSegment.segmentSize}.bytes uncompressed.")
      IO {
        closedSegment.segmentBytes.head moveWritePosition 0
        closedSegment.segmentBytes.head addIntUnsigned headerSize
        closedSegment.segmentBytes.head add uncompressedBlockId
        closedSegment
      }
    } else {
      create(
        headerSize = headerSize,
        bytes = closedSegment.flattenSegmentBytes,
        compressions = compressions
      ) map {
        bytes =>
          SegmentBlock.ClosedSegment(
            segmentBytes = Slice(bytes),
            minMaxFunctionId = closedSegment.minMaxFunctionId,
            nearestDeadline = closedSegment.nearestDeadline
          )
      }
    }

  private def readCompressionInfo(formatID: Int,
                                  headerSize: Int,
                                  reader: Reader): IO[Option[CompressionInfo]] =
    if (formatID == compressedBlockID)
      for {
        decompressor <- reader.readIntUnsigned() flatMap (DecompressorInternal(_))
        decompressedLength <- reader.readIntUnsigned()
      } yield
        Some(
          new CompressionInfo(
            decompressor = decompressor,
            decompressedLength = decompressedLength,
            reserve = Reserve(),
            headerSize = headerSize,
            _decompressedBytes = None
          )
        )
    else if (formatID == Block.uncompressedBlockId)
      IO.none
    else
      IO.Failure(
        IO.Error.Fatal(
          new Exception(s"Invalid formatID: $formatID. Expected: ${Block.uncompressedBlockId} or ${Block.compressedBlockID}")
        )
      )

  def readHeader(offset: OffsetBase,
                 reader: Reader): IO[Block.Header] = {
    val movedReader = reader.moveTo(offset.start)
    for {
      headerSize <- movedReader.readIntUnsigned()
      headerReader <- movedReader.read(headerSize).map(Reader(_))
      formatID <- headerReader.get()
      compressionInfo <- {
        Block.readCompressionInfo(
          formatID = formatID,
          headerSize = headerSize,
          reader = headerReader
        )
      }
    } yield
      Header(
        compressionInfo = compressionInfo,
        headerReader = headerReader,
        headerSize = headerSize
      )
  }

  /**
    * Decompresses the block skipping the header bytes.
    */
  def decompress(compressionInfo: CompressionInfo,
                 reader: Reader,
                 offset: OffsetBase): IO[Slice[Byte]] =
    compressionInfo
      .decompressedBytes
      .getOrElse {
        if (Reserve.setBusyOrGet((), compressionInfo.reserve).isEmpty)
          try
            reader
              .copy()
              .moveTo(offset.start + compressionInfo.headerSize)
              .read(offset.size - compressionInfo.headerSize)
              .flatMap {
                compressedBytes =>
                  compressionInfo.decompressor.decompress(
                    slice = compressedBytes,
                    decompressLength = compressionInfo.decompressedLength
                  ) map {
                    decompressedBytes =>
                      compressionInfo.decompressedBytes = decompressedBytes
                      decompressedBytes
                  }
              }
          finally
            Reserve.setFree(compressionInfo.reserve)
        else
          IO.Failure(IO.Error.DecompressingValues(compressionInfo.reserve))
      }
}
