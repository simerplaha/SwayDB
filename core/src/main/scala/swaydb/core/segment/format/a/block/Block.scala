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
import swaydb.core.segment.SegmentException
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

/**
  * A block is a group of compressed or uncompressed bytes.
  */
private[core] trait Block {
  def offset: BlockOffset
  def headerSize: Int
  def compressionInfo: Option[Block.CompressionInfo]
  def updateOffset(start: Int, size: Int): Block
  def createBlockReader(segmentReader: BlockReader[SegmentBlock]): BlockReader[_ <: Block]
}

private[core] object Block extends LazyLogging {

  val uncompressedBlockId: Byte = 0.toByte
  val compressedBlockID: Byte = 1.toByte

  class CompressionInfo(val decompressor: DecompressorInternal,
                        val decompressedLength: Int,
                        val headerSize: Int)

  case class Header(compressionInfo: Option[CompressionInfo],
                    headerReader: Reader,
                    headerSize: Int)

  private val headerSizeWithCompression =
    ByteSizeOf.byte + //formatId
      ByteSizeOf.byte + //decompressor
      ByteSizeOf.varInt //decompressed length. +1 for larger varints

  private val headerSizeNoCompression =
    ByteSizeOf.byte //formatId

  def headerSize(hasCompression: Boolean): Int =
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
             compressions: Seq[CompressionInternal],
             blockName: String): IO[Slice[Byte]] =
    compressions.untilSome(_.compressor.compress(headerSize, bytes.drop(headerSize))) flatMap {
      case Some((compressedBytes, compression)) =>
        IO {
          compressedBytes moveWritePosition 0
          compressedBytes addIntUnsigned headerSize
          compressedBytes add compressedBlockID
          compressedBytes addIntUnsigned compression.decompressor.id
          compressedBytes addIntUnsigned (bytes.size - headerSize) //decompressed bytes
          assert(compressedBytes.currentWritePosition <= headerSize, s"Compressed header bytes written over to data bytes. CurrentPosition: ${compressedBytes.currentWritePosition}, headerSize: $headerSize, dataSize: ${compressedBytes.size}")
          compressedBytes
        }

      case None =>
        logger.debug {
          if (compressions.isEmpty)
            s"No compression strategies provided for $blockName. Storing ${bytes.size}.bytes uncompressed."
          else
            s"Unable to satisfy compression requirement from ${compressions.size} compression strategies for $blockName. Storing ${bytes.size}.bytes uncompressed."
        }
        IO {
          bytes moveWritePosition 0
          bytes addIntUnsigned headerSize
          bytes add uncompressedBlockId
          assert(bytes.currentWritePosition <= headerSize, s"Uncompressed header bytes written over to data bytes. CurrentPosition: ${bytes.currentWritePosition}, headerSize: $headerSize, dataSize: ${bytes.size}")
          bytes
        }
    }

  def create(openSegment: SegmentBlock.Open,
             compressions: Seq[CompressionInternal],
             blockName: String): IO[SegmentBlock.Blocked] =
    if (compressions.isEmpty) {
      logger.debug(s"No compression strategies provided for Segment level compression for $blockName. Storing ${openSegment.segmentSize}.bytes uncompressed.")
      IO {
        openSegment.headerBytes moveWritePosition 0
        openSegment.headerBytes addIntUnsigned openSegment.headerBytes.size
        openSegment.headerBytes add uncompressedBlockId
        SegmentBlock.Blocked(
          segmentBytes = openSegment.segmentBytes,
          minMaxFunctionId = openSegment.functionMinMax,
          nearestDeadline = openSegment.nearestDeadline
        )
      }
    } else {
      Block.create(
        headerSize = openSegment.headerBytes.size,
        bytes = openSegment.flattenSegmentBytes,
        compressions = compressions,
        blockName = blockName
      ) map {
        bytes =>
          SegmentBlock.Blocked(
            segmentBytes = Slice(bytes),
            minMaxFunctionId = openSegment.functionMinMax,
            nearestDeadline = openSegment.nearestDeadline
          )
      }
    }

  private def readCompressionInfo(formatID: Int,
                                  headerSize: Int,
                                  reader: Reader): IO[Option[CompressionInfo]] =
    if (formatID == compressedBlockID) {
      for {
        decompressor <- reader.readIntUnsigned() flatMap (DecompressorInternal(_))
        decompressedLength <- reader.readIntUnsigned()
      } yield
        Some(
          new CompressionInfo(
            decompressor = decompressor,
            decompressedLength = decompressedLength,
            headerSize = headerSize
          )
        )
    }
    else if (formatID == Block.uncompressedBlockId)
      IO.none
    else
      IO.Failure {
        val message = s"Invalid formatID: $formatID. Expected: ${Block.uncompressedBlockId} or ${Block.compressedBlockID}"
        SegmentException.SegmentCorruptionException(message, new Exception(message))
      }

  def readHeader(offset: BlockOffset,
                 reader: Reader): IO[Block.Header] = {
    val movedReader = reader.moveTo(offset.start)
    for {
      headerSize <- movedReader.readIntUnsigned()
      headerReader <- movedReader.read(headerSize - Bytes.sizeOf(headerSize)).map(Reader(_))
      formatID <- headerReader.get()
      compressionInfo <- {
        Block.readCompressionInfo(
          formatID = formatID,
          headerSize = headerSize,
          reader = headerReader
        )
      }
    } yield {
      Header(
        compressionInfo = compressionInfo,
        headerReader = headerReader,
        headerSize = headerSize
      )
    }
  } recoverWith {
    case error =>
      IO.Failure {
        val message = s"Failed to read Segment block."
        SegmentException.SegmentCorruptionException(message, error.exception)
      }
  }

  /**
    * Decompresses the block skipping the header bytes.
    */
  private def decompress(compressionInfo: CompressionInfo,
                         reader: BlockReader[_ <: Block]): IO[Slice[Byte]] =
    reader
      .copy()
      .moveTo(reader.block.offset.start + compressionInfo.headerSize)
      .read(reader.block.offset.size - compressionInfo.headerSize)
      .flatMap {
        compressedBytes =>
          compressionInfo.decompressor.decompress(
            slice = compressedBytes,
            decompressLength = compressionInfo.decompressedLength
          )
      }

  def createDecompressedBlockReader[B <: Block](block: B,
                                                readFullBlockIfUncompressed: Boolean,
                                                segmentReader: BlockReader[SegmentBlock]): IO[BlockReader[B]] =
    block.compressionInfo match {
      case Some(compressionInfo) =>
        Block.decompress(
          compressionInfo = compressionInfo,
          reader = block createBlockReader segmentReader
        ) flatMap {
          decompressedBytes =>
            if (decompressedBytes.size == compressionInfo.decompressedLength)
              IO {
                BlockReader(
                  reader = Reader(decompressedBytes),
                  block =
                    block.updateOffset(
                      start = 0,
                      size = decompressedBytes.size
                    ).asInstanceOf[B]
                )
              }
            else
              IO.Failure(s"Decompressed bytes size (${decompressedBytes.size}) != decompressedLength (${compressionInfo.decompressedLength}).")
        }

      case None =>
        val reader =
          BlockReader(
            reader = segmentReader,
            block =
              block.updateOffset(
                start = block.offset.start + block.headerSize,
                size = block.offset.size - block.headerSize
              ).asInstanceOf[B]
          )

        if (readFullBlockIfUncompressed)
          reader.readFullBlockAndGetBlockReader()
        else
          IO.Success(reader)
    }
}
