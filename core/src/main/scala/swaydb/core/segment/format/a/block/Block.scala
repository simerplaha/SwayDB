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
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.IO._
import swaydb.compression.{CompressionInternal, DecompressorInternal}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.util.Bytes
import swaydb.data.config.IOAction
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.ByteSizeOf

/**
 * A block is a group of compressed or uncompressed bytes.
 */
private[core] trait Block[O <: BlockOffset] {
  def offset: O
  def headerSize: Int
  def compressionInfo: Option[Block.CompressionInfo]
  def dataType: IOAction =
    compressionInfo map {
      compressionInfo =>
        IOAction.ReadCompressedData(
          compressedSize = offset.size - headerSize,
          decompressedSize = compressionInfo.decompressedLength
        )
    } getOrElse {
      IOAction.ReadUncompressedData(offset.size - headerSize)
    }
}

private[core] object Block extends LazyLogging {

  val uncompressedBlockId: Byte = 0.toByte
  val compressedBlockID: Byte = 1.toByte

  object CompressionInfo {
    def apply(decompressor: DecompressorInternal,
              decompressedLength: Int): CompressionInfo =
      new CompressionInfo(decompressor, decompressedLength)
  }

  class CompressionInfo(val decompressor: DecompressorInternal,
                        val decompressedLength: Int)

  case class Header[O](compressionInfo: Option[CompressionInfo],
                       headerReader: ReaderBase[swaydb.Error.Segment],
                       headerSize: Int,
                       offset: O)

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
  def block(headerSize: Int,
            bytes: Slice[Byte],
            compressions: Seq[CompressionInternal],
            blockName: String): IO[swaydb.Error.Segment, Slice[Byte]] =
    compressions.untilSome(_.compressor.compress(headerSize, bytes.drop(headerSize))) flatMap {
      case Some((compressedBytes, compression)) =>
        IO {
          compressedBytes moveWritePosition 0
          compressedBytes addIntUnsigned headerSize
          compressedBytes add compressedBlockID
          compressedBytes addIntUnsigned compression.decompressor.id
          compressedBytes addIntUnsigned (bytes.size - headerSize) //decompressed bytes
          if (compressedBytes.currentWritePosition > headerSize)
            throw new Exception(s"Compressed header bytes written over to data bytes for $blockName. CurrentPosition: ${compressedBytes.currentWritePosition}, headerSize: $headerSize, dataSize: ${compressedBytes.size}")
          compressedBytes
        }

      case None =>
        logger.debug {
          if (compressions.isEmpty)
            s"No compression strategies provided for $blockName. Storing ${bytes.size}.bytes uncompressed."
          else
            s"Unable to satisfy compression requirement from ${compressions.size} compression strategies for $blockName. Storing ${bytes.size}.bytes uncompressed."
        }
        unblock(
          headerSize = headerSize,
          bytes = bytes,
          blockName = blockName
        )
    }

  def unblock(headerSize: Int,
              bytes: Slice[Byte],
              blockName: String): IO[swaydb.Error.Segment, Slice[Byte]] =
    IO {
      bytes moveWritePosition 0
      bytes addIntUnsigned headerSize
      bytes add uncompressedBlockId
      if (bytes.currentWritePosition > headerSize)
        throw new Exception(s"Uncompressed header bytes written over to data bytes for $blockName. CurrentPosition: ${bytes.currentWritePosition}, headerSize: $headerSize, dataSize: ${bytes.size}")
      bytes
    }

  def block(openSegment: SegmentBlock.Open,
            compressions: Seq[CompressionInternal],
            blockName: String): IO[swaydb.Error.Segment, SegmentBlock.Closed] =
    if (compressions.isEmpty) {
      logger.debug(s"No compression strategies provided for Segment level compression for $blockName. Storing ${openSegment.segmentSize}.bytes uncompressed.")
      IO {
        openSegment.headerBytes moveWritePosition 0
        openSegment.headerBytes addIntUnsigned openSegment.headerBytes.size
        openSegment.headerBytes add uncompressedBlockId
        SegmentBlock.Closed(
          segmentBytes = openSegment.segmentBytes,
          minMaxFunctionId = openSegment.functionMinMax,
          nearestDeadline = openSegment.nearestDeadline
        )
      }
    } else {
      Block.block(
        headerSize = openSegment.headerBytes.size,
        bytes = openSegment.flattenSegmentBytes,
        compressions = compressions,
        blockName = blockName
      ) map {
        bytes =>
          SegmentBlock.Closed(
            segmentBytes = Slice(bytes),
            minMaxFunctionId = openSegment.functionMinMax,
            nearestDeadline = openSegment.nearestDeadline
          )
      }
    }

  private def readCompressionInfo(formatID: Int,
                                  headerSize: Int,
                                  reader: ReaderBase[swaydb.Error.Segment]): IO[swaydb.Error.Segment, Option[CompressionInfo]] =
    if (formatID == compressedBlockID) {
      for {
        decompressor <- reader.readIntUnsigned() flatMap (DecompressorInternal(_))
        decompressedLength <- reader.readIntUnsigned()
      } yield
        Some(
          new CompressionInfo(
            decompressor = decompressor,
            decompressedLength = decompressedLength
          )
        )
    }
    else if (formatID == Block.uncompressedBlockId)
      IO.none
    else
      IO.Failure {
        val message = s"Invalid formatID: $formatID. Expected: ${Block.uncompressedBlockId} or ${Block.compressedBlockID}"
        swaydb.Error.DataAccess(message, new Exception(message))
      }

  def readHeader[O <: BlockOffset](reader: BlockRefReader[O])(implicit blockOps: BlockOps[O, _]): IO[swaydb.Error.Segment, Block.Header[O]] = {
    for {
      headerSize <- reader.readIntUnsigned()
      headerReader <- reader.read(headerSize - Bytes.sizeOf(headerSize)).map(Reader[swaydb.Error.Segment](_))
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
        headerSize = headerSize,
        offset = blockOps.createOffset(reader.offset.start + headerSize, reader.offset.size - headerSize)
      )
    }
  }

  def unblock[O <: BlockOffset, B <: Block[O]](bytes: Slice[Byte])(implicit blockOps: BlockOps[O, B]): IO[swaydb.Error.Segment, UnblockedReader[O, B]] =
    unblock(BlockRefReader(bytes))

  def unblock[O <: BlockOffset, B <: Block[O]](ref: BlockRefReader[O])(implicit blockOps: BlockOps[O, B]): IO[swaydb.Error.Segment, UnblockedReader[O, B]] =
    BlockedReader(ref) flatMap {
      blockedReader =>
        Block.unblock[O, B](
          reader = blockedReader,
          readAllIfUncompressed = false
        )
    }

  def unblock[O <: BlockOffset, B <: Block[O]](reader: BlockedReader[O, B],
                                               readAllIfUncompressed: Boolean)(implicit blockOps: BlockOps[O, B]): IO[swaydb.Error.Segment, UnblockedReader[O, B]] =
    reader.block.compressionInfo match {
      case Some(compressionInfo) =>
        reader
          .readFullBlock()
          .flatMap {
            compressedBytes =>
              compressionInfo.decompressor.decompress(
                slice = compressedBytes,
                decompressLength = compressionInfo.decompressedLength
              )
          }
          .flatMap {
            decompressedBytes =>
              if (decompressedBytes.size == compressionInfo.decompressedLength)
                IO {
                  UnblockedReader[O, B](
                    bytes = decompressedBytes,
                    block =
                      blockOps.updateBlockOffset(
                        block = reader.block,
                        start = 0,
                        size = decompressedBytes.size
                      )
                  )
                }
              else
                IO.Failure {
                  val message = s"Decompressed bytes size (${decompressedBytes.size}) != decompressedLength (${compressionInfo.decompressedLength})."
                  swaydb.Error.DataAccess(message, new Exception(message))
                }
          }

      case None =>
        //no compression just skip the header bytes.
        val unblockedReader = UnblockedReader.fromUncompressed(reader)
        if (readAllIfUncompressed)
          unblockedReader.readAllAndGetReader()
        else
          IO.Success(unblockedReader)
    }
}
