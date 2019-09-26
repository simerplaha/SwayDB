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
import swaydb.IO
import swaydb.compression.{CompressionInternal, DecompressorInternal}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.util.Bytes
import swaydb.core.util.Collections._
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
  def dataType: IOAction.DataAction =
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
                       headerReader: ReaderBase,
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
            blockName: String): Slice[Byte] =
    compressions.untilSome(_.compressor.compress(headerSize, bytes.drop(headerSize))) match {
      case Some((compressedBytes, compression)) =>
        compressedBytes moveWritePosition 0
        compressedBytes addUnsignedInt headerSize
        compressedBytes add compressedBlockID
        compressedBytes addUnsignedInt compression.decompressor.id
        compressedBytes addUnsignedInt (bytes.size - headerSize) //decompressed bytes
        if (compressedBytes.currentWritePosition > headerSize)
          throw new Exception(s"Compressed header bytes written over to data bytes for $blockName. CurrentPosition: ${compressedBytes.currentWritePosition}, headerSize: $headerSize, dataSize: ${compressedBytes.size}")
        compressedBytes

      case None =>
        logger.trace {
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
              blockName: String): Slice[Byte] = {
    bytes moveWritePosition 0
    bytes addUnsignedInt headerSize
    bytes add uncompressedBlockId
    if (bytes.currentWritePosition > headerSize)
      throw new Exception(s"Uncompressed header bytes written over to data bytes for $blockName. CurrentPosition: ${bytes.currentWritePosition}, headerSize: $headerSize, dataSize: ${bytes.size}")
    bytes
  }

  def block(openSegment: SegmentBlock.Open,
            compressions: Seq[CompressionInternal],
            blockName: String): SegmentBlock.Closed =
    if (compressions.isEmpty) {
      logger.trace(s"No compression strategies provided for Segment level compression for $blockName. Storing ${openSegment.segmentSize}.bytes uncompressed.")
      openSegment.headerBytes moveWritePosition 0
      openSegment.headerBytes addUnsignedInt openSegment.headerBytes.size
      openSegment.headerBytes add uncompressedBlockId
      SegmentBlock.Closed(
        segmentBytes = openSegment.segmentBytes,
        minMaxFunctionId = openSegment.functionMinMax,
        nearestDeadline = openSegment.nearestDeadline
      )
    } else {
      val bytes =
        Block.block(
          headerSize = openSegment.headerBytes.size,
          bytes = openSegment.flattenSegmentBytes,
          compressions = compressions,
          blockName = blockName
        )

      SegmentBlock.Closed(
        segmentBytes = Slice(bytes),
        minMaxFunctionId = openSegment.functionMinMax,
        nearestDeadline = openSegment.nearestDeadline
      )
    }

  private def readCompressionInfo(formatID: Int,
                                  headerSize: Int,
                                  reader: ReaderBase): Option[CompressionInfo] =
    if (formatID == compressedBlockID) {
      val decompressor = DecompressorInternal(reader.readUnsignedInt())
      val decompressedLength = reader.readUnsignedInt()
      Some(
        new CompressionInfo(
          decompressor = decompressor,
          decompressedLength = decompressedLength
        )
      )
    }
    else if (formatID == Block.uncompressedBlockId)
      None
    else {
      val message = s"Invalid formatID: $formatID. Expected: ${Block.uncompressedBlockId} or ${Block.compressedBlockID}"
      throw swaydb.Exception.InvalidDataId(formatID, message)
    }

  def readHeader[O <: BlockOffset](reader: BlockRefReader[O])(implicit blockOps: BlockOps[O, _]): Block.Header[O] = {
    val headerSize = reader.readUnsignedInt()
    val headerReader = Reader(reader.read(headerSize - Bytes.sizeOfUnsignedInt(headerSize)))
    val formatID = headerReader.get()

    val compressionInfo =
      Block.readCompressionInfo(
        formatID = formatID,
        headerSize = headerSize,
        reader = headerReader
      )

    Header(
      compressionInfo = compressionInfo,
      headerReader = headerReader,
      headerSize = headerSize,
      offset = blockOps.createOffset(reader.offset.start + headerSize, reader.offset.size - headerSize)
    )
  }

  def unblock[O <: BlockOffset, B <: Block[O]](bytes: Slice[Byte])(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    unblock(BlockRefReader(bytes))

  def unblock[O <: BlockOffset, B <: Block[O]](ref: BlockRefReader[O],
                                               readAllIfUncompressed: Boolean = false)(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    Block.unblock[O, B](
      reader = BlockedReader(ref),
      readAllIfUncompressed = readAllIfUncompressed
    )

  def unblock[O <: BlockOffset, B <: Block[O]](reader: BlockedReader[O, B],
                                               readAllIfUncompressed: Boolean)(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    reader.block.compressionInfo match {
      case Some(compressionInfo) =>
        val compressedBytes =
          reader
            .readFullBlock()

        val decompressedBytes =
          compressionInfo.decompressor.decompress(
            slice = compressedBytes,
            decompressLength = compressionInfo.decompressedLength
          )

        if (decompressedBytes.size == compressionInfo.decompressedLength)
          UnblockedReader[O, B](
            bytes = decompressedBytes,
            block =
              blockOps.updateBlockOffset(
                block = reader.block,
                start = 0,
                size = decompressedBytes.size
              )
          )
        else
          throw IO.throwable(s"Decompressed bytes size (${decompressedBytes.size}) != decompressedLength (${compressionInfo.decompressedLength}).")

      case None =>
        //no compression just skip the header bytes.
        val unblockedReader = UnblockedReader.fromUncompressed(reader)
        if (readAllIfUncompressed)
          unblockedReader.readAllAndGetReader()
        else
          unblockedReader
    }
}
