/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
import swaydb.core.segment.format.a.block.segment.{TransientSegment, TransientSegmentBlock}
import swaydb.core.util.Collections._
import swaydb.data.config.IOAction
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.ByteSizeOf

/**
 * A block is a group of compressed or uncompressed bytes.
 */
private[core] trait Block[+O <: BlockOffset] {
  def offset: O
  def headerSize: Int
  def compressionInfo: Option[Block.CompressionInfo]
  def dataType: IOAction.DataAction =
    compressionInfo match {
      case Some(compressionInfo) =>
        IOAction.ReadCompressedData(
          compressedSize = offset.size - headerSize,
          decompressedSize = compressionInfo.decompressedLength
        )

      case None =>
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

  class Header[O](val compressionInfo: Option[CompressionInfo],
                  val headerReader: ReaderBase,
                  val headerSize: Int,
                  val offset: O)

  class CompressionResult(val compressedBytes: Option[Slice[Byte]],
                          val headerBytes: Slice[Byte]) {
    /**
     * More bytes could get allocated to headerBytes when it's created. This fixes the size to it's original size.
     * currently not more that 1 byte is required to store the size of the header.
     *
     * @return mutates and sets the size of [[headerBytes]]
     */
    def fixHeaderSize(): Unit = {
      headerBytes moveWritePosition 0
      headerBytes addUnsignedInt (headerBytes.size - 1)
    }
  }

  private val headerSizeWithCompression =
    ByteSizeOf.byte + //headerSize
      ByteSizeOf.byte + //formatId
      ByteSizeOf.byte + //decompressor
      ByteSizeOf.varInt //decompressed length. +1 for larger varints

  private val headerSizeNoCompression =
    ByteSizeOf.byte + //headerSize
      ByteSizeOf.byte //formatId

  def minimumHeaderSize(hasCompression: Boolean): Int =
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
   * Others using this function should ensure that [[minimumHeaderSize]] is accounted for in the byte size calculations.
   * They should also allocate enough bytes to write the total headerSize.
   *
   * NOTE: Always invoke [[CompressionResult.fixHeaderSize()]] when done writing header bytes outside this function.
   */
  def compress(bytes: Slice[Byte],
               compressions: Seq[CompressionInternal],
               blockName: String): CompressionResult =
    compressions.untilSome(_.compressor.compress(bytes)) match {
      case Some((compressedBytes, compression)) =>
        val header = Slice.create[Byte](Byte.MaxValue)

        header moveWritePosition 1 //skip writing header size since it's not known.
        header add compressedBlockID
        header addUnsignedInt compression.decompressor.id
        header addUnsignedInt bytes.size //decompressed bytes

        new CompressionResult(
          compressedBytes = Some(compressedBytes),
          headerBytes = header
        )

      case None =>
        logger.trace {
          if (compressions.isEmpty)
            s"No compression strategies provided for $blockName. Storing ${bytes.size}.bytes uncompressed."
          else
            s"Unable to satisfy compression requirement from ${compressions.size} compression strategies for $blockName. Storing ${bytes.size}.bytes uncompressed."
        }

        val header = Slice.create[Byte](Byte.MaxValue)

        header moveWritePosition 1 //skip writing header size since it's not known.
        header add uncompressedBlockId

        new CompressionResult(
          compressedBytes = None,
          headerBytes = header
        )
    }

  def block(ref: TransientSegmentBlock,
            compressions: Seq[CompressionInternal],
            blockName: String): TransientSegment =
    if (compressions.isEmpty) {
      logger.trace(s"No compression strategies provided for Segment level compression for $blockName. Storing ${ref.segmentSize}.bytes uncompressed.")
      //      openSegment.segmentHeader moveWritePosition 0
      ref.segmentHeader addUnsignedInt 1
      ref.segmentHeader add uncompressedBlockId

      val segmentBytes =
        ref.segmentBytes.collect {
          case bytes if bytes.nonEmpty => bytes.close()
        }

      new TransientSegment(
        minKey = ref.minKey,
        maxKey = ref.maxKey,
        segmentBytes = segmentBytes,
        minMaxFunctionId = ref.functionMinMax,
        nearestDeadline = ref.nearestDeadline,
        valuesUnblockedReader = ref.valuesUnblockedReader,
        sortedIndexClosedState = ref.sortedIndexClosedState,
        sortedIndexUnblockedReader = ref.sortedIndexUnblockedReader,
        hashIndexUnblockedReader = ref.hashIndexUnblockedReader,
        binarySearchUnblockedReader = ref.binarySearchUnblockedReader,
        bloomFilterUnblockedReader = ref.bloomFilterUnblockedReader,
        footerUnblocked = ref.footerUnblocked
      )
    } else {
      //header is empty so no header bytes are included.
      val uncompressedSegmentBytes = ref.flattenSegmentBytes

      val compressionResult =
        Block.compress(
          bytes = uncompressedSegmentBytes,
          compressions = compressions,
          blockName = blockName
        )

      val compressedOrUncompressedSegmentBytes =
        compressionResult.compressedBytes getOrElse uncompressedSegmentBytes

      compressionResult.fixHeaderSize()

      new TransientSegment(
        minKey = ref.minKey,
        maxKey = ref.maxKey,
        segmentBytes = Slice(compressionResult.headerBytes.close(), compressedOrUncompressedSegmentBytes),
        minMaxFunctionId = ref.functionMinMax,
        nearestDeadline = ref.nearestDeadline,
        valuesUnblockedReader = ref.valuesUnblockedReader,
        sortedIndexClosedState = ref.sortedIndexClosedState,
        sortedIndexUnblockedReader = ref.sortedIndexUnblockedReader,
        hashIndexUnblockedReader = ref.hashIndexUnblockedReader,
        binarySearchUnblockedReader = ref.binarySearchUnblockedReader,
        bloomFilterUnblockedReader = ref.bloomFilterUnblockedReader,
        footerUnblocked = ref.footerUnblocked
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
    } else if (formatID == Block.uncompressedBlockId) {
      None
    } else {
      val message = s"Invalid formatID: $formatID. Expected: ${Block.uncompressedBlockId} or ${Block.compressedBlockID}"
      throw swaydb.Exception.InvalidDataId(formatID, message)
    }

  def readHeader[O <: BlockOffset](reader: BlockRefReader[O])(implicit blockOps: BlockOps[O, _]): Block.Header[O] = {
    val (headerSize, headerSizeByteSize) = reader.readUnsignedIntWithByteSize()
    val headerReader = Reader(reader.read(headerSize))
    val formatID = headerReader.get()

    val compressionInfo =
      Block.readCompressionInfo(
        formatID = formatID,
        headerSize = headerSize,
        reader = headerReader
      )

    val actualHeaderSize = headerSize + headerSizeByteSize

    new Header(
      compressionInfo = compressionInfo,
      headerReader = headerReader,
      headerSize = actualHeaderSize,
      offset =
        blockOps.createOffset(
          reader.offset.start + actualHeaderSize,
          reader.offset.size - actualHeaderSize
        )
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
