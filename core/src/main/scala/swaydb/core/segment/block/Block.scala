/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.block

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.{CompressionInternal, DecompressorInternal}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.segment.block.segment.transient.{TransientSegment, TransientSegmentRef}
import swaydb.core.util.Collections._
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.effect.IOAction
import swaydb.utils.ByteSizeOf

/**
 * A block is a group of compressed or uncompressed bytes.
 */
private[core] trait Block[+O <: BlockOffset] {
  def offset: O
  def headerSize: Int
  def compressionInfo: BlockCompressionInfoOption

  def decompressionAction: IOAction.DecompressAction =
    compressionInfo match {
      case compressionInfo: BlockCompressionInfo =>
        IOAction.ReadCompressedData(
          compressedSize = offset.size - headerSize,
          decompressedSize = compressionInfo.decompressedLength
        )

      case BlockCompressionInfo.Null =>
        IOAction.ReadUncompressedData(offset.size - headerSize)
    }
}

private[core] object Block extends LazyLogging {

  val uncompressedBlockId: Byte = 0.toByte
  val compressedBlockID: Byte = 1.toByte

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
   * NOTE: Always invoke [[BlockCompressionResult.fixHeaderSize()]] when done writing header bytes outside this function.
   */
  def compress(bytes: Slice[Byte],
               compressions: Iterable[CompressionInternal],
               blockName: String): BlockCompressionResult =
    compressions.untilSome(_.compressor.compress(bytes)) match {
      case Some((compressedBytes, compression)) =>
        val header = Slice.of[Byte](Byte.MaxValue)

        header moveWritePosition 1 //skip writing header size since it's not known.
        header add Block.compressedBlockID
        header addUnsignedInt compression.decompressor.id
        header addUnsignedInt bytes.size //decompressed bytes

        new BlockCompressionResult(
          compressedBytes = compressedBytes,
          headerBytes = header
        )

      case None =>
        logger.trace {
          if (compressions.isEmpty)
            s"No compression strategies provided for $blockName. Storing ${bytes.size}.bytes uncompressed."
          else
            s"Unable to satisfy compression requirement from ${compressions.size} compression strategies for $blockName. Storing ${bytes.size}.bytes uncompressed."
        }

        val header = Slice.of[Byte](Byte.MaxValue)

        header moveWritePosition 1 //skip writing header size since it's not known.
        header add Block.uncompressedBlockId

        new BlockCompressionResult(
          compressedBytes = Slice.Null,
          headerBytes = header
        )
    }

  def block(blocks: TransientSegmentRef,
            compressions: Iterable[CompressionInternal],
            blockName: String): TransientSegment.One =
    if (compressions.isEmpty) {
      logger.trace(s"No compression strategies provided for Segment level compression for $blockName. Storing ${blocks.segmentSize}.bytes uncompressed.")
      //      openSegment.segmentHeader moveWritePosition 0
      blocks.segmentHeader addUnsignedInt 1
      blocks.segmentHeader add Block.uncompressedBlockId

      val segmentBytes: Slice[Slice[Byte]] =
        blocks.segmentBytes collectToSlice {
          case bytes if bytes.nonEmpty => bytes.close()
        }

      TransientSegment.One(
        minKey = blocks.minKey,
        maxKey = blocks.maxKey,
        fileHeader = Slice.emptyBytes,
        bodyBytes = segmentBytes,
        minMaxFunctionId = blocks.functionMinMax,
        nearestPutDeadline = blocks.nearestDeadline,
        updateCount = blocks.updateCount,
        rangeCount = blocks.rangeCount,
        putCount = blocks.putCount,
        putDeadlineCount = blocks.putDeadlineCount,
        keyValueCount = blocks.keyValueCount,
        createdInLevel = blocks.createdInLevel,
        valuesUnblockedReader = blocks.valuesUnblockedReader,
        sortedIndexUnblockedReader = blocks.sortedIndexUnblockedReader,
        hashIndexUnblockedReader = blocks.hashIndexUnblockedReader,
        binarySearchUnblockedReader = blocks.binarySearchUnblockedReader,
        bloomFilterUnblockedReader = blocks.bloomFilterUnblockedReader,
        footerUnblocked = blocks.footerUnblocked
      )
    } else {
      //header is empty so no header bytes are included.
      val uncompressedSegmentBytes = blocks.flattenSegmentBytes

      val compressionResult =
        Block.compress(
          bytes = uncompressedSegmentBytes,
          compressions = compressions,
          blockName = blockName
        )

      val compressedOrUncompressedSegmentBytes =
        compressionResult.compressedBytes getOrElseC uncompressedSegmentBytes

      compressionResult.fixHeaderSize()

      TransientSegment.One(
        minKey = blocks.minKey,
        maxKey = blocks.maxKey,
        fileHeader = Slice.emptyBytes,
        bodyBytes = Slice(compressionResult.headerBytes.close(), compressedOrUncompressedSegmentBytes),
        minMaxFunctionId = blocks.functionMinMax,
        nearestPutDeadline = blocks.nearestDeadline,
        updateCount = blocks.updateCount,
        rangeCount = blocks.rangeCount,
        putCount = blocks.putCount,
        putDeadlineCount = blocks.putDeadlineCount,
        keyValueCount = blocks.keyValueCount,
        createdInLevel = blocks.createdInLevel,
        valuesUnblockedReader = blocks.valuesUnblockedReader,
        sortedIndexUnblockedReader = blocks.sortedIndexUnblockedReader,
        hashIndexUnblockedReader = blocks.hashIndexUnblockedReader,
        binarySearchUnblockedReader = blocks.binarySearchUnblockedReader,
        bloomFilterUnblockedReader = blocks.bloomFilterUnblockedReader,
        footerUnblocked = blocks.footerUnblocked
      )
    }

  private def readCompressionInfo(formatID: Int,
                                  headerSize: Int,
                                  reader: ReaderBase[Byte]): BlockCompressionInfoOption =
    if (formatID == Block.compressedBlockID)
      new BlockCompressionInfo(
        decompressor = DecompressorInternal(reader.readUnsignedInt()),
        decompressedLength = reader.readUnsignedInt()
      )
    else if (formatID == Block.uncompressedBlockId)
      BlockCompressionInfo.Null
    else
      throw swaydb.Exception.InvalidDataId(
        id = formatID,
        message = s"Invalid formatID: $formatID. Expected: ${Block.uncompressedBlockId} or ${Block.compressedBlockID}"
      )

  def readHeader[O <: BlockOffset](reader: BlockRefReader[O])(implicit blockOps: BlockOps[O, _]): BlockHeader[O] = {
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

    new BlockHeader(
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
      case compressionInfo: BlockCompressionInfo =>
        val compressedBytes = reader.readFullBlock()

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
          throw new Exception(s"Decompressed bytes size (${decompressedBytes.size}) != decompressedLength (${compressionInfo.decompressedLength}).")

      case BlockCompressionInfo.Null =>
        //no compression just skip the header bytes.
        val unblockedReader = UnblockedReader.fromUncompressed(reader)
        if (readAllIfUncompressed)
          unblockedReader.readAllAndGetReader()
        else
          unblockedReader
    }
}
