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
import swaydb.core.compression.{CompressionInternal, DecompressorInternal}
import swaydb.core.file.reader.Reader
import swaydb.core.segment.block.reader.{BlockRefReader, BlockedReader, UnblockedReader}
import swaydb.core.segment.block.segment.transient.{TransientSegment, TransientSegmentRef}
import swaydb.core.util.Bytes
import swaydb.effect.IOAction
import swaydb.slice.{ReaderBase, Slice, SliceMut}
import swaydb.utils.ByteSizeOf
import swaydb.utils.Collections._

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
      headerSizeWithCompression
    else
      headerSizeNoCompression

  /**
   * Creates the header byte array and inserts the compression information
   *
   * @param bytes                    Bytes compressed
   * @param dataBlocksHeaderByteSize extra header bytes required by the data-byte
   * @param compression              compression applied
   * @return header bytes
   */
  def createCompressedHeaderBytes(bytes: Slice[Byte],
                                  dataBlocksHeaderByteSize: Int,
                                  compression: CompressionInternal): SliceMut[Byte] = {
    val requiredByteSize =
      ByteSizeOf.byte + //headerSize
        ByteSizeOf.byte + //formatId
        Bytes.sizeOfUnsignedInt(compression.decompressor.id) +
        Bytes.sizeOfUnsignedInt(bytes.size) +
        dataBlocksHeaderByteSize

    val header = Slice.of[Byte](requiredByteSize)

    header addUnsignedInt (requiredByteSize - 1)
    header add BlockHeader.compressedBlockID
    header addUnsignedInt compression.decompressor.id
    header addUnsignedInt bytes.size //decompressed bytes
  }

  /**
   * Creates the header byte array and inserts the un-compression information
   *
   * @param dataBlocksHeaderByteSize extra header bytes required by the data-byte
   * @return header bytes
   */
  @inline def createUnCompressedHeaderBytes(dataBlocksHeaderByteSize: Int): SliceMut[Byte] = {
    val requiredBytes = headerSizeNoCompression + dataBlocksHeaderByteSize

    val header = Slice.of[Byte](requiredBytes)

    header addUnsignedInt (requiredBytes - 1)
    header add BlockHeader.uncompressedBlockId
  }

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
   * @param bytes                    Bytes to compress
   * @param dataBlocksHeaderByteSize Extra header bytes required to store data block's header byte size. The data block
   *                                 caller has this information.
   * @param compressions             Compressions to apply
   * @param blockName                Name of the block being compressed.
   */
  def compress(bytes: Slice[Byte],
               dataBlocksHeaderByteSize: Int,
               compressions: Iterable[CompressionInternal],
               blockName: String): BlockCompressionResult =
    compressions.untilSome(_.compressor.compress(bytes)) match {
      case Some((compressedBytes, compression)) =>

        new BlockCompressionResult(
          compressedBytes = compressedBytes,
          headerBytes =
            createCompressedHeaderBytes(
              bytes = bytes,
              dataBlocksHeaderByteSize = dataBlocksHeaderByteSize,
              compression = compression
            )
        )

      case None =>
        logger.trace {
          if (compressions.isEmpty)
            s"No compression strategies provided for $blockName. Storing ${bytes.size}.bytes uncompressed."
          else
            s"Unable to satisfy compression requirement from ${compressions.size} compression strategies for $blockName. Storing ${bytes.size}.bytes uncompressed."
        }

        new BlockCompressionResult(
          compressedBytes = Slice.Null,
          headerBytes = createUnCompressedHeaderBytes(dataBlocksHeaderByteSize)
        )
    }

  def block(segment: TransientSegmentRef,
            compressions: Iterable[CompressionInternal],
            blockName: String): TransientSegment.One =
    if (compressions.isEmpty) {
      logger.trace(s"No compression strategies provided for Segment level compression for $blockName. Storing ${segment.segmentSizeWithoutHeader}.bytes uncompressed.")

      val header = createUnCompressedHeaderBytes(0)

      val segmentBytes: Slice[Slice[Byte]] =
        segment.segmentBytesWithoutHeader.collectToSlice(header) {
          case bytes if bytes.nonEmpty =>
            bytes.close().asMut()
        }

      TransientSegment.One(
        minKey = segment.minKey,
        maxKey = segment.maxKey,
        fileHeader = Slice.emptyBytes,
        bodyBytes = segmentBytes,
        minMaxFunctionId = segment.functionMinMax,
        nearestPutDeadline = segment.nearestDeadline,
        updateCount = segment.updateCount,
        rangeCount = segment.rangeCount,
        putCount = segment.putCount,
        putDeadlineCount = segment.putDeadlineCount,
        keyValueCount = segment.keyValueCount,
        createdInLevel = segment.createdInLevel,
        valuesUnblockedReader = segment.valuesUnblockedReader,
        sortedIndexUnblockedReader = segment.sortedIndexUnblockedReader,
        hashIndexUnblockedReader = segment.hashIndexUnblockedReader,
        binarySearchUnblockedReader = segment.binarySearchUnblockedReader,
        bloomFilterUnblockedReader = segment.bloomFilterUnblockedReader,
        footerUnblocked = segment.footerUnblocked
      )
    } else {
      //header is empty so no header bytes are included.
      val uncompressedSegmentBytes = segment.flattenSegmentBytesWithoutHeader

      val compressionResult =
        Block.compress(
          bytes = uncompressedSegmentBytes,
          dataBlocksHeaderByteSize = 0,
          compressions = compressions,
          blockName = blockName
        )

      val compressedOrUncompressedSegmentBytes =
        compressionResult.compressedBytes getOrElseC uncompressedSegmentBytes

      TransientSegment.One(
        minKey = segment.minKey,
        maxKey = segment.maxKey,
        fileHeader = Slice.emptyBytes,
        bodyBytes = Slice(Array(compressionResult.headerBytes, compressedOrUncompressedSegmentBytes)),
        minMaxFunctionId = segment.functionMinMax,
        nearestPutDeadline = segment.nearestDeadline,
        updateCount = segment.updateCount,
        rangeCount = segment.rangeCount,
        putCount = segment.putCount,
        putDeadlineCount = segment.putDeadlineCount,
        keyValueCount = segment.keyValueCount,
        createdInLevel = segment.createdInLevel,
        valuesUnblockedReader = segment.valuesUnblockedReader,
        sortedIndexUnblockedReader = segment.sortedIndexUnblockedReader,
        hashIndexUnblockedReader = segment.hashIndexUnblockedReader,
        binarySearchUnblockedReader = segment.binarySearchUnblockedReader,
        bloomFilterUnblockedReader = segment.bloomFilterUnblockedReader,
        footerUnblocked = segment.footerUnblocked
      )
    }

  private def readCompressionInfo(formatID: Int,
                                  headerSize: Int,
                                  reader: ReaderBase): BlockCompressionInfoOption =
    if (formatID == BlockHeader.compressedBlockID)
      new BlockCompressionInfo(
        decompressor = DecompressorInternal(reader.readUnsignedInt()),
        decompressedLength = reader.readUnsignedInt()
      )
    else if (formatID == BlockHeader.uncompressedBlockId)
      BlockCompressionInfo.Null
    else
      throw swaydb.Exception.InvalidDataId(
        id = formatID,
        message = s"Invalid formatID: $formatID. Expected: ${BlockHeader.uncompressedBlockId} or ${BlockHeader.compressedBlockID}"
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

  @inline def unblock[O <: BlockOffset, B <: Block[O]](bytes: Slice[Byte])(implicit blockOps: BlockOps[O, B]): UnblockedReader[O, B] =
    unblock(BlockRefReader(bytes))

  @inline def unblock[O <: BlockOffset, B <: Block[O]](ref: BlockRefReader[O],
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
