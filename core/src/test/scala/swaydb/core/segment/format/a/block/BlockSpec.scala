package swaydb.core.segment.format.a.block

import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.segment.format.a.block.Block.CompressionInfo
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.data.config.IOAction
import swaydb.data.slice.Slice
import swaydb.ErrorHandler.CoreErrorHandler

class BlockSpec extends TestBase {

  "dataType" in {
    //uncompressed
    ValuesBlock(ValuesBlock.Offset(start = 0, size = 20), headerSize = 10, compressionInfo = None).dataType shouldBe IOAction.ReadUncompressedData(10)
    ValuesBlock(ValuesBlock.Offset(start = 10, size = 20), headerSize = 10, compressionInfo = None).dataType shouldBe IOAction.ReadUncompressedData(10)

    //compressed
    ValuesBlock(ValuesBlock.Offset(start = 0, size = 20), headerSize = 10, compressionInfo = Some(CompressionInfo(null, 200))).dataType shouldBe IOAction.ReadCompressedData(10, 200)
    ValuesBlock(ValuesBlock.Offset(start = 10, size = 20), headerSize = 10, compressionInfo = Some(CompressionInfo(null, 200))).dataType shouldBe IOAction.ReadCompressedData(10, 200)

    //uncompressed
    SegmentBlock(SegmentBlock.Offset(start = 0, size = 20), headerSize = 10, compressionInfo = None).dataType shouldBe IOAction.ReadUncompressedData(10)
    SegmentBlock(SegmentBlock.Offset(start = 10, size = 20), headerSize = 10, compressionInfo = None).dataType shouldBe IOAction.ReadUncompressedData(10)

    //compressed
    SegmentBlock(SegmentBlock.Offset(start = 0, size = 20), headerSize = 10, compressionInfo = Some(CompressionInfo(null, 200))).dataType shouldBe IOAction.ReadCompressedData(10, 200)
    SegmentBlock(SegmentBlock.Offset(start = 10, size = 20), headerSize = 10, compressionInfo = Some(CompressionInfo(null, 200))).dataType shouldBe IOAction.ReadCompressedData(10, 200)
  }

  "block & unblock" when {
    "no compression" should {
      "for bytes" in {
        runThis(100.times) {
          val headerSize = Block.headerSize(false) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val dataBytes = randomBytesSlice(randomIntMax(100) + 1)
          val uncompressedBytes = Slice.fill(headerSize)(0.toByte) ++ dataBytes

          val compressedBytes = Block.block(headerSize, uncompressedBytes, Seq.empty, "test-block").get

          compressedBytes.size shouldBe uncompressedBytes.size
          compressedBytes.underlyingArraySize shouldBe uncompressedBytes.underlyingArraySize
          compressedBytes.hashCode() shouldBe uncompressedBytes.hashCode() //same object - mutated!

          uncompressedBytes.drop(headerSize) shouldBe dataBytes

          //read header
          val header = Block.readHeader[SegmentBlock.Offset](BlockRefReader(uncompressedBytes)).get

          header.headerSize shouldBe headerSize
          header.compressionInfo shouldBe empty
          header.headerReader.hasMore.get shouldBe false

          //create block reader
          def blockReader = Block.unblock[SegmentBlock.Offset, SegmentBlock](BlockRefReader[SegmentBlock.Offset](uncompressedBytes)).get

          blockReader.readRemaining().get shouldBe dataBytes
          blockReader.read(Int.MaxValue).get shouldBe dataBytes
          blockReader.readAll().get shouldBe dataBytes
          blockReader.readAllAndGetReader().get.readRemaining().get shouldBe dataBytes
        }
      }

      "for segment" in {
        runThis(100.times) {
          val headerSize = Block.headerSize(false) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val segment =
            SegmentBlock.Open(
              headerBytes = Slice.fill(headerSize)(0.toByte),
              footerBlock = randomBytesSlice(),
              valuesBlock = randomBytesSliceOption(2),
              sortedIndexBlock = randomBytesSlice(2),
              hashIndexBlock = randomBytesSliceOption(2),
              binarySearchIndexBlock = randomBytesSliceOption(2),
              bloomFilterBlock = randomBytesSliceOption(2),
              functionMinMax = None,
              nearestDeadline = randomDeadlineOption()
            )

          val blockedSegment = Block.block(segment, Seq.empty, "test-segment-block").get

          //first slice gets written
          blockedSegment.segmentBytes.head.exists(_ != 0) shouldBe true
          val ref = BlockRefReader[SegmentBlock.Offset](blockedSegment.flattenSegmentBytes)

          //read header
          val header = Block.readHeader(ref.copy()).get

          header.headerSize shouldBe headerSize
          header.compressionInfo shouldBe empty
          header.headerReader.hasMore.get shouldBe false

          //create block reader
          def decompressedBlockReader = Block.unblock(ref.copy()).get

          val dataBytes = segment.segmentBytes.dropHead().flatten.toSlice

          decompressedBlockReader.readRemaining().get shouldBe dataBytes
          decompressedBlockReader.read(Int.MaxValue).get shouldBe dataBytes
          decompressedBlockReader.readAll().get shouldBe dataBytes
          decompressedBlockReader.readAllAndGetReader().get.readRemaining().get shouldBe dataBytes
        }
      }
    }

    "has compression" should {
      "for bytes" in {
        runThis(100.times) {
          val headerSize = Block.headerSize(true) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val dataBytes = randomBytesSlice(randomIntMax(100) + 1)
          val uncompressedBytes = Slice.fill(headerSize)(0.toByte) ++ dataBytes

          val compression = randomCompressions().head
          val compressedBytes = Block.block(headerSize, uncompressedBytes, Seq(compression), "test-block").get
          compressedBytes.hashCode() should not be uncompressedBytes.hashCode() //different objects

          val ref = BlockRefReader[SegmentBlock.Offset](compressedBytes)

          //read header
          val header = Block.readHeader(ref).get

          header.headerSize shouldBe headerSize
          header.compressionInfo shouldBe defined
          header.compressionInfo.get.decompressor.id shouldBe compression.decompressor.id

          //create block reader
          def blockReader = Block.unblock(ref.copy()).get

          blockReader.readRemaining().get shouldBe dataBytes
          blockReader.read(Int.MaxValue).get shouldBe dataBytes
          blockReader.readAll().get shouldBe dataBytes
          blockReader.readAllAndGetReader().get.readRemaining().get shouldBe dataBytes
        }
      }

      "for segment" in {
        runThis(100.times) {
          val headerSize = Block.headerSize(true) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val uncompressedSegment =
            SegmentBlock.Open(
              headerBytes = Slice.fill(headerSize)(0.toByte),
              valuesBlock = randomBytesSliceOption(randomIntMax(100) + 1),
              sortedIndexBlock = randomBytesSlice(randomIntMax(100) + 1),
              hashIndexBlock = randomBytesSliceOption(randomIntMax(100) + 1),
              binarySearchIndexBlock = randomBytesSliceOption(randomIntMax(100) + 1),
              bloomFilterBlock = randomBytesSliceOption(randomIntMax(100) + 1),
              footerBlock = randomBytesSlice(randomIntMax(100) + 1),
              functionMinMax = None,
              nearestDeadline = randomDeadlineOption()
            )

          val compression = randomCompressions().head

          val compressedSegment = Block.block(uncompressedSegment, Seq(compression), "test-segment-block").get

          compressedSegment.hashCode() should not be uncompressedSegment.hashCode() //different object, because it's compressed.
          compressedSegment.segmentBytes should have size 1 //compressed

          val ref = BlockRefReader[SegmentBlock.Offset](compressedSegment.flattenSegmentBytes)

          //read header
          val header = Block.readHeader(ref).get

          header.headerSize shouldBe headerSize
          header.compressionInfo shouldBe defined
          header.compressionInfo.get.decompressor.id shouldBe compression.decompressor.id

          //create block reader
          def decompressedBlockReader = Block.unblock(ref.copy()).get

          val uncompressedSegmentBytesWithoutHeader = uncompressedSegment.segmentBytes.dropHead().flatten.toSlice

          decompressedBlockReader.readRemaining().get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.read(Int.MaxValue).get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.readAll().get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.readAllAndGetReader().get.readRemaining().get shouldBe uncompressedSegmentBytesWithoutHeader
        }
      }
    }
  }

  "unblock" in {
    val bytes = Slice.create[Byte](30, true)
    val headerSize = 10
    val compression = randomCompression()
    val compressedBytes = Block.block(headerSize, bytes, Seq(compression), "testBlock").get

    val ref = BlockRefReader[SegmentBlock.Offset](compressedBytes)

    val decompressedBlock = Block.unblock(ref).get
    decompressedBlock.readAll().get shouldBe bytes.drop(headerSize)
  }

  "unblocking nested compressed block" in {
    /**
      * This test compressed 2 blocks into a root block and then decompresses.
      * Format: { rootBlock: {childBlock1: header+child1Bytes} {childBlock2: header+child2Bytes} }
      */

    val child1Bytes = Slice.fill[Byte](20)(1.toByte)
    val child2Bytes = Slice.fill[Byte](20)(2.toByte)

    val headerSize = 10

    def compression = randomCompression()

    //compress both child blocks
    val compressedChildBytes1 = Block.block(headerSize, child1Bytes, Seq(compression), "testBlock1").get
    val compressedChildBytes2 = Block.block(headerSize, child2Bytes, Seq(compression), "testBlock2").get

    //merge compressed child blocks and write them to a root block
    //fill with header bytes.
    val rootUncompressedBytes = Slice.fill[Byte](headerSize)(3.toByte) ++ compressedChildBytes1 ++ compressedChildBytes2
    val compressedRootBytes = Block.block(headerSize, rootUncompressedBytes, Seq(compression), "compressedRootBlocks").get

    val rootRef = BlockRefReader[SegmentBlock.Offset](compressedRootBytes)
    val decompressedRootBlock = Block.unblock(rootRef).get

    //got the original rootBlock bytes without the header.
    val decompressedRoot = decompressedRootBlock.readAllAndGetReader().get
    decompressedRoot.readAll().get shouldBe rootUncompressedBytes.drop(headerSize)

    val child1Ref = BlockRefReader.moveTo[SegmentBlock.Offset, SegmentBlock](SegmentBlock.Offset(0, compressedChildBytes1.size), decompressedRoot.copy())
    val child1DecompressedBytes = Block.unblock(child1Ref).get
    child1DecompressedBytes.readAll().get shouldBe child1Bytes.drop(headerSize)

    //read child block2 using the same decompressed reader from root block but set the offset start to be the end of child1's compressed bytes end.
    //    decompressedRootBytes.readAll().get.drop(compressedChildBytes1.size)
    //    val child2Ref = BlockRefReader.moveTo[SegmentBlock.Offset, SegmentBlock](SegmentBlock.Offset(compressedChildBytes1.size, compressedChildBytes2.size), decompressedRoot.copy())
    //    val child2DecompressedBytes = Block.unblock(child2Ref).get
    //    child2DecompressedBytes.readAll().get shouldBe child2Bytes.drop(headerSize)
  }
}
