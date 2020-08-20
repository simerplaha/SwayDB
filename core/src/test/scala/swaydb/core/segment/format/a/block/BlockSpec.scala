package swaydb.core.segment.format.a.block

import org.scalatest.OptionValues._
import swaydb.compression.CompressionInternal
import swaydb.data.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.segment.format.a.block.Block.CompressionInfo
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.segment.data.ClosedBlocksWithFooter
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.data.config.IOAction
import swaydb.data.slice.Slice

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
          //          val headerSize = Block.headerSize(false) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val dataBytes = randomBytesSlice(10)

          val compressionResult = Block.compress(dataBytes, Seq.empty, "test-block")

          compressionResult.compressedBytes shouldBe empty
          //          compressionResult.underlyingArraySize shouldBe uncompressedBytes.underlyingArraySize
          //          compressionResult.hashCode() shouldBe uncompressedBytes.hashCode() //same object - mutated!

          //          uncompressedBytes.drop(headerSize) shouldBe dataBytes

          compressionResult.fixHeaderSize()

          val segmentBytes = compressionResult.headerBytes ++ dataBytes

          //read header
          val header = Block.readHeader[SegmentBlock.Offset](BlockRefReader(segmentBytes))

          header.headerSize shouldBe Block.minimumHeaderSize(false)
          header.compressionInfo shouldBe empty
          header.headerReader.hasMore shouldBe false

          //create block reader
          def blockReader = Block.unblock[SegmentBlock.Offset, SegmentBlock](BlockRefReader[SegmentBlock.Offset](segmentBytes))

          blockReader.readRemaining() shouldBe dataBytes
          blockReader.read(Int.MaxValue) shouldBe dataBytes
          blockReader.readFullBlock() shouldBe dataBytes
          blockReader.readAllAndGetReader().readRemaining() shouldBe dataBytes
        }
      }

      "for segment" in {
        runThis(100.times) {
          val headerSize = Block.minimumHeaderSize(false)
          val segment =
            new ClosedBlocksWithFooter(
              minKey = null,
              maxKey = null,

              functionMinMax = None,
              nearestDeadline = randomDeadlineOption(),
              valuesBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              valuesBlock = randomBytesSliceOption(2),
              valuesUnblockedReader = None,
              sortedIndexBlockHeader = Slice.fill(headerSize)(0.toByte),

              sortedIndexClosedState = null,
              sortedIndexBlock = randomBytesSlice(2),
              sortedIndexUnblockedReader = None,
              hashIndexBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              hashIndexBlock = randomBytesSliceOption(2),
              hashIndexUnblockedReader = None,
              binarySearchIndexBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              binarySearchIndexBlock = randomBytesSliceOption(2),
              binarySearchUnblockedReader = None,
              bloomFilterBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              bloomFilterBlock = randomBytesSliceOption(2),

              bloomFilterUnblockedReader = None,
              footerBlock = randomBytesSlice()
            )

          val blockedSegment = Block.block(segment, Seq.empty, "test-segment-block")

          //first slice gets written
          blockedSegment.segmentBytes.head.exists(_ != 0) shouldBe true
          val ref = BlockRefReader[SegmentBlock.Offset](blockedSegment.flattenSegmentBytes)

          //read header
          val header = Block.readHeader(ref.copy())

          header.headerSize shouldBe headerSize
          header.compressionInfo shouldBe empty
          header.headerReader.hasMore shouldBe false

          //create block reader
          def decompressedBlockReader = Block.unblock(ref.copy())

          val dataBytes = segment.segmentBytes.dropHead().flatten.toSlice

          decompressedBlockReader.readRemaining() shouldBe dataBytes
          decompressedBlockReader.read(Int.MaxValue) shouldBe dataBytes
          decompressedBlockReader.readFullBlock() shouldBe dataBytes
          decompressedBlockReader.readAllAndGetReader().readRemaining() shouldBe dataBytes
        }
      }
    }

    "has compression" should {
      "for bytes" in {
        runThis(100.times) {
          val dataBytes = randomBytesSlice(randomIntMax(100) + 1)
          //          val uncompressedBytes = Slice.fill(headerSize)(0.toByte) ++ dataBytes

          val compression = randomCompressions().head
          val compressedBytes = Block.compress(dataBytes, Seq(compression), "test-block")
          compressedBytes.fixHeaderSize()

          compressedBytes.compressedBytes shouldBe defined

          val ref = BlockRefReader[SegmentBlock.Offset](compressedBytes.headerBytes ++ compressedBytes.compressedBytes.getOrElse(dataBytes))

          //read header
          val header = Block.readHeader(ref)

          header.headerSize should be > Block.minimumHeaderSize(false)
          header.compressionInfo shouldBe defined
          header.compressionInfo.value.decompressor.id shouldBe compression.decompressor.id

          //create block reader
          def blockReader = Block.unblock(ref.copy())

          blockReader.readRemaining() shouldBe dataBytes
          blockReader.read(Int.MaxValue) shouldBe dataBytes
          blockReader.readFullBlock() shouldBe dataBytes
          blockReader.readAllAndGetReader().readRemaining() shouldBe dataBytes
        }
      }

      "for segment" in {
        runThis(100.times) {
          val headerSize = Block.minimumHeaderSize(true) //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val uncompressedSegment =
            new ClosedBlocksWithFooter(
              minKey = null,
              maxKey = null,

              functionMinMax = None,
              nearestDeadline = randomDeadlineOption(),
              valuesBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              valuesBlock = randomBytesSliceOption(randomIntMax(100) + 1),
              valuesUnblockedReader = None,
              sortedIndexBlockHeader = Slice.fill(headerSize)(0.toByte),

              sortedIndexClosedState = null,
              sortedIndexBlock = randomBytesSlice(randomIntMax(100) + 1),
              sortedIndexUnblockedReader = None,
              hashIndexBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              hashIndexBlock = randomBytesSliceOption(randomIntMax(100) + 1),
              hashIndexUnblockedReader = None,
              binarySearchIndexBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              binarySearchIndexBlock = randomBytesSliceOption(randomIntMax(100) + 1),
              binarySearchUnblockedReader = None,
              bloomFilterBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              bloomFilterBlock = randomBytesSliceOption(randomIntMax(100) + 1),

              bloomFilterUnblockedReader = None,
              footerBlock = randomBytesSlice(randomIntMax(100) + 1)
            )

          val compression = randomCompressions().head

          val compressedSegment = Block.block(uncompressedSegment, Seq(compression), "test-segment-block")

          compressedSegment.hashCode() should not be uncompressedSegment.hashCode() //different object, because it's compressed.
          compressedSegment.segmentBytes should have size 2 //compressed

          val ref = BlockRefReader[SegmentBlock.Offset](compressedSegment.flattenSegmentBytes)

          //read header
          val header = Block.readHeader(ref)

          header.headerSize should be > Block.minimumHeaderSize(false)
          header.compressionInfo shouldBe defined
          header.compressionInfo.value.decompressor.id shouldBe compression.decompressor.id

          //create block reader
          def decompressedBlockReader = Block.unblock(ref.copy())

          val uncompressedSegmentBytesWithoutHeader = uncompressedSegment.segmentBytes.dropHead().flatten.toSlice

          decompressedBlockReader.readRemaining() shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.read(Int.MaxValue) shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.readFullBlock() shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.readAllAndGetReader().readRemaining() shouldBe uncompressedSegmentBytesWithoutHeader
        }
      }
    }
  }

  "unblock" in {
    runThis(100.times) {
      val dataBytes = Slice.create[Byte](300, true)
      val compression = randomCompression()
      val compressedBytes = Block.compress(dataBytes, Seq(compression), "testBlock")
      compressedBytes.fixHeaderSize()

      compressedBytes.compressedBytes shouldBe defined

      val segmentBytes = compressedBytes.headerBytes ++ compressedBytes.compressedBytes.get

      if (compression == CompressionInternal.UnCompressed)
        segmentBytes shouldBe compressedBytes.headerBytes ++ dataBytes
      else
        segmentBytes.size should be < dataBytes.size

      val ref = BlockRefReader[SegmentBlock.Offset](segmentBytes)

      val decompressedBlock = Block.unblock(ref)
      decompressedBlock.readFullBlock() shouldBe dataBytes
    }
  }

  "unblocking nested compressed block" in {
    /**
     * This test compressed 2 blocks into a root block and then decompresses.
     * Format: { rootBlock: {childBlock1: header+child1Bytes} {childBlock2: header+child2Bytes} }
     */

    val child1Bytes = Slice.fill[Byte](20)(1.toByte)
    val child2Bytes = Slice.fill[Byte](20)(2.toByte)

    def compression = randomCompression()

    //compress both child blocks
    val childCompressionResult1 = Block.compress(child1Bytes, Seq(compression), "testBlock1")
    childCompressionResult1.fixHeaderSize()
    val compressedChildBytes1 = childCompressionResult1.headerBytes ++ childCompressionResult1.compressedBytes.get

    val childCompressionResult2 = Block.compress(child2Bytes, Seq(compression), "testBlock2")
    childCompressionResult2.fixHeaderSize()
    val compressedChildBytes2 = childCompressionResult2.headerBytes ++ childCompressionResult2.compressedBytes.get

    //merge compressed child blocks and write them to a root block
    //fill with header bytes.
    val allChildBytes = compressedChildBytes1 ++ compressedChildBytes2

    val compressionResult = Block.compress(allChildBytes, Seq(compression), "compressedRootBlocks")
    compressionResult.fixHeaderSize()
    val compressionBytes = compressionResult.headerBytes ++ compressionResult.compressedBytes.get

    val rootRef = BlockRefReader[ValuesBlock.Offset](compressionBytes)
    val decompressedRootBlock = Block.unblock(rootRef)

    //got the original rootBlock bytes without the header.
    val decompressedRoot = decompressedRootBlock.readAllAndGetReader()
    decompressedRoot.readFullBlock() shouldBe allChildBytes

    val child1Ref = BlockRefReader.moveTo[ValuesBlock.Offset, ValuesBlock.Offset](0, compressedChildBytes1.size, decompressedRoot.copy())
    val child1DecompressedBytes = Block.unblock(child1Ref)
    child1DecompressedBytes.readFullBlock() shouldBe child1Bytes

    val child2Ref = BlockRefReader.moveTo[ValuesBlock.Offset, ValuesBlock.Offset](child1Ref.size.toInt, compressedChildBytes2.size, decompressedRoot.copy())
    val child2DecompressedBytes = Block.unblock(child2Ref)
    child2DecompressedBytes.readFullBlock() shouldBe child2Bytes
  }
}
