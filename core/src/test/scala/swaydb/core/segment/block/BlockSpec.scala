package swaydb.core.segment.block

import swaydb.compression.CompressionInternal
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.segment.block.reader.BlockRefReader
import swaydb.core.segment.block.segment.transient.TransientSegmentRef
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockOffset}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}
import swaydb.data.slice.Slice
import swaydb.effect.IOAction
import swaydb.testkit.RunThis._

class BlockSpec extends TestBase {

  "decompressionAction" in {
    //uncompressed
    ValuesBlock(
      offset = ValuesBlockOffset(start = 0, size = 20),
      headerSize = 10,
      compressionInfo = BlockCompressionInfo.Null
    ).decompressionAction shouldBe IOAction.ReadUncompressedData(10)

    ValuesBlock(
      offset = ValuesBlockOffset(start = 10, size = 20),
      headerSize = 10,
      compressionInfo = BlockCompressionInfo.Null
    ).decompressionAction shouldBe IOAction.ReadUncompressedData(10)

    //compressed
    ValuesBlock(
      offset = ValuesBlockOffset(start = 0, size = 20),
      headerSize = 10,
      compressionInfo = BlockCompressionInfo(null, 200)
    ).decompressionAction shouldBe IOAction.ReadCompressedData(10, 200)

    ValuesBlock(
      offset = ValuesBlockOffset(start = 10, size = 20),
      headerSize = 10,
      compressionInfo = BlockCompressionInfo(null, 200)
    ).decompressionAction shouldBe IOAction.ReadCompressedData(10, 200)

    //uncompressed
    SegmentBlock(
      offset = SegmentBlockOffset(start = 0, size = 20),
      headerSize = 10,
      compressionInfo = BlockCompressionInfo.Null
    ).decompressionAction shouldBe IOAction.ReadUncompressedData(10)

    SegmentBlock(
      offset = SegmentBlockOffset(start = 10, size = 20),
      headerSize = 10,
      compressionInfo = BlockCompressionInfo.Null
    ).decompressionAction shouldBe IOAction.ReadUncompressedData(10)

    //compressed
    SegmentBlock(
      offset = SegmentBlockOffset(start = 0, size = 20),
      headerSize = 10,
      compressionInfo = BlockCompressionInfo(null, 200)
    ).decompressionAction shouldBe IOAction.ReadCompressedData(10, 200)

    SegmentBlock(
      offset = SegmentBlockOffset(start = 10, size = 20),
      headerSize = 10,
      compressionInfo = BlockCompressionInfo(null, 200)
    ).decompressionAction shouldBe IOAction.ReadCompressedData(10, 200)
  }

  "createCompressedHeader" in {
    runThis(10.times) {
      val compression = randomCompression()

      val data = Slice.fill[Byte](randomIntMax(10))(randomByte())

      //create header bytes
      val headerBytes =
        Block.createCompressedHeaderBytes(
          bytes = data,
          dataBlocksHeaderByteSize = 0,
          compression = compression
        )

      headerBytes.isOriginalFullSlice shouldBe true

      //read header
      import swaydb.core.segment.block.sortedindex.SortedIndexBlockOffset.SortedIndexBlockOps
      val header = Block.readHeader(BlockRefReader(headerBytes ++ data))

      header.headerSize shouldBe headerBytes.size
      header.compressionInfo.getS.decompressor shouldBe compression.decompressor
      header.offset.start shouldBe headerBytes.size
      header.offset.end shouldBe headerBytes.size + data.size - 1
      header.offset.size shouldBe data.size
    }
  }

  "createUnCompressedHeaderBytes" in {
    runThis(10.times) {
      val data = Slice.fill[Byte](randomIntMax(10))(randomByte())

      //create header bytes
      val headerBytes = Block.createUnCompressedHeaderBytes(dataBlocksHeaderByteSize = 0)
      headerBytes.isOriginalFullSlice shouldBe true

      //read header
      import swaydb.core.segment.block.sortedindex.SortedIndexBlockOffset.SortedIndexBlockOps
      val header = Block.readHeader(BlockRefReader(headerBytes ++ data))

      header.headerSize shouldBe headerBytes.size
      header.compressionInfo.isNoneS shouldBe true
      header.offset.start shouldBe headerBytes.size
      header.offset.end shouldBe headerBytes.size + data.size - 1
      header.offset.size shouldBe data.size
    }
  }

  "block & unblock" when {
    "no compression" should {
      "for bytes" in {
        runThis(100.times) {
          //          val headerSize = Block.headerSize(false) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val dataBytes = randomBytesSlice(10)

          val extraHeaderBytes = Slice.fill(randomIntMax(10))(randomByte())

          val compressionResult =
            Block.compress(
              bytes = dataBytes,
              dataBlocksHeaderByteSize = extraHeaderBytes.size,
              compressions = Seq.empty,
              blockName = "test-block"
            )

          //add the extra head bytes which gets inserted by the called
          compressionResult.headerBytes addAll extraHeaderBytes
          compressionResult.headerBytes.isOriginalFullSlice shouldBe true

          compressionResult.compressedBytes shouldBe Slice.Null
          val segmentBytes = compressionResult.headerBytes ++ dataBytes

          val expectedHeaderSize = Block.minimumHeaderSize(false) + extraHeaderBytes.size

          compressionResult.headerBytes.size shouldBe expectedHeaderSize
          compressionResult.headerBytes.size shouldBe compressionResult.headerBytes.size

          //read header
          val header = Block.readHeader[SegmentBlockOffset](BlockRefReader(segmentBytes))

          //assert default header information
          header.headerSize shouldBe (Block.minimumHeaderSize(false) + extraHeaderBytes.size)
          header.compressionInfo shouldBe BlockCompressionInfo.Null

          //if there are extra bytes then there is more header bytes to be read by the block
          header.headerReader.hasMore shouldBe extraHeaderBytes.nonEmpty
          extraHeaderBytes.foreach(byte => header.headerReader.get() shouldBe byte) //read the remaining bytes
          header.headerReader.hasMore shouldBe false //reader has been read.

          //create block reader
          def blockReader = Block.unblock[SegmentBlockOffset, SegmentBlock](BlockRefReader[SegmentBlockOffset](segmentBytes))

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
            new TransientSegmentRef(
              minKey = null,
              maxKey = null,

              functionMinMax = None,
              nearestDeadline = randomDeadlineOption(),

              updateCount = randomIntMax(),
              rangeCount = randomIntMax(),
              putCount = randomIntMax(),
              putDeadlineCount = randomIntMax(),
              keyValueCount = randomIntMax(),
              createdInLevel = randomIntMax(),

              valuesBlockHeader = Some(Slice.fill(headerSize)(0.toByte)),

              valuesBlock = randomBytesSliceOption(2),
              valuesUnblockedReader = None,
              sortedIndexClosedState = null,

              sortedIndexBlockHeader = Slice.fill(headerSize)(0.toByte),
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
          blockedSegment.flattenSegmentBytes.exists(_ != 0) shouldBe true
          val ref = BlockRefReader[SegmentBlockOffset](blockedSegment.flattenSegmentBytes)

          //read header
          val header = Block.readHeader(ref.copy())

          header.headerSize shouldBe headerSize
          header.compressionInfo.isNoneS shouldBe true
          header.headerReader.hasMore shouldBe false

          //create block reader
          def decompressedBlockReader = Block.unblock(ref.copy())

          val dataBytes = segment.segmentBytesWithoutHeader.flatten

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
          val compressedBytes = Block.compress(dataBytes, 0, Seq(compression), "test-block")
          //          compressedBytes.fixHeaderSize()

          compressedBytes.compressedBytes.isSomeC shouldBe true

          val ref =
            BlockRefReader[SegmentBlockOffset](
              bytes = compressedBytes.headerBytes ++ compressedBytes.compressedBytes.getOrElseC(dataBytes)
            )

          //read header
          val header = Block.readHeader(ref)

          header.headerSize should be > Block.minimumHeaderSize(false)
          header.compressionInfo.isSomeS shouldBe true
          header.compressionInfo.getS.decompressor.id shouldBe compression.decompressor.id

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
            new TransientSegmentRef(
              minKey = null,
              maxKey = null,

              functionMinMax = None,
              nearestDeadline = randomDeadlineOption(),

              updateCount = randomIntMax(),
              rangeCount = randomIntMax(),
              putCount = randomIntMax(),
              putDeadlineCount = randomIntMax(),
              keyValueCount = randomIntMax(),
              createdInLevel = randomIntMax(),

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
          compressedSegment.fileHeader shouldBe empty
          compressedSegment.bodyBytes should not be empty

          val ref = BlockRefReader[SegmentBlockOffset](compressedSegment.flattenSegmentBytes)

          //read header
          val header = Block.readHeader(ref)

          header.headerSize should be > Block.minimumHeaderSize(false)
          header.compressionInfo.isSomeS shouldBe true
          header.compressionInfo.getS.decompressor.id shouldBe compression.decompressor.id

          //create block reader
          def decompressedBlockReader = Block.unblock(ref.copy())

          val uncompressedSegmentBytesWithoutHeader = uncompressedSegment.segmentBytesWithoutHeader.flatten

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
      val dataBytes = Slice.of[Byte](300, true)
      val compression = randomCompression()
      val compressedBytes = Block.compress(dataBytes, 0, Seq(compression), "testBlock")

      compressedBytes.compressedBytes.isSomeC shouldBe true

      val segmentBytes = compressedBytes.headerBytes ++ compressedBytes.compressedBytes.getC

      if (compression == CompressionInternal.UnCompressed)
        segmentBytes shouldBe compressedBytes.headerBytes ++ dataBytes
      else
        segmentBytes.size should be < dataBytes.size

      val ref = BlockRefReader[SegmentBlockOffset](segmentBytes)

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
    val childCompressionResult1 = Block.compress(child1Bytes, 0, Seq(compression), "testBlock1")
    val compressedChildBytes1 = childCompressionResult1.headerBytes ++ childCompressionResult1.compressedBytes.getC

    val childCompressionResult2 = Block.compress(child2Bytes, 0, Seq(compression), "testBlock2")
    val compressedChildBytes2 = childCompressionResult2.headerBytes ++ childCompressionResult2.compressedBytes.getC

    //merge compressed child blocks and write them to a root block
    //fill with header bytes.
    val allChildBytes = compressedChildBytes1 ++ compressedChildBytes2

    val compressionResult = Block.compress(allChildBytes, 0, Seq(compression), "compressedRootBlocks")
    val compressionBytes = compressionResult.headerBytes ++ compressionResult.compressedBytes.getC

    val rootRef = BlockRefReader[ValuesBlockOffset](compressionBytes)
    val decompressedRootBlock = Block.unblock(rootRef)

    //got the original rootBlock bytes without the header.
    val decompressedRoot = decompressedRootBlock.readAllAndGetReader()
    decompressedRoot.readFullBlock() shouldBe allChildBytes

    val child1Ref = BlockRefReader.moveTo[ValuesBlockOffset, ValuesBlockOffset](0, compressedChildBytes1.size, decompressedRoot.copy(), None)
    val child1DecompressedBytes = Block.unblock(child1Ref)
    child1DecompressedBytes.readFullBlock() shouldBe child1Bytes

    val child2Ref = BlockRefReader.moveTo[ValuesBlockOffset, ValuesBlockOffset](child1Ref.size, compressedChildBytes2.size, decompressedRoot.copy(), None)
    val child2DecompressedBytes = Block.unblock(child2Ref)
    child2DecompressedBytes.readFullBlock() shouldBe child2Bytes
  }
}
