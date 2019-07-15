package swaydb.core.segment.format.a.block

import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.CompressedBlockReader._
import swaydb.core.segment.format.a.block.reader.DecompressedBlockReader
import swaydb.data.slice.Slice

class BlockSpec extends TestBase {

  "create" when {
    "no compression for some random bytes" in {
      runThis(100.times) {
        val headerSize = Block.headerSize(false) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
        val dataBytes = randomBytesSlice(randomIntMax(100) + 1)
        val uncompressedBytes = Slice.fill(headerSize)(0.toByte) ++ dataBytes

        val compressedBytes = Block.compress(headerSize, uncompressedBytes, Seq.empty, "test-block").get

        compressedBytes.size shouldBe uncompressedBytes.size
        compressedBytes.underlyingArraySize shouldBe uncompressedBytes.underlyingArraySize
        compressedBytes.hashCode() shouldBe uncompressedBytes.hashCode() //same object - mutated!

        uncompressedBytes.drop(headerSize) shouldBe dataBytes

        val offset =
          new BlockOffset {
            override def start: Int = 0
            override def size: Int = uncompressedBytes.size
          }

        //read header
        val header =
          Block.readHeader(
            offset = offset,
            reader = Reader(uncompressedBytes)
          ).get

        header.headerSize shouldBe headerSize
        header.compressionInfo shouldBe empty
        header.headerReader.hasMore.get shouldBe false

        //create block reader
        def blockReader =
          Block.decompress(
            childBlock = ValuesBlock(ValuesBlock.Offset(0, uncompressedBytes.size), headerSize, None),
            readAllIfUncompressed = randomBoolean(),
            parentBlock = SegmentBlock.decompressed(uncompressedBytes)
          ).get

        blockReader.readRemaining().get shouldBe dataBytes
        blockReader.read(Int.MaxValue).get shouldBe dataBytes
        blockReader.readAll().get shouldBe dataBytes
        blockReader.readAllAndGetReader().get.readRemaining().get shouldBe dataBytes
      }
    }

    "no compression for some segment bytes bytes" in {
      runThis(1.times) {
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

        val uncompressedBytes = segment.flattenSegmentBytes

        val compressedSegment = Block.compress(segment, Seq.empty, "test-segment-block").get

        //first slice gets written
        compressedSegment.segmentBytes.head.exists(_ != 0) shouldBe true

        val offset =
          new BlockOffset {
            override def start: Int = 0
            override def size: Int = segment.flattenSegmentBytes.size
          }

        //read header
        val header =
          Block.readHeader(
            offset = offset,
            reader = Reader(segment.flattenSegmentBytes)
          ).get

        header.headerSize shouldBe headerSize
        header.compressionInfo shouldBe empty
        header.headerReader.hasMore.get shouldBe false

        //create block reader
        def decompressedBlockReader =
          Block.decompress(
            childBlock = ValuesBlock(ValuesBlock.Offset(0, uncompressedBytes.size), headerSize, None),
            readAllIfUncompressed = randomBoolean(),
            parentBlock = SegmentBlock.decompressed(uncompressedBytes)
          ).get

        val dataBytes = segment.segmentBytes.dropHead().flatten.toSlice

        decompressedBlockReader.readRemaining().get shouldBe dataBytes
        decompressedBlockReader.read(Int.MaxValue).get shouldBe dataBytes
        decompressedBlockReader.readAll().get shouldBe dataBytes
        decompressedBlockReader.readAllAndGetReader().get.readRemaining().get shouldBe dataBytes
      }
    }

    "has compression" when {
      "some random bytes" in {
        runThis(100.times) {
          val headerSize = Block.headerSize(true) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val dataBytes = randomBytesSlice(randomIntMax(100) + 1)
          val uncompressedBytes = Slice.fill(headerSize)(0.toByte) ++ dataBytes

          val compression = randomCompressions().head
          val compressedBytes = Block.compress(headerSize, uncompressedBytes, Seq(compression), "test-block").get
          compressedBytes.hashCode() should not be uncompressedBytes.hashCode() //different objects

          val offset =
            new BlockOffset {
              override def start: Int = 0
              override def size: Int = compressedBytes.size
            }

          //read header
          val header =
            Block.readHeader(
              offset = offset,
              reader = Reader(compressedBytes)
            ).get

          header.headerSize shouldBe headerSize
          header.compressionInfo shouldBe defined
          header.compressionInfo.get.decompressor.id shouldBe compression.decompressor.id

          //create block reader
          def blockReader =
            Block.decompress(
              childBlock = ValuesBlock(ValuesBlock.Offset(0, compressedBytes.size), headerSize, header.compressionInfo),
              readAllIfUncompressed = randomBoolean(),
              parentBlock = SegmentBlock.decompressed(compressedBytes)
            ).get

          blockReader.readRemaining().get shouldBe dataBytes
          blockReader.read(Int.MaxValue).get shouldBe dataBytes
          blockReader.readAll().get shouldBe dataBytes
          blockReader.readAllAndGetReader().get.readRemaining().get shouldBe dataBytes
        }
      }

      "segment bytes" in {
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

          val compressedSegment = Block.compress(uncompressedSegment, Seq(compression), "test-segment-block").get

          compressedSegment.hashCode() should not be uncompressedSegment.hashCode() //different object, because it's compressed.
          compressedSegment.segmentBytes should have size 1 //compressed

          val offset =
            new BlockOffset {
              override def start: Int = 0
              override def size: Int = compressedSegment.segmentSize
            }

          //read header
          val header =
            Block.readHeader(
              offset = offset,
              reader = Reader(compressedSegment.flattenSegmentBytes)
            ).get

          header.headerSize shouldBe headerSize
          header.compressionInfo shouldBe defined
          header.compressionInfo.get.decompressor.id shouldBe compression.decompressor.id

          //create block reader
          def decompressedBlockReader =
            Block.decompress(
              childBlock = ValuesBlock(ValuesBlock.Offset(0, compressedSegment.segmentSize), headerSize, header.compressionInfo),
              readAllIfUncompressed = randomBoolean(),
              parentBlock = SegmentBlock.decompressed(compressedSegment.flattenSegmentBytes)
            ).get

          val uncompressedSegmentBytesWithoutHeader = uncompressedSegment.segmentBytes.dropHead().flatten.toSlice

          decompressedBlockReader.readRemaining().get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.read(Int.MaxValue).get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.readAll().get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.readAllAndGetReader().get.readRemaining().get shouldBe uncompressedSegmentBytesWithoutHeader
        }
      }
    }
  }

  "decompressing root block" in {
    val bytes = Slice.create[Byte](30, true)
    val headerSize = 10
    val compression = randomCompression()
    val compressedBytes = Block.compress(headerSize, bytes, Seq(compression), "testBlock").get

    println(compressedBytes)
    val header =
      Block.readHeader(
        offset = ValuesBlock.Offset(0, compressedBytes.size),
        reader = Reader(compressedBytes)
      ).get

    val compressedBlockReader =
      compressed(
        SegmentBlock(SegmentBlock.Offset(0, compressedBytes.size), headerSize, header.compressionInfo),
        compressedBytes
      )

    val decompressedBlock = Block.decompress(compressedBlockReader, randomBoolean()).get
    decompressedBlock.readAll().get shouldBe bytes.drop(headerSize)
  }

  "decompressing nested compressed block" in {
    /**
      * This test compressed 2 blocks into a root block and then decompresses.
      * Format: { rootBlock: {childBlock1: header+child1Bytes} {childBlock2: header+child2Bytes} }
      */

    val child1Bytes = Slice.fill[Byte](20)(1.toByte)
    val child2Bytes = Slice.fill[Byte](20)(2.toByte)

    val headerSize = 10

    def compression = randomCompression()

    //compress both child blocks
    val compressedChildBytes1 = Block.compress(headerSize, child1Bytes, Seq(compression), "testBlock1").get
    val compressedChildBytes2 = Block.compress(headerSize, child2Bytes, Seq(compression), "testBlock2").get

    //merge compressed child blocks and write them to a root block
    //fill with header bytes.
    val rootUncompressedBytes = Slice.fill[Byte](headerSize)(3.toByte) ++ compressedChildBytes1 ++ compressedChildBytes2
    val compressedRootBytes = Block.compress(headerSize, rootUncompressedBytes, Seq(compression), "compressedRootBlocks").get

    //read root block's header
    val rootHeader =
      Block.readHeader(
        offset = ValuesBlock.Offset(0, compressedRootBytes.size),
        reader = Reader(compressedRootBytes)
      ).get

    //decompress the root block
    val decompressedRootBlock: DecompressedBlockReader[SegmentBlock] =
      Block.decompress(
        reader = compressed(
          SegmentBlock(SegmentBlock.Offset(0, compressedRootBytes.size), headerSize, rootHeader.compressionInfo),
          compressedRootBytes
        ),
        readAllIfUncompressed = randomBoolean()
      ).get

    //got the original rootBlock bytes without the header.
    val decompressedRootBytes = decompressedRootBlock.readAllAndGetReader().get
    decompressedRootBytes.readAll().get shouldBe rootUncompressedBytes.drop(headerSize)

    //read child block1 using the same decompressed reader from root block.
    val child1Header =
      Block.readHeader(
        offset = ValuesBlock.Offset(0, compressedChildBytes1.size),
        reader = decompressedRootBytes.copy()
      ).get

    val child1DecompressedBytes =
      Block.decompress(
        childBlock = ValuesBlock(ValuesBlock.Offset(0, compressedChildBytes1.size), child1Header.headerSize, child1Header.compressionInfo),
        parentBlock = decompressedRootBlock.copy(),
        readAllIfUncompressed = randomBoolean()
      ).get

    child1DecompressedBytes.readAll().get shouldBe child1Bytes.drop(headerSize)

    //read child block2 using the same decompressed reader from root block but set the offset start to be the end of child1's compressed bytes end.
    val child2Header =
      Block.readHeader(
        offset = ValuesBlock.Offset(compressedChildBytes1.size, compressedChildBytes2.size),
        reader = decompressedRootBytes.copy()
      ).get

    val child2DecompressedBytes =
      Block.decompress(
        childBlock = ValuesBlock(ValuesBlock.Offset(compressedChildBytes1.size, compressedChildBytes2.size), child2Header.headerSize, child2Header.compressionInfo),
        parentBlock = decompressedRootBlock.copy(),
        readAllIfUncompressed = randomBoolean()
      ).get

    child2DecompressedBytes.readAll().get shouldBe child2Bytes.drop(headerSize)
  }
}
