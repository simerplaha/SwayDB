package swaydb.core.segment.format.a.block

import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.data.slice.Slice

class BlockSpec extends TestBase {

  "create" when {
    "no compression for some random bytes" in {
      runThis(100.times) {
        val headerSize = Block.headerSize(false) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
        val dataBytes = randomBytesSlice(randomIntMax(100) + 1)
        val uncompressedBytes = Slice.fill(headerSize)(0.toByte) ++ dataBytes

        val compressedBytes = Block.create(headerSize, uncompressedBytes, Seq.empty, "test-block").get

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
          Block.createBlockDataReader(
            block = ValuesBlock(ValuesBlock.Offset(0, uncompressedBytes.size), headerSize, None),
            readFullBlockIfUncompressed = randomBoolean(),
            segmentReader = SegmentBlock.createUnblockedReader(uncompressedBytes).get
          ).get

        blockReader.readRemaining().get shouldBe dataBytes
        blockReader.read(Int.MaxValue).get shouldBe dataBytes
        blockReader.readFullBlock().get shouldBe dataBytes
        blockReader.readFullBlockAndGetBlockReader().get.readRemaining().get shouldBe dataBytes
      }
    }

    "no compression for some segment bytes bytes" in {
      runThis(100.times) {
        val headerSize = Block.headerSize(false) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
        val segment =
          SegmentBlock.Open(
            headerBytes = Slice.fill(headerSize)(0.toByte),
            values = randomBytesSliceOption(2),
            sortedIndex = randomBytesSlice(2),
            hashIndex = randomBytesSliceOption(2),
            binarySearchIndex = randomBytesSliceOption(2),
            bloomFilter = randomBytesSliceOption(2),
            footer = randomBytesSlice(),
            functionMinMax = None,
            nearestDeadline = randomDeadlineOption()
          )

        val uncompressedBytes = segment.flattenSegmentBytes

        val compressedSegment = Block.create(segment, Seq.empty, "test-segment-block").get

        compressedSegment.hashCode() shouldBe segment.hashCode() //same object - mutated!

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
          Block.createBlockDataReader(
            block = ValuesBlock(ValuesBlock.Offset(0, uncompressedBytes.size), headerSize, None),
            readFullBlockIfUncompressed = randomBoolean(),
            segmentReader = SegmentBlock.createUnblockedReader(uncompressedBytes).get
          ).get

        val dataBytes = segment.segmentBytes.dropHead().flatten.toSlice

        decompressedBlockReader.readRemaining().get shouldBe dataBytes
        decompressedBlockReader.read(Int.MaxValue).get shouldBe dataBytes
        decompressedBlockReader.readFullBlock().get shouldBe dataBytes
        decompressedBlockReader.readFullBlockAndGetBlockReader().get.readRemaining().get shouldBe dataBytes
      }
    }

    "has compression" when {
      "some random bytes" in {
        runThis(100.times) {
          val headerSize = Block.headerSize(true) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val dataBytes = randomBytesSlice(randomIntMax(100) + 1)
          val uncompressedBytes = Slice.fill(headerSize)(0.toByte) ++ dataBytes

          val compression = randomCompressions().head
          val compressedBytes = Block.create(headerSize, uncompressedBytes, Seq(compression), "test-block").get
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
            Block.createBlockDataReader(
              block = ValuesBlock(ValuesBlock.Offset(0, compressedBytes.size), headerSize, header.compressionInfo),
              readFullBlockIfUncompressed = randomBoolean(),
              segmentReader = SegmentBlock.createUnblockedReader(compressedBytes).get
            ).get

          blockReader.readRemaining().get shouldBe dataBytes
          blockReader.read(Int.MaxValue).get shouldBe dataBytes
          blockReader.readFullBlock().get shouldBe dataBytes
          blockReader.readFullBlockAndGetBlockReader().get.readRemaining().get shouldBe dataBytes
        }
      }

      "segment bytes" in {
        runThis(100.times) {
          val headerSize = Block.headerSize(true) + 1 //+1 for Bytes.sizeOf(headerSize) that is calculated by the block itself.
          val uncompressedSegment =
            SegmentBlock.Open(
              headerBytes = Slice.fill(headerSize)(0.toByte),
              values = randomBytesSliceOption(randomIntMax(100) + 1),
              sortedIndex = randomBytesSlice(randomIntMax(100) + 1),
              hashIndex = randomBytesSliceOption(randomIntMax(100) + 1),
              binarySearchIndex = randomBytesSliceOption(randomIntMax(100) + 1),
              bloomFilter = randomBytesSliceOption(randomIntMax(100) + 1),
              footer = randomBytesSlice(randomIntMax(100) + 1),
              functionMinMax = None,
              nearestDeadline = randomDeadlineOption()
            )

          val compression = randomCompressions().head

          val compressedSegment = Block.create(uncompressedSegment, Seq(compression), "test-segment-block").get

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
            Block.createBlockDataReader(
              block = ValuesBlock(ValuesBlock.Offset(0, compressedSegment.segmentSize), headerSize, header.compressionInfo),
              readFullBlockIfUncompressed = randomBoolean(),
              segmentReader = SegmentBlock.createUnblockedReader(compressedSegment.flattenSegmentBytes).get
            ).get

          val uncompressedSegmentBytesWithoutHeader = uncompressedSegment.segmentBytes.dropHead().flatten.toSlice

          decompressedBlockReader.readRemaining().get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.read(Int.MaxValue).get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.readFullBlock().get shouldBe uncompressedSegmentBytesWithoutHeader
          decompressedBlockReader.readFullBlockAndGetBlockReader().get.readRemaining().get shouldBe uncompressedSegmentBytesWithoutHeader
        }
      }
    }
  }
}
