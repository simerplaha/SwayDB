///*
// * Copyright (c) 2019 Simer Plaha (@simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.io.reader
//
//import org.scalatest.{Matchers, WordSpec}
//import swaydb.core.TestData._
//import swaydb.core.segment.format.a.block.reader.{BlockedReader, UnblockedReader}
//import swaydb.core.segment.format.a.block._
//import swaydb.data.config.BlockStatus.CompressedBlock
//import swaydb.data.slice.Slice
//
//class BlockReaderSpec extends WordSpec with Matchers {
//
//  def assertDecompressed[O <: BlockOffset, B <: Block[O]](reader: UnblockedReader[O, B],
//                                                          expectedBlockBytes: Slice[Byte])(implicit blockOps: BlockOps[O, B]) = {
//    //size
//    reader.size.get shouldBe reader.block.offset.size
//
//    //moveTo and value
//    (0 until expectedBlockBytes.size) foreach {
//      index =>
//        reader.moveTo(index).get().get shouldBe expectedBlockBytes(index)
//    }
//
//    //hasMore
//    (0 to expectedBlockBytes.size) foreach {
//      index =>
//        reader.moveTo(index)
//
//        if (index < expectedBlockBytes.size)
//          reader.hasMore.get shouldBe true
//        else
//          reader.hasMore.get shouldBe false
//    }
//
//    //hasAtLeast
//    (0 to expectedBlockBytes.size) foreach {
//      index =>
//        reader.moveTo(index)
//
//        if (index < expectedBlockBytes.size)
//          reader.hasAtLeast(1).get shouldBe true
//        else
//          reader.hasAtLeast(1).get shouldBe false
//    }
//
//    //remaining
//    (0 until expectedBlockBytes.size) foreach {
//      index =>
//        reader.moveTo(index)
//        reader.remaining.get shouldBe expectedBlockBytes.size - index
//        reader.get().get
//        reader.remaining.get shouldBe expectedBlockBytes.size - index - 1
//        reader.readRemaining().get
//        reader.remaining.get shouldBe 0
//    }
//
//    //readRemaining
//    (0 until expectedBlockBytes.size) foreach {
//      index =>
//        reader.moveTo(index).get().get shouldBe expectedBlockBytes(index)
//        reader.readRemaining().get shouldBe expectedBlockBytes.drop(index + 1)
//    }
//
//    //readFullBlockAndGetReader
//    reader.readAll().get shouldBe expectedBlockBytes
//    reader.readAllAndGetReader().get.readRemaining().get shouldBe expectedBlockBytes
//  }
//
//  "reading" when {
//    "decompressed" should {
//      "not allow reading outside the block" in {
//        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
//        val block = ValuesBlock(ValuesBlock.Offset(0, bodyBytes.size - 5), 0, None)
//        val reader = UnblockedReader(block, bodyBytes)
//
//        reader.size.get shouldBe 5
//        reader.read(100).get should have size 5
//        reader.get().failed.get.exception.getMessage.contains("Has no more bytes") shouldBe true
//      }
//
//      "not allow reading outside the nested block" in {
//        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
//        val decompressedParentBlock = SegmentBlock.unblocked(bodyBytes)
//        val decompressedChildBlock =
//          UnblockedReader(
//            block = ValuesBlock(ValuesBlock.Offset(4, 2), 0, None),
//            reader = decompressedParentBlock
//          )
//
//        decompressedChildBlock.size.get shouldBe 2
//        decompressedChildBlock.read(100).get should have size 2
//        decompressedChildBlock.copy().read(100).get shouldBe bodyBytes.drop(4).take(2)
//        decompressedChildBlock.moveTo(0).read(100).get shouldBe bodyBytes.drop(4).take(2)
//        decompressedChildBlock.moveTo(-100).read(100).get shouldBe bodyBytes.drop(4).take(2)
//      }
//
//      "read bytes" in {
//        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
//        assertDecompressed(
//          reader = SegmentBlock.unblocked(bodyBytes),
//          expectedBlockBytes = bodyBytes
//        )
//      }
//
//      "read bytes in nested blocks" in {
//        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
//        //create a segment block
//        val segmentReader = SegmentBlock.unblocked(bodyBytes)
//
//        assertDecompressed(
//          reader = segmentReader,
//          expectedBlockBytes = bodyBytes
//        )
//
//        //first 3 bytes are values block
//        val valuesReader =
//          UnblockedReader(
//            block = ValuesBlock(ValuesBlock.Offset(0, 3), 0, None),
//            reader = segmentReader
//          )
//        assertDecompressed(
//          reader = valuesReader,
//          expectedBlockBytes = bodyBytes.take(3).unslice()
//        )
//
//        //within the values there is a segment block
//        //        val valuesReaderNestedSegment = DecompressedBlockReader.decompressed(SegmentBlock(SegmentBlock.Offset(0, 2), 0, None), valuesReader)
//        //        assertDecompressed(
//        //          reader = valuesReaderNestedSegment,
//        //          expectedBlockBytes = bodyBytes.take(2).unslice()
//        //        )
//        //
//        //        //3,4 are index bytes are values block
//        //        assertDecompressed(
//        //          reader = DecompressedBlockReader.decompressed(SortedIndexBlock(SortedIndexBlock.Offset(3, 2), randomBoolean(), randomBoolean(), 0, None), segmentReader),
//        //          expectedBlockBytes = bodyBytes.drop(3).take(2).unslice()
//        //        )
//        //
//        //        assertDecompressed(
//        //          reader = DecompressedBlockReader.decompressed(segmentReader, HashIndexBlock(HashIndexBlock.Offset(5, 2), None, 0, 0, 0, 0, 0, 0)),
//        //          expectedBlockBytes = bodyBytes.drop(5).take(2).unslice()
//        //        )
//        //
//        //        assertDecompressed(
//        //          reader = DecompressedBlockReader.decompressed(segmentReader, BinarySearchIndexBlock(BinarySearchIndexBlock.Offset(7, 2), 0, 0, 0, randomBoolean(), None)),
//        //          expectedBlockBytes = bodyBytes.drop(7).take(2).unslice()
//        //        )
//        //
//        //        assertDecompressed(
//        //          reader = DecompressedBlockReader.decompressed(segmentReader, BloomFilterBlock(BloomFilterBlock.Offset(8, 2), 0, 0, 0, None)),
//        //          expectedBlockBytes = bodyBytes.drop(8).take(2).unslice()
//        //        )
//        //
//        //        assertDecompressed(
//        //          reader =
//        //            DecompressedBlockReader.decompressed(
//        //              reader = segmentReader,
//        //              block = SegmentFooterBlock(
//        //                offset = SegmentFooterBlock.Offset(9, 1),
//        //                headerSize = 0,
//        //                compressionInfo = None,
//        //                valuesOffset = None,
//        //                sortedIndexOffset = SortedIndexBlock.Offset(0, 0),
//        //                hashIndexOffset = None,
//        //                binarySearchIndexOffset = None,
//        //                bloomFilterOffset = None,
//        //                keyValueCount = 0,
//        //                createdInLevel = 0,
//        //                bloomFilterItemsCount = 0,
//        //                hasRange = randomBoolean(),
//        //                hasGroup = randomBoolean(),
//        //                hasPut = randomBoolean()
//        //              )
//        //            ),
//        //          expectedBlockBytes = bodyBytes.drop(9).unslice()
//        //        )
//      }
//    }
//
//    "compressed" should {
//      "not allow reading outside the block" in {
//        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
//        val block = ValuesBlock(ValuesBlock.Offset(0, bodyBytes.size - 5), 0, None)
//        val reader = UnblockedReader(block, bodyBytes)
//
//        reader.size.get shouldBe 5
//        reader.read(100).get should have size 5
//        reader.get().failed.get.exception.getMessage.contains("Has no more bytes") shouldBe true
//      }
//
//      "not allow reading outside the nested block" in {
//        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
//        val decompressedParentBlock = SegmentBlock.unblocked(bodyBytes)
//        val decompressedChildBlock = UnblockedReader(ValuesBlock(ValuesBlock.Offset(4, 2), 0, None), decompressedParentBlock)
//
//        decompressedChildBlock.size.get shouldBe 2
//        decompressedChildBlock.read(100).get should have size 2
//        decompressedChildBlock.copy().read(100).get shouldBe bodyBytes.drop(4).take(2)
//        decompressedChildBlock.moveTo(0).read(100).get shouldBe bodyBytes.drop(4).take(2)
//        decompressedChildBlock.moveTo(-100).read(100).get shouldBe bodyBytes.drop(4).take(2)
//      }
//
//      "read bytes" in {
//        val headerBytes = Slice.fill[Byte](10)(0)
//        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
//        val bytes = headerBytes ++ bodyBytes
//        val compression = randomCompressions().head
//        val compressedBlockBytes =
//          Block.block(
//            headerSize = headerBytes.size,
//            bytes = bytes,
//            compressions = Seq(compression),
//            blockName = "test-block"
//          ).get
//
//        val compressedReader =
//          SegmentBlock.blocked(
//            headerSize = headerBytes.size,
//            bytes = compressedBlockBytes,
//            compressionInfo = Block.CompressionInfo(
//              decompressor = compression.decompressor,
//              decompressedLength = bodyBytes.size
//            )
//          )
//
//
//
//        //        Block.decompress()
//
//        //        assertDecompressed(
//        //          reader = Block.decompress(blockBytes, )),
//        //          expectedBlockBytes = bodyBytes
//        //        )
//      }
//
//      //      "read bytes in nested blocks" in {
//      //        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
//      //        //create a segment block
//      //        val segmentBlock = SegmentBlock.decompressed(bodyBytes)
//      //
//      //        assertDecompressed(
//      //          reader = segmentBlock,
//      //          expectedBlockBytes = bodyBytes
//      //        )
//      //
//      //        //first 3 bytes are values block
//      //        val valuesReader = DecompressedBlockReader.decompressed(segmentBlock, ValuesBlock(ValuesBlock.Offset(0, 3), 0, None))
//      //        assertDecompressed(
//      //          reader = valuesReader,
//      //          expectedBlockBytes = bodyBytes.take(3).unslice()
//      //        )
//      //
//      //        //within the values there is a segment block
//      //        val valuesReaderNestedSegment = DecompressedBlockReader.decompressed(valuesReader, SegmentBlock(SegmentBlock.Offset(0, 2), 0, None))
//      //        assertDecompressed(
//      //          reader = valuesReaderNestedSegment,
//      //          expectedBlockBytes = bodyBytes.take(2).unslice()
//      //        )
//      //
//      //        //3,4 are index bytes are values block
//      //        assertDecompressed(
//      //          reader = DecompressedBlockReader.decompressed(segmentBlock, SortedIndexBlock(SortedIndexBlock.Offset(3, 2), randomBoolean(), randomBoolean(), 0, None)),
//      //          expectedBlockBytes = bodyBytes.drop(3).take(2).unslice()
//      //        )
//      //
//      //        assertDecompressed(
//      //          reader = DecompressedBlockReader.decompressed(segmentBlock, HashIndexBlock(HashIndexBlock.Offset(5, 2), None, 0, 0, 0, 0, 0, 0)),
//      //          expectedBlockBytes = bodyBytes.drop(5).take(2).unslice()
//      //        )
//      //
//      //        assertDecompressed(
//      //          reader = DecompressedBlockReader.decompressed(segmentBlock, BinarySearchIndexBlock(BinarySearchIndexBlock.Offset(7, 2), 0, 0, 0, randomBoolean(), None)),
//      //          expectedBlockBytes = bodyBytes.drop(7).take(2).unslice()
//      //        )
//      //
//      //        assertDecompressed(
//      //          reader = DecompressedBlockReader.decompressed(segmentBlock, BloomFilterBlock(BloomFilterBlock.Offset(8, 2), 0, 0, 0, None)),
//      //          expectedBlockBytes = bodyBytes.drop(8).take(2).unslice()
//      //        )
//      //
//      //        assertDecompressed(
//      //          reader =
//      //            DecompressedBlockReader.decompressed(
//      //              reader = segmentBlock,
//      //              block = SegmentFooterBlock(
//      //                offset = SegmentFooterBlock.Offset(9, 1),
//      //                headerSize = 0,
//      //                compressionInfo = None,
//      //                valuesOffset = None,
//      //                sortedIndexOffset = SortedIndexBlock.Offset(0, 0),
//      //                hashIndexOffset = None,
//      //                binarySearchIndexOffset = None,
//      //                bloomFilterOffset = None,
//      //                keyValueCount = 0,
//      //                createdInLevel = 0,
//      //                bloomFilterItemsCount = 0,
//      //                hasRange = randomBoolean(),
//      //                hasGroup = randomBoolean(),
//      //                hasPut = randomBoolean()
//      //              )
//      //            ),
//      //          expectedBlockBytes = bodyBytes.drop(9).unslice()
//      //        )
//      //      }
//    }
//  }
//}
