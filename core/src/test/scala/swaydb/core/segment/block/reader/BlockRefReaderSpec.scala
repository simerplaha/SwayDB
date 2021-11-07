///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core.segment.block.reader
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.compression.CompressionInternal
//import swaydb.core.CommonAssertions.orNone
//import swaydb.core.TestData._
//import swaydb.core.io.reader.Reader
//import swaydb.core.segment.block.segment.SegmentBlock
//import swaydb.core.segment.block.values.ValuesBlock
//import swaydb.core.segment.block.values.ValuesBlock.ValuesBlockOps
//import swaydb.core.segment.block.{Block, BlockCache}
//import swaydb.core.{TestBase, TestCaseSweeper}
//import swaydb.data.slice.{Reader, Slice}
//
//class BlockRefReaderSpec extends TestBase with MockFactory {
//
//  "apply" when {
//    "File, bytes & reader" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          val bytes = randomBytesSlice(100)
//          val fileReader = createRandomFileReader(bytes)
//          val file = fileReader.file
//
//          val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))
//
//          //DBFile
//          BlockRefReader(file = file, blockCache = blockCache).readRemaining() shouldBe bytes
//          //Slice[Byte]
//          BlockRefReader[SegmentBlock.Offset](bytes).readRemaining() shouldBe bytes
//
//          //Reader: FileReader
//          BlockRefReader[SegmentBlock.Offset](fileReader: Reader[Byte], blockCache = blockCache).readRemaining() shouldBe bytes
//          //Reader: SliceReader
//          BlockRefReader[SegmentBlock.Offset](Reader(bytes): Reader[Byte], blockCache = blockCache).readRemaining() shouldBe bytes
//      }
//    }
//  }
//
//  "moveTo & moveWithin" when {
//    "random bytes with header" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))
//
//          val header = Slice(1.toByte, 0.toByte)
//          val bodyBytes = randomBytesSlice(20)
//          val bytes = header ++ bodyBytes
//
//          val ref = BlockRefReader[ValuesBlock.Offset](bytes)
//          ref.copy().readRemaining() shouldBe bytes
//          ref.copy().moveTo(10).readRemaining() shouldBe bytes.drop(10)
//
//          val blocked = BlockedReader(ref)
//          blocked.copy().readRemaining() shouldBe bodyBytes
//
//          val unblocked = UnblockedReader(blocked, randomBoolean())
//          unblocked.copy().readRemaining() shouldBe bodyBytes
//
//          val moveTo = BlockRefReader.moveTo(5, 5, unblocked, blockCache)(ValuesBlockOps)
//          moveTo.copy().readRemaining() shouldBe bodyBytes.drop(5).take(5)
//
//          val moveWithin = BlockRefReader.moveTo(ValuesBlock.Offset(5, 5), unblocked, blockCache)(ValuesBlockOps)
//          moveWithin.copy().readRemaining() shouldBe bodyBytes.drop(5).take(5)
//      }
//    }
//
//    "compressed & uncompressed blocks" in {
//      TestCaseSweeper {
//        implicit sweeper =>
//          val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))
//
//          def runTest(compressions: Iterable[CompressionInternal]) = {
//            val body = randomBytesSlice(1000)
//            val compressed = Block.compress(body, compressions, "test")
//            compressed.fixHeaderSize()
//
//            val compressedBytes = compressed.headerBytes ++ compressed.compressedBytes.getOrElse(body)
//
//            val ref = BlockRefReader[ValuesBlock.Offset](compressedBytes)
//            ref.copy().readRemaining() shouldBe compressedBytes
//            ref.copy().moveTo(10).readRemaining() shouldBe compressedBytes.drop(10)
//
//            val blocked = BlockedReader(ref)
//            blocked.copy().readRemaining() shouldBe compressedBytes.drop(compressed.headerBytes.size)
//
//            val unblocked = UnblockedReader(blocked, randomBoolean())
//            unblocked.copy().readRemaining() shouldBe body
//
//            val moveTo = BlockRefReader.moveTo(5, 5, unblocked, blockCache)(ValuesBlockOps)
//            moveTo.copy().readRemaining() shouldBe body.drop(5).take(5)
//
//            val moveWithin = BlockRefReader.moveTo(ValuesBlock.Offset(5, 5), unblocked, blockCache)(ValuesBlockOps)
//            moveWithin.copy().readRemaining() shouldBe body.drop(5).take(5)
//          }
//
//          runTest(randomCompressionsLZ4OrSnappyOrEmpty())
//          runTest(Seq(randomCompressionLZ4()))
//          runTest(Seq(randomCompressionSnappy()))
//          runTest(Seq.empty)
//      }
//    }
//  }
//}
