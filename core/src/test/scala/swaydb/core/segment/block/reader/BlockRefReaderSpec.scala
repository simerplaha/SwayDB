/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.block.reader

import org.scalamock.scalatest.MockFactory
import swaydb.compression.CompressionInternal
import swaydb.core.CommonAssertions.orNone
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.block.values.ValuesBlock.ValuesBlockOps
import swaydb.core.segment.block.{Block, BlockCache}
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.slice.{Reader, Slice}

class BlockRefReaderSpec extends TestBase with MockFactory {

  "apply" when {
    "File, bytes & reader" in {
      TestCaseSweeper {
        implicit sweeper =>
          val bytes = randomBytesSlice(100)
          val fileReader = createRandomFileReader(bytes)
          val file = fileReader.file

          val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))

          //DBFile
          BlockRefReader(file = file, blockCache = blockCache).readRemaining() shouldBe bytes
          //Slice[Byte]
          BlockRefReader[SegmentBlock.Offset](bytes).readRemaining() shouldBe bytes

          //Reader: FileReader
          BlockRefReader[SegmentBlock.Offset](fileReader: Reader[Byte], blockCache = blockCache).readRemaining() shouldBe bytes
          //Reader: SliceReader
          BlockRefReader[SegmentBlock.Offset](Reader(bytes): Reader[Byte], blockCache = blockCache).readRemaining() shouldBe bytes
      }
    }
  }

  "moveTo & moveWithin" when {
    "random bytes with header" in {
      TestCaseSweeper {
        implicit sweeper =>
          val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))

          val header = Slice(1.toByte, 0.toByte)
          val bodyBytes = randomBytesSlice(20)
          val bytes = header ++ bodyBytes

          val ref = BlockRefReader[ValuesBlock.Offset](bytes)
          ref.copy().readRemaining() shouldBe bytes
          ref.copy().moveTo(10).readRemaining() shouldBe bytes.drop(10)

          val blocked = BlockedReader(ref)
          blocked.copy().readRemaining() shouldBe bodyBytes

          val unblocked = UnblockedReader(blocked, randomBoolean())
          unblocked.copy().readRemaining() shouldBe bodyBytes

          val moveTo = BlockRefReader.moveTo(5, 5, unblocked, blockCache)(ValuesBlockOps)
          moveTo.copy().readRemaining() shouldBe bodyBytes.drop(5).take(5)

          val moveWithin = BlockRefReader.moveTo(ValuesBlock.Offset(5, 5), unblocked, blockCache)(ValuesBlockOps)
          moveWithin.copy().readRemaining() shouldBe bodyBytes.drop(5).take(5)
      }
    }

    "compressed & uncompressed blocks" in {
      TestCaseSweeper {
        implicit sweeper =>
          val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))

          def runTest(compressions: Iterable[CompressionInternal]) = {
            val body = randomBytesSlice(1000)
            val compressed = Block.compress(body, compressions, "test")
            compressed.fixHeaderSize()

            val compressedBytes = compressed.headerBytes ++ compressed.compressedBytes.getOrElse(body)

            val ref = BlockRefReader[ValuesBlock.Offset](compressedBytes)
            ref.copy().readRemaining() shouldBe compressedBytes
            ref.copy().moveTo(10).readRemaining() shouldBe compressedBytes.drop(10)

            val blocked = BlockedReader(ref)
            blocked.copy().readRemaining() shouldBe compressedBytes.drop(compressed.headerBytes.size)

            val unblocked = UnblockedReader(blocked, randomBoolean())
            unblocked.copy().readRemaining() shouldBe body

            val moveTo = BlockRefReader.moveTo(5, 5, unblocked, blockCache)(ValuesBlockOps)
            moveTo.copy().readRemaining() shouldBe body.drop(5).take(5)

            val moveWithin = BlockRefReader.moveTo(ValuesBlock.Offset(5, 5), unblocked, blockCache)(ValuesBlockOps)
            moveWithin.copy().readRemaining() shouldBe body.drop(5).take(5)
          }

          runTest(randomCompressionsLZ4OrSnappyOrEmpty())
          runTest(Seq(randomCompressionLZ4()))
          runTest(Seq(randomCompressionSnappy()))
          runTest(Seq.empty)
      }
    }
  }
}
