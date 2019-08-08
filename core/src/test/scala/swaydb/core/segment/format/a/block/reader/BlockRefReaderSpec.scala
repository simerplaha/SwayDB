/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment.format.a.block.reader

import org.scalamock.scalatest.MockFactory
import swaydb.IOValues._
import swaydb.compression.CompressionInternal
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.ValuesBlock.ValuesBlockOps
import swaydb.core.segment.format.a.block.{Block, SegmentBlock, ValuesBlock}
import swaydb.data.slice.{Reader, Slice}

class BlockRefReaderSpec extends TestBase with MockFactory {

  "apply" when {
    "File, bytes & reader" in {
      val bytes = randomBytesSlice(100)
      val fileReader = createRandomFileReader(bytes)
      val file = fileReader.file

      //DBFile
      BlockRefReader(file).get.readRemaining().value shouldBe bytes
      //Slice[Byte]
      BlockRefReader[SegmentBlock.Offset](bytes).readRemaining().value shouldBe bytes

      //Reader: FileReader
      BlockRefReader[SegmentBlock.Offset](fileReader: Reader[swaydb.Error.Segment]).get.readRemaining().value shouldBe bytes
      //Reader: SliceReader
      BlockRefReader[SegmentBlock.Offset](Reader(bytes): Reader[swaydb.Error.Segment]).get.readRemaining().value shouldBe bytes
    }
  }

  "moveTo & moveWithin" when {
    "random bytes with header" in {
      val header = Slice(2.toByte, 0.toByte)
      val bodyBytes = randomBytesSlice(20)
      val bytes = header ++ bodyBytes

      val ref = BlockRefReader[ValuesBlock.Offset](bytes)
      ref.copy().readRemaining().get shouldBe bytes
      ref.copy().moveTo(10).readRemaining().get shouldBe bytes.drop(10)

      val blocked = BlockedReader(ref).value
      blocked.copy().readRemaining().get shouldBe bodyBytes

      val unblocked = UnblockedReader(blocked, randomBoolean()).value
      unblocked.copy().readRemaining().get shouldBe bodyBytes

      val moveTo = BlockRefReader.moveTo(5, 5, unblocked)(ValuesBlockOps)
      moveTo.copy().readRemaining().value shouldBe bodyBytes.drop(5).take(5)

      val moveWithin = BlockRefReader.moveTo(ValuesBlock.Offset(5, 5), unblocked)(ValuesBlockOps)
      moveWithin.copy().readRemaining().value shouldBe bodyBytes.drop(5).take(5)
    }

    "compressed & uncompressed blocks" in {
      def runTest(compressions: Seq[CompressionInternal]) = {
        val header = Slice.fill(5)(0.toByte)
        val body = randomBytesSlice(1000)
        val bytes = header ++ body
        val compressed = Block.block(5, bytes, compressions, "test").value

        val ref = BlockRefReader[ValuesBlock.Offset](compressed)
        ref.copy().readRemaining().get shouldBe compressed
        ref.copy().moveTo(10).readRemaining().get shouldBe compressed.drop(10)

        val blocked = BlockedReader(ref).value
        blocked.copy().readRemaining().get shouldBe compressed.drop(5)

        val unblocked = UnblockedReader(blocked, randomBoolean()).value
        unblocked.copy().readRemaining().get shouldBe body

        val moveTo = BlockRefReader.moveTo(5, 5, unblocked)(ValuesBlockOps)
        moveTo.copy().readRemaining().value shouldBe body.drop(5).take(5)

        val moveWithin = BlockRefReader.moveTo(ValuesBlock.Offset(5, 5), unblocked)(ValuesBlockOps)
        moveWithin.copy().readRemaining().value shouldBe body.drop(5).take(5)
      }

      runTest(randomCompressionsLZ4OrSnappyOrEmpty())
      runTest(Seq(randomCompressionLZ4()))
      runTest(Seq(randomCompressionSnappy()))
      runTest(Seq.empty)
    }
  }
}
