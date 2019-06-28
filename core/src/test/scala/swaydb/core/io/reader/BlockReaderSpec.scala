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

package swaydb.core.io.reader

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData._
import swaydb.core.segment.format.a.block.{Block, Values}
import swaydb.data.slice.Slice

class BlockReaderSpec extends WordSpec with Matchers {

  def assertReader(bodyBytes: Slice[Byte], reader: BlockReader[_]) = {
    //size
    reader.size.get shouldBe bodyBytes.size

    //moveTo and get
    (0 until bodyBytes.size) foreach {
      index =>
        reader.moveTo(index).get().get shouldBe bodyBytes(index)
    }

    //hasMore
    (0 to bodyBytes.size) foreach {
      index =>
        reader.moveTo(index)

        if (index < bodyBytes.size)
          reader.hasMore.get shouldBe true
        else
          reader.hasMore.get shouldBe false
    }

    //hasAtLeast
    (0 to bodyBytes.size) foreach {
      index =>
        reader.moveTo(index)

        if (index < bodyBytes.size)
          reader.hasAtLeast(1).get shouldBe true
        else
          reader.hasAtLeast(1).get shouldBe false
    }

    //remaining
    (0 until bodyBytes.size) foreach {
      index =>
        reader.moveTo(index)
        reader.remaining.get shouldBe bodyBytes.size - index
        reader.get().get
        reader.remaining.get shouldBe bodyBytes.size - index - 1
        reader.readRemaining().get
        reader.remaining.get shouldBe 0
    }

    //readRemaining
    (0 until bodyBytes.size) foreach {
      index =>
        reader.moveTo(index).get().get shouldBe bodyBytes(index)
        reader.readRemaining().get shouldBe bodyBytes.drop(index + 1)
    }

    //readFullBlockAndGetReader
    reader.readFullBlockAndGetReader().get.readRemaining().get shouldBe bodyBytes
  }

  "read bytes" when {

    "there is no header and no compression" in {
      val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
      val block = Values(Values.Offset(0, bodyBytes.size), 0, None)
      val reader = BlockReader(Reader(bodyBytes), block)
      assertReader(bodyBytes, reader)
    }

    "there is header and no compression" in {
      val header = Slice.fill(10)(6.toByte)
      val bodyBytes = (1 to 10).flatMap(Slice.writeLong(_)).toSlice
      val segmentBytes = header ++ bodyBytes
      val block = Values(Values.Offset(0, segmentBytes.size), header.size, None)
      val reader = new BlockReader(Reader(segmentBytes), block)

      assertReader(bodyBytes, reader)
    }

    "there is compression" in {
      val headerBytes = Slice.fill(10)(0.toByte)
      val bodyBytes = (1 to 10).flatMap(Slice.writeLong(_)).toSlice
      val segmentBytes = headerBytes ++ bodyBytes
      val compression = Seq(randomCompression())
      val compressedSegmentBytes = Block.compress(headerBytes.size, segmentBytes, compression).get
      val blockOffset = Values.Offset(0, compressedSegmentBytes.size)
      val blockHeader = Block.readHeader(blockOffset, Reader(compressedSegmentBytes)).get
      blockHeader.compressionInfo shouldBe defined
      blockHeader.headerSize shouldBe headerBytes.size

      val reader = new BlockReader(Reader(compressedSegmentBytes), Values(blockOffset, blockHeader.headerSize, blockHeader.compressionInfo))

      assertReader(bodyBytes, reader)
    }
  }

  "reading bytes outside the block" should {
    "not be allowed" when {
      "there is no header and no compression" in {
        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
        val block = Values(Values.Offset(0, bodyBytes.size - 5), 0, None)
        val reader = BlockReader(Reader(bodyBytes), block)

        reader.size.get shouldBe 5
        reader.read(100).get should have size 5
        reader.get().failed.get.exception.getMessage.contains("Has no more bytes") shouldBe true
      }
    }
  }
}
