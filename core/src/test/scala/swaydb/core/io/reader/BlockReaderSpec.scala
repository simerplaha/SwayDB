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
import swaydb.data.slice.{Reader, Slice}

class BlockReaderSpec extends WordSpec with Matchers {

  def assertReader(bytes: Slice[Byte], reader: Reader) = {
    //size
    reader.size.get shouldBe bytes.size

    //moveTo and get
    (0 until bytes.size) foreach {
      index =>
        reader.moveTo(index).get().get shouldBe bytes(index)
    }

    //hasMore
    (0 until bytes.size) foreach {
      index =>
        reader.moveTo(index)

        if (index < bytes.size - 1)
          reader.hasMore.get shouldBe true
        else
          reader.hasMore.get shouldBe false
    }

    //hasAtLeast
    (0 until bytes.size) foreach {
      index =>
        reader.moveTo(index)

        if (index < bytes.size - 1)
          reader.hasAtLeast(1).get shouldBe true
        else
          reader.hasAtLeast(1).get shouldBe false
    }

    //readRemaining
    (0 until bytes.size) foreach {
      index =>
        reader.moveTo(index).get().get shouldBe bytes(index)
        reader.readRemaining().get shouldBe bytes.drop(index + 1)
    }
  }

  "read bytes" when {

    "there is no header and no compression" in {
      val bytes = Slice((1 to 10).map(_.toByte).toArray)
      val reader = new BlockReader(Reader(bytes), Values.Offset(0, bytes.size), 0, None)

      assertReader(bytes, reader)
    }

    "there is header and no compression" in {
      val header = Slice.fill(10)(0.toByte)
      val bytes = (1 to 10).flatMap(Slice.writeLong(_)).toSlice
      val segmentBytes = header ++ bytes
      val reader = new BlockReader(Reader(segmentBytes), Values.Offset(0, segmentBytes.size), header.size, None)

      assertReader(bytes, reader)
    }

    "there is compression" should {
      val header = Slice.fill(10)(0.toByte)
      val bytes = (1 to 10).flatMap(Slice.writeLong(_)).toSlice
      val segmentBytes = header ++ bytes
      val compression = Seq(randomCompression())
      val compressedBytes = Block.compress(header.size, segmentBytes, compression).get
      val offset = Values.Offset(0, compressedBytes.size)
      val compressionInfo = Block.readHeader(offset, Reader(compressedBytes)).get
      compressionInfo.compressionInfo shouldBe defined
      compressionInfo.headerSize shouldBe header.size
      val reader = new BlockReader(Reader(compressedBytes), offset, compressionInfo.headerSize, compressionInfo.compressionInfo)

      assertReader(bytes, reader)
    }
  }
}
