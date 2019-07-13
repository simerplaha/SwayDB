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
import swaydb.core.segment.format.a.block.{Block, ValuesBlock}
import swaydb.data.slice.Slice

class BlockReaderSpec extends WordSpec with Matchers {

  def assertReader(expectedBlockBytes: Slice[Byte],
                   reader: BlockReader[Block]) = {
    //size
    reader.size.get shouldBe reader.block.offset.size

    //moveTo and value
    (0 until expectedBlockBytes.size) foreach {
      index =>
        reader.moveTo(index).get().get shouldBe expectedBlockBytes(index)
    }

    //hasMore
    (0 to expectedBlockBytes.size) foreach {
      index =>
        reader.moveTo(index)

        if (index < expectedBlockBytes.size)
          reader.hasMore.get shouldBe true
        else
          reader.hasMore.get shouldBe false
    }

    //hasAtLeast
    (0 to expectedBlockBytes.size) foreach {
      index =>
        reader.moveTo(index)

        if (index < expectedBlockBytes.size)
          reader.hasAtLeast(1).get shouldBe true
        else
          reader.hasAtLeast(1).get shouldBe false
    }

    //remaining
    (0 until expectedBlockBytes.size) foreach {
      index =>
        reader.moveTo(index)
        reader.remaining.get shouldBe expectedBlockBytes.size - index
        reader.get().get
        reader.remaining.get shouldBe expectedBlockBytes.size - index - 1
        reader.readRemaining().get
        reader.remaining.get shouldBe 0
    }

    //readRemaining
    (0 until expectedBlockBytes.size) foreach {
      index =>
        reader.moveTo(index).get().get shouldBe expectedBlockBytes(index)
        reader.readRemaining().get shouldBe expectedBlockBytes.drop(index + 1)
    }

    //readFullBlockAndGetReader
    reader.readFullBlockAndGetBlockReader().get.readRemaining().get shouldBe expectedBlockBytes
  }

  "read bytes" when {

    "there is no header and no compression" in {
      val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
      val block = ValuesBlock(ValuesBlock.Offset(0, bodyBytes.size), 0, None)
      val reader = BlockReader(Reader(bodyBytes), block)
      assertReader(bodyBytes, reader)
    }

    "nested blocks" in {
      val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
      val block = ValuesBlock(ValuesBlock.Offset(0, bodyBytes.size), 0, None)
      val reader = BlockReader(Reader(bodyBytes), block)
      assertReader(bodyBytes, reader)

      val innerBlock = ValuesBlock(ValuesBlock.Offset(5, 5), 0, None)
      val innerBlockReader = BlockReader(reader, innerBlock)
      assertReader(bodyBytes.drop(5).unslice(), innerBlockReader)

      val innerBlock2 = ValuesBlock(ValuesBlock.Offset(3, 2), 0, None)
      val innerBlockReader2 = BlockReader(innerBlockReader, innerBlock2)
      assertReader(bodyBytes.drop(8).unslice(), innerBlockReader2)

      val innerBlock3 = ValuesBlock(ValuesBlock.Offset(2, 0), 0, None)
      val innerBlockReader3 = BlockReader(innerBlockReader, innerBlock3)
      assertReader(bodyBytes.drop(10).unslice(), innerBlockReader3)
    }
  }

  "reading bytes outside the block" should {
    "not be allowed" when {
      "there is no header" in {
        val bodyBytes = Slice((1 to 10).map(_.toByte).toArray)
        val block = ValuesBlock(ValuesBlock.Offset(0, bodyBytes.size - 5), 0, None)
        val reader = BlockReader(Reader(bodyBytes), block)

        reader.size.get shouldBe 5
        reader.read(100).get should have size 5
        reader.get().failed.get.exception.getMessage.contains("Has no more bytes") shouldBe true
      }
    }
  }
}
