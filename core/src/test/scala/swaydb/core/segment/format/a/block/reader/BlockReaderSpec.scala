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
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.io.reader.FileReader
import swaydb.core.segment.format.a.block.{BlockOffset, ValuesBlock}
import swaydb.data.slice.{Reader, ReaderBase, Slice}

class BlockReaderSpec extends TestBase with MockFactory {

  object BlockReader {
    def apply(blockReader: Reader[swaydb.Error.Segment], blockOffset: BlockOffset, _blockSize: Int): BlockReader =
      new BlockReader {
        def path = reader.path
        override private[reader] def reader = blockReader
        override def offset: BlockOffset = blockOffset
        override def copy(): ReaderBase[swaydb.Error.Segment] = apply(blockReader.copy(), blockOffset, _blockSize)
        def blockSize = _blockSize
      }
  }

  "read when block size is 0" in {
    val slice = (1 to 10).map(_.toByte).toSlice
    val fileReader = mock[FileReader]("fileReader")

    val reader = BlockReader(blockReader = fileReader, blockOffset = ValuesBlock.Offset(0, slice.size), _blockSize = 0)
    (0 to 9) foreach {
      i =>
        fileReader.moveTo _ expects i returning fileReader
        fileReader.read _ expects 1 returning IO(Slice(slice(i)))

        val byte = reader.read(1).get
        byte should have size 1
        byte.head shouldBe slice(i)
        reader.readFromCache(0, 100) shouldBe empty
    }
  }

  "read when block size is larger than data size" in {
    val slice = (1 to 10).map(_.toByte).toSlice
    val fileReader = mock[FileReader]("fileReader")

    val reader = BlockReader(blockReader = fileReader, blockOffset = ValuesBlock.Offset(0, slice.size), _blockSize = 100)

    fileReader.moveTo _ expects 0 returning fileReader
    //even though the blockSize is 100, reader only reads 10 which is the total size.
    fileReader.read _ expects 10 returning IO(slice)

    //expect only one call to fetch the bytes, the remaining will be read from the blockCache.
    (0 to 9) foreach {
      i =>
        val byte = reader.read(1).get
        byte should have size 1
        byte.head shouldBe slice(i)
        reader.readFromCache(0, 100) shouldBe slice
    }
  }

  "read when blockSize smaller than the data size" in {
    val slice = (1 to 10).map(_.toByte).toSlice
    val fileReader = mock[FileReader]("fileReader")

    val reader = BlockReader(blockReader = fileReader, blockOffset = ValuesBlock.Offset(0, slice.size), _blockSize = 2)

    (0 to 9) foreach {
      i =>
        //expect every second call to do a disk seek.
        if (i % 2 == 0) {
          fileReader.moveTo _ expects i returning fileReader
          //since 1 byte is being read at time and the block size is 2 only 2 bytes will requested from the file.
          fileReader.read _ expects 2 returning IO(slice.drop(i).take(2))
        }

        val byte = reader.read(1).get
        byte should have size 1
        byte.head shouldBe slice(i)
        //there should be a minimum of 2 bytes in cache as that is the block size.
        reader.cacheSize shouldBe 2
    }
  }

  "read when blockSize is odd number" in {
    val slice = (1 to 20).map(_.toByte).toSlice
    val fileReader = mock[FileReader]("fileReader")

    val reader = BlockReader(blockReader = fileReader, blockOffset = ValuesBlock.Offset(0, slice.size), _blockSize = 5)

    //data size.
    //{[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15], [16, 17, 18 , 19, 20]}

    //fist expect a disk seek - [1, 2, 3, 4, 5]
    fileReader.moveTo _ expects 0 returning fileReader
    fileReader.read _ expects 5 returning IO(slice.take(5))

    //expect above disk seek [1, 2]
    var bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(1 to 2)
    reader.cachedBytes shouldBe Slice.bytes(1 to 5)

    //read again, do not expect any disk seek. [3, 4]
    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(3 to 4)
    reader.cachedBytes shouldBe Slice.bytes(1 to 5)

    //next one will be [5, 6] but 6 is not cached should disk seek is expected for next block.
    //expect disk seek for the next blockSize [6, 7, 8, 9, 10]
    fileReader.moveTo _ expects 5 returning fileReader
    fileReader.read _ expects 5 returning IO(slice.drop(5).take(5))

    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(5 to 6)
    reader.cachedBytes shouldBe Slice.bytes(6 to 10)

    //again, no disk seek.
    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(7 to 8)
    reader.cachedBytes shouldBe Slice.bytes(6 to 10)

    //again, no disk seek.
    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(9 to 10)
    reader.cachedBytes shouldBe Slice.bytes(6 to 10)

    //next one will be [11, 12] but this block does not exists
    //expect disk seek for the next blockSize [11, 12, 13, 14, 15]
    fileReader.moveTo _ expects 10 returning fileReader
    fileReader.read _ expects 5 returning IO(slice.drop(10).take(5))

    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(11 to 12)
    reader.cachedBytes shouldBe Slice.bytes(11 to 15)

    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(13 to 14)
    reader.cachedBytes shouldBe Slice.bytes(11 to 15)

    //next one will be [15, 16] but 16 does not exist. So expect disk seek.
    //expect disk seek for the next blockSize [11, 12, 13, 14, 15]
    fileReader.moveTo _ expects 15 returning fileReader
    fileReader.read _ expects 5 returning IO(slice.drop(15).take(5))
    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(15 to 16)
    reader.cachedBytes shouldBe Slice.bytes(16 to 20)

    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(17 to 18)
    reader.cachedBytes shouldBe Slice.bytes(16 to 20)

    bytes = reader.read(2).get
    bytes should have size 2
    bytes shouldBe Slice.bytes(19 to 20)
    reader.cachedBytes shouldBe Slice.bytes(16 to 20)

    reader.hasMore.get shouldBe false
    reader.read(2).get shouldBe empty
  }
}
