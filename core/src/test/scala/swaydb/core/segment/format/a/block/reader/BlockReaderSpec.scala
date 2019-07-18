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
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.io.reader.FileReader
import swaydb.core.segment.format.a.block.{BlockOffset, ValuesBlock}
import swaydb.data.IO
import swaydb.data.slice.Reader

class BlockReaderSpec extends TestBase with MockFactory {

  object BlockReader {
    def apply(blockReader: Reader, blockOffset: BlockOffset, _blockSize: Int): BlockReader =
      new BlockReader {
        override private[reader] def reader = blockReader
        override def offset: BlockOffset = blockOffset
        override def copy(): Reader = apply(blockReader.copy(), blockOffset, _blockSize)
        def blockSize = _blockSize
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
        reader.readFromCache(i, 100).size should be <= 2
        reader.readFromCache(i, 100).size should be >= 1
    }
  }
}
