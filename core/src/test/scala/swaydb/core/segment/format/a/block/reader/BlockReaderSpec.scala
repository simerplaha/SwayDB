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
import swaydb.IOValues._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.io.reader.FileReader
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.data.slice.Slice

import scala.util.Random

class BlockReaderSpec extends TestBase with MockFactory {

  "state" in {
    val bytes = randomBytesSlice(100)
    val reader = createRandomFileReader(bytes)
    val state = BlockReader(ValuesBlock.Offset(0, 10), 10, reader)

    state.isSequentialRead shouldBe true

    state.isFile shouldBe true
    state.size shouldBe 10
    state.remaining shouldBe 10

    state.moveTo(2)
    state.remaining shouldBe 8

    state.hasAtLeast(8) shouldBe true
    state.hasAtLeast(9) shouldBe false

    state.moveTo(9)
    state.hasMore shouldBe true
    state.hasAtLeast(2) shouldBe false

    state.moveTo(10)
    state.hasMore shouldBe false
  }

  "isSequentialRead" when {
    //0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    val blockSize = 5
    val bytes: Slice[Byte] = (0 to 19).map(_.toByte).toSlice
    val reader = createRandomFileReader(bytes)
    val state = BlockReader(ValuesBlock.Offset(0, bytes.size), blockSize, reader)

    "previousReadEndPosition is 0" in {
      //moving position but not reading any data will always assume the read is sequential.
      bytes foreach {
        byte =>
          state.previousReadEndPosition shouldBe 0
          state.cachedBytes shouldBe empty

          state moveTo (byte - 1)
          BlockReader.isSequentialRead(1, state) shouldBe true

          state.previousReadEndPosition shouldBe 0
          state.cachedBytes shouldBe empty
      }
    }

    "read bytes smaller than blockSIze" in {
      //empty cache
      //0, 1
      state moveTo 0
      BlockReader.read(2, state).get shouldBe bytes.take(2)
      state.isSequentialRead shouldBe true

      //0, 1, 2, 3, 4
      state.cachedBytes shouldBe (0 to 4).map(_.toByte).toSlice

      //      2, 3
      BlockReader.isSequentialRead(2, state) shouldBe true
      BlockReader.readFromCache(2, state) shouldBe Slice(2.toByte, 3.toByte)
      BlockReader.read(2, state).get shouldBe Slice(2.toByte, 3.toByte)

      //            4, 5
      BlockReader.isSequentialRead(2, state) shouldBe true
      BlockReader.readFromCache(2, state) shouldBe Slice(4.toByte)
      BlockReader.read(2, state).get shouldBe Slice(4.toByte, 5.toByte)

      //               5, 6, 7, 8, 9
      state.cachedBytes shouldBe (5 to 9).map(_.toByte).toSlice

      //now the fun stuff! Position is 6 but there is data in cache that can be read even if position jumps forward
      state.position shouldBe 6
      Random.shuffle((5 to 9).toList) foreach {
        position =>
          state moveTo position
          BlockReader.isSequentialRead(2, state) shouldBe true
      }

      //it's jumped forward but the position is still the end of the current cache block size.
      state moveTo 10
      BlockReader.isSequentialRead(2, state) shouldBe true

      //it's jumped forward to the next block's not 0 but 1's index therefore it's not sequential.
      state moveTo 11
      BlockReader.isSequentialRead(2, state) shouldBe false

      //               5, 6, 7, 8, 9
      state.cachedBytes shouldBe (5 to 9).map(_.toByte).toSlice //cached bytes are still the same.

      //jumping back to previous blocks will not result in sequential read.
      (0 to 4) foreach {
        position =>
          state moveTo position
          BlockReader.isSequentialRead(2, state) shouldBe false
      }
    }
  }

  "read" when {
    "block size is 0" in {
      val fileReader = mock[FileReader]("fileReader")

      val slice = (1 to 10).map(_.toByte).toSlice

      val state = BlockReader(reader = fileReader, offset = ValuesBlock.Offset(0, slice.size), blockSize = 0)

      (0 to 9) foreach {
        i =>
          fileReader.moveTo _ expects i returning fileReader
          fileReader.read _ expects 1 returning IO(Slice(slice(i)))

          val bytes = BlockReader.read(1, state).value
          bytes should have size 1
          bytes.head shouldBe slice(i)
          BlockReader.readFromCache(100, state) shouldBe empty
      }
    }

    "block size is larger than data size" in {
      val slice = (1 to 10).map(_.toByte).toSlice
      val fileReader = mock[FileReader]("fileReader")

      val state = BlockReader(reader = fileReader, offset = ValuesBlock.Offset(0, slice.size), blockSize = 100)

      fileReader.moveTo _ expects 0 returning fileReader
      //even though the blockSize is 100, reader only reads 10 which is the total size.
      fileReader.read _ expects 10 returning IO(slice)

      //expect only one call to fetch the bytes, the remaining will be read from the blockCache.
      (0 to 9) foreach {
        i =>
          val byte = BlockReader.read(1, state).get
          byte should have size 1
          byte.head shouldBe slice(i)
          state.cachedBytes shouldBe slice
      }
    }

    "blockSize smaller than the data size" in {
      val slice = (1 to 10).map(_.toByte).toSlice
      val fileReader = mock[FileReader]("fileReader")

      val state = BlockReader(reader = fileReader, offset = ValuesBlock.Offset(0, slice.size), blockSize = 2)

      (0 to 9) foreach {
        i =>
          //expect every second call to do a disk seek.
          if (i % 2 == 0) {
            fileReader.moveTo _ expects i returning fileReader
            //since 1 byte is being read at time and the block size is 2 only 2 bytes will requested from the file.
            fileReader.read _ expects 2 returning IO(slice.drop(i).take(2))
          }

          val byte = BlockReader.read(1, state).get
          byte should have size 1
          byte.head shouldBe slice(i)
          //there should be a minimum of 2 bytes in cache as that is the block size.
          state.cache.bytes.size shouldBe 2
      }
    }

    "blockSize is odd number" in {
      val slice = (1 to 20).map(_.toByte).toSlice
      val fileReader = mock[FileReader]("fileReader")

      val state = BlockReader(reader = fileReader, offset = ValuesBlock.Offset(0, slice.size), blockSize = 5)

      //data size.
      //{[1, 2, 3, 4, 5], [6, 7, 8, 9, 10], [11, 12, 13, 14, 15], [16, 17, 18 , 19, 20]}

      //fist expect a disk seek - [1, 2, 3, 4, 5]
      fileReader.moveTo _ expects 0 returning fileReader
      fileReader.read _ expects 5 returning IO(slice.take(5))

      //expect above disk seek [1, 2]
      var bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(1 to 2)
      state.cachedBytes shouldBe Slice.bytes(1 to 5)

      //read again, do not expect any disk seek. [3, 4]
      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(3 to 4)
      state.cachedBytes shouldBe Slice.bytes(1 to 5)

      //next one will be [5, 6] but 6 is not cached should disk seek is expected for next block.
      //expect disk seek for the next blockSize [6, 7, 8, 9, 10]
      fileReader.moveTo _ expects 5 returning fileReader
      fileReader.read _ expects 5 returning IO(slice.drop(5).take(5))

      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(5 to 6)
      state.cachedBytes shouldBe Slice.bytes(6 to 10)

      //again, no disk seek.
      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(7 to 8)
      state.cachedBytes shouldBe Slice.bytes(6 to 10)

      //again, no disk seek.
      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(9 to 10)
      state.cachedBytes shouldBe Slice.bytes(6 to 10)

      //next one will be [11, 12] but this block does not exists
      //expect disk seek for the next blockSize [11, 12, 13, 14, 15]
      fileReader.moveTo _ expects 10 returning fileReader
      fileReader.read _ expects 5 returning IO(slice.drop(10).take(5))

      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(11 to 12)
      state.cachedBytes shouldBe Slice.bytes(11 to 15)

      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(13 to 14)
      state.cachedBytes shouldBe Slice.bytes(11 to 15)

      //next one will be [15, 16] but 16 does not exist. So expect disk seek.
      //expect disk seek for the next blockSize [11, 12, 13, 14, 15]
      fileReader.moveTo _ expects 15 returning fileReader
      fileReader.read _ expects 5 returning IO(slice.drop(15).take(5))
      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(15 to 16)
      state.cachedBytes shouldBe Slice.bytes(16 to 20)

      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(17 to 18)
      state.cachedBytes shouldBe Slice.bytes(16 to 20)

      bytes = BlockReader.read(2, state).get
      bytes should have size 2
      bytes shouldBe Slice.bytes(19 to 20)
      state.cachedBytes shouldBe Slice.bytes(16 to 20)

      state.hasMore shouldBe false
      BlockReader.read(2, state).get shouldBe empty
    }
  }
}
