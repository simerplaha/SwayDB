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

package swaydb.core.io.file

import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.data.slice.Slice

class FileBlockCacheSpec extends TestBase {

  "seekSize" should {
    val bytes = randomBytesSlice(1000)
    val file = createRandomFileReader(bytes).file.file.get

    val blockSize = 10

    val state =
      BlockCache.init(blockSize = blockSize)

    "round size" when {
      "keyPosition <= (fileSize - blockSize)" when {

        "size == blockSize" in {
          (0 to file.fileSize.get.toInt - blockSize).filter(_ % blockSize == 0) foreach {
            position =>
              val size =
                BlockCache.seekSize(
                  keyPosition = position,
                  size = blockSize,
                  file = file,
                  state = state
                ).value

              println(s"position: $position -> size: $size")
              size shouldBe blockSize
          }
        }

        "size > multiples of blockSize" in {
          BlockCache.seekSize(keyPosition = 0, size = 11, file = file, state = state).value shouldBe (blockSize * 2)
          BlockCache.seekSize(keyPosition = 0, size = 21, file = file, state = state).value shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 0, size = 25, file = file, state = state).value shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 0, size = 29, file = file, state = state).value shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 0, size = 30, file = file, state = state).value shouldBe (blockSize * 3) //multiple but still included in this test
          BlockCache.seekSize(keyPosition = 0, size = 31, file = file, state = state).value shouldBe (blockSize * 4)
          BlockCache.seekSize(keyPosition = 0, size = 35, file = file, state = state).value shouldBe (blockSize * 4)

          BlockCache.seekSize(keyPosition = 5, size = 11, file = file, state = state).value shouldBe (blockSize * 2)
          BlockCache.seekSize(keyPosition = 5, size = 21, file = file, state = state).value shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 5, size = 25, file = file, state = state).value shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 5, size = 29, file = file, state = state).value shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 5, size = 30, file = file, state = state).value shouldBe (blockSize * 3) //multiple but still included in this test
          BlockCache.seekSize(keyPosition = 5, size = 31, file = file, state = state).value shouldBe (blockSize * 4)
          BlockCache.seekSize(keyPosition = 5, size = 35, file = file, state = state).value shouldBe (blockSize * 4)
        }
      }

      "keyPosition > (fileSize - blockSize)" when {

        "size == blockSize" in {
          //in reality position should be multiples of blockSize.
          //but this works for the test-case.
          ((file.fileSize.get.toInt - blockSize + 1) to file.fileSize.get.toInt) foreach {
            position =>
              val size =
                BlockCache.seekSize(
                  keyPosition = position,
                  size = blockSize,
                  file = file,
                  state = state
                ).value

              println(s"position: $position -> size: $size")
              size shouldBe file.fileSize.get.toInt - position
          }
        }
      }
    }

    "getOrSeek" in {
      val bytes: Slice[Byte] = (0.toByte to Byte.MaxValue).toSlice.map(_.toByte)
      val file = createRandomFileReader(bytes).file.file.get

      val blockSize = 10

      val state =
        BlockCache.init(blockSize = blockSize)

      //0 -----------------------------------------> 1000
      //0 read 1
      BlockCache.getOrSeek(position = 0, size = 1, file = file, state = state).get shouldBe bytes.take(1)
      state.map.asScala should have size 1
      state.map.head shouldBe((file.path, 0), bytes.take(blockSize))
      var headBytesHashCode = state.map.head._2.hashCode()

      //0 -----------------------------------------> 1000
      //0 read 2
      BlockCache.getOrSeek(position = 0, size = 2, file = file, state = state)(null).get shouldBe bytes.take(2)
      state.map.asScala should have size 1
      state.map.head shouldBe((file.path, 0), bytes.take(blockSize))
      state.map.head._2.hashCode() shouldBe headBytesHashCode //no disk seek

      //0 -----------------------------------------> 1000
      //0 read 9
      BlockCache.getOrSeek(position = 0, size = 9, file = file, state = state)(null).get shouldBe bytes.take(9)
      state.map.asScala should have size 1
      state.map.head shouldBe((file.path, 0), bytes.take(blockSize))
      state.map.head._2.hashCode() shouldBe headBytesHashCode //no disk seek

      //0 -----------------------------------------> 1000
      //0 read 10
      BlockCache.getOrSeek(position = 0, size = 10, file = file, state = state)(null).get shouldBe bytes.take(10)
      state.map.asScala should have size 1
      state.map.head shouldBe((file.path, 0), bytes.take(blockSize))
      state.map.head._2.hashCode() shouldBe headBytesHashCode //no disk seek


      //0 -----------------------------------------> 1000
      //0 read 11
      BlockCache.getOrSeek(position = 0, size = 11, file = file, state = state).get shouldBe bytes.take(11)
      state.map.asScala should have size 2
      state.map.last shouldBe((file.path, 10), bytes.drop(blockSize).take(blockSize))

      //0 -----------------------------------------> 1000
      //0 read 15
      BlockCache.getOrSeek(position = 0, size = 15, file = file, state = state)(null).get shouldBe bytes.take(15)
      state.map.asScala should have size 2
      state.map.last shouldBe((file.path, 10), bytes.drop(blockSize).take(blockSize))

      //0 -----------------------------------------> 1000
      //0 read 19
      BlockCache.getOrSeek(position = 0, size = 19, file = file, state = state)(null).get shouldBe bytes.take(19)
      state.map.asScala should have size 2
      state.map.last shouldBe((file.path, 10), bytes.drop(blockSize).take(blockSize))


      //0 -----------------------------------------> 1000
      //0 read 20
      BlockCache.getOrSeek(position = 0, size = 20, file = file, state = state)(null).get shouldBe bytes.take(20)
      state.map.asScala should have size 2
      state.map.last shouldBe((file.path, 10), bytes.drop(blockSize).take(blockSize))
    }
  }

  "randomAccess" in {
    val bytes: Slice[Byte] = (0.toByte to Byte.MaxValue).toSlice.map(_.toByte)
    val file = createRandomFileReader(bytes).file.file.get
    runThis(500.times, log = true) {
      val blockSize = randomIntMax(bytes.size * 2)

      val state =
        BlockCache.init(
          blockSize = blockSize
        )

      runThis(1000.times) {
        val position = randomNextInt(bytes.size)

        val readSize = randomIntMax(bytes.size * 3)

        val seek =
          BlockCache.getOrSeek(
            position = position,
            size = readSize,
            file = file,
            state = state
          ).get

        seek shouldBe bytes.drop(position).take(readSize)
      }

      if (blockSize <= 0)
        state.map.asScala shouldBe empty
      else
        state.map.asScala should not be empty

      val (blockSized, small) =
        state.map.asScala partition {
          case (_, bytes) =>
            bytes.size == state.blockSize
        }

      //only one offset can be smaller
      small.size should be <= 1

      //byte values match the index so all the head bytes should match the index.
      state.map.asScala foreach {
        case (offset, bytes) =>
          offset._2 shouldBe bytes.head
      }
    }
  }
}
