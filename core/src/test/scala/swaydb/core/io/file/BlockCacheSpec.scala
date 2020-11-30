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

package swaydb.core.io.file

import org.scalamock.scalatest.MockFactory
import swaydb.core.TestData._
import swaydb.core.actor.MemorySweeper
import swaydb.core.io.file.BlockCache.Key
import swaydb.core.util.Bytes
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.RunThis._
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class BlockCacheSpec extends TestBase with MockFactory {

  "seekSize" should {
    val bytes = randomBytesSlice(1000)
    val blockSize = 10

    def createTestData() = {
      val file = new BlockCacheSource {
        override def size: Long =
          bytes.size

        override def read(filePosition: Int, size: Int): Slice[Byte] =
          bytes.drop(filePosition).take(size)
      }

      val state =
        BlockCache.init(MemorySweeper.BlockSweeper(blockSize, 1.mb / 2, 100.mb, None))

      (file, state)
    }

    "round size" when {
      "keyPosition <= (fileSize - blockSize)" when {

        "size == blockSize" in {
          val (source, state) = createTestData()

          (0 to source.size.toInt - blockSize).filter(_ % blockSize == 0) foreach {
            position =>
              val size =
                BlockCache.seekSize(
                  keyPosition = position,
                  size = blockSize,
                  source = source,
                  state = state
                )

              println(s"position: $position -> size: $size")
              size shouldBe blockSize
          }
        }

        "size > multiples of blockSize" in {
          val (source, state) = createTestData()

          BlockCache.seekSize(keyPosition = 0, size = 11, source = source, state = state) shouldBe (blockSize * 2)
          BlockCache.seekSize(keyPosition = 0, size = 21, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 0, size = 25, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 0, size = 29, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 0, size = 30, source = source, state = state) shouldBe (blockSize * 3) //multiple but still included in this test
          BlockCache.seekSize(keyPosition = 0, size = 31, source = source, state = state) shouldBe (blockSize * 4)
          BlockCache.seekSize(keyPosition = 0, size = 35, source = source, state = state) shouldBe (blockSize * 4)

          BlockCache.seekSize(keyPosition = 5, size = 11, source = source, state = state) shouldBe (blockSize * 2)
          BlockCache.seekSize(keyPosition = 5, size = 21, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 5, size = 25, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 5, size = 29, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(keyPosition = 5, size = 30, source = source, state = state) shouldBe (blockSize * 3) //multiple but still included in this test
          BlockCache.seekSize(keyPosition = 5, size = 31, source = source, state = state) shouldBe (blockSize * 4)
          BlockCache.seekSize(keyPosition = 5, size = 35, source = source, state = state) shouldBe (blockSize * 4)
        }
      }

      "keyPosition > (fileSize - blockSize)" when {

        "size == blockSize" in {
          val (file, state) = createTestData()
          //in reality position should be multiples of blockSize.
          //but this works for the test-case.
          ((file.size.toInt - blockSize + 1) to file.size.toInt) foreach {
            position =>
              val size =
                BlockCache.seekSize(
                  keyPosition = position,
                  size = blockSize,
                  source = file,
                  state = state
                )

              println(s"position: $position -> size: $size")
              size shouldBe file.size.toInt - position
          }
        }
      }
    }

    "getOrSeek" in {
      TestCaseSweeper {
        implicit sweeper =>
          val bytes: Slice[Byte] = Slice.range(Bytes.zero, Byte.MaxValue)
          val file = createRandomFileReader(bytes).file.file

          val blockSize = 10

          val state =
            BlockCache.init(MemorySweeper.BlockSweeper(blockSize, 50000.bytes, 100.mb, None))

          //0 -----------------------------------------> 1000
          //0 read 1
          BlockCache.getOrSeek(sourceId = 0, paddingLeft = 0, position = 0, size = 1, source = file, state = state) shouldBe bytes.take(1)
          state.map.asScala should have size 1
          state.map.head shouldBe(Key(0, 0), bytes.take(blockSize))
          val headBytesHashCode = state.map.head._2.hashCode()

          //0 -----------------------------------------> 1000
          //0 read 2
          BlockCache.getOrSeek(sourceId = 0, paddingLeft = 0, position = 0, size = 2, source = file, state = state)(null) shouldBe bytes.take(2)
          state.map.asScala should have size 1
          state.map.head shouldBe(Key(0, 0), bytes.take(blockSize))
          state.map.head._2.hashCode() shouldBe headBytesHashCode //no disk seek

          //0 -----------------------------------------> 1000
          //0 read 9
          BlockCache.getOrSeek(sourceId = 0, paddingLeft = 0, position = 0, size = 9, source = file, state = state)(null) shouldBe bytes.take(9)
          state.map.asScala should have size 1
          state.map.head shouldBe(Key(0, 0), bytes.take(blockSize))
          state.map.head._2.hashCode() shouldBe headBytesHashCode //no disk seek

          //0 -----------------------------------------> 1000
          //0 read 10
          BlockCache.getOrSeek(sourceId = 0, paddingLeft = 0, position = 0, size = 10, source = file, state = state)(null) shouldBe bytes.take(10)
          state.map.asScala should have size 1
          state.map.head shouldBe(Key(0, 0), bytes.take(blockSize))
          state.map.head._2.hashCode() shouldBe headBytesHashCode //no disk seek

          //0 -----------------------------------------> 1000
          //0 read 11
          BlockCache.getOrSeek(sourceId = 0, paddingLeft = 0, position = 0, size = 11, source = file, state = state) shouldBe bytes.take(11)
          state.map.asScala should have size 2
          state.map.asScala should contain(Key(0, 10), bytes.drop(blockSize).take(blockSize))

          //0 -----------------------------------------> 1000
          //0 read 15
          BlockCache.getOrSeek(sourceId = 0, paddingLeft = 0, position = 0, size = 15, source = file, state = state)(null) shouldBe bytes.take(15)
          state.map.asScala should have size 2
          state.map.asScala should contain(Key(0, 10), bytes.drop(blockSize).take(blockSize))

          //0 -----------------------------------------> 1000
          //0 read 19
          BlockCache.getOrSeek(sourceId = 0, paddingLeft = 0, position = 0, size = 19, source = file, state = state)(null) shouldBe bytes.take(19)
          state.map.asScala should have size 2
          state.map.asScala should contain(Key(0, 10), bytes.drop(blockSize).take(blockSize))


          //0 -----------------------------------------> 1000
          //0 read 20
          BlockCache.getOrSeek(sourceId = 0, paddingLeft = 0, position = 0, size = 20, source = file, state = state)(null) shouldBe bytes.take(20)
          state.map.asScala should have size 2
          state.map.asScala should contain(Key(0, 10), bytes.drop(blockSize).take(blockSize))
      }
    }
  }

  "randomAccess" in {
    TestCaseSweeper {
      implicit sweeper =>
        val bytes: Slice[Byte] = Slice.range(Bytes.zero, Byte.MaxValue)
        val file = createRandomFileReader(bytes).file.file
        runThis(500.times, log = true) {
          val blockSize = randomIntMax(bytes.size * 2)

          val state = BlockCache.init(MemorySweeper.BlockSweeper(blockSize, randomIntMax(Byte.MaxValue).bytes, 100.mb, None))

          runThis(1000.times) {
            val position = randomNextInt(bytes.size)

            val readSize = randomIntMax(bytes.size * 3)

            val seek =
              BlockCache.getOrSeek(
                sourceId = 0,
                paddingLeft = 0,
                position = position,
                size = readSize,
                source = file,
                state = state
              )

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
            case (key, bytes) =>
              key.position shouldBe bytes.head
          }
        }
    }
  }

  "fetch cached bytes" when {
    "padding has changed but sourceId is consistent" in {
      val bytes: Slice[Byte] = Slice.range(Byte.MinValue, Byte.MaxValue)
      val source = mock[BlockCacheSource]

      (source.size _).expects() returning bytes.size // only called once
      (source.read _).expects(*, *).onCall { // only called once
        (position, size) =>
          bytes.drop(position).take(size)
      }

      val state = BlockCache.init(MemorySweeper.BlockSweeper(blockSize = 10.bytes, cacheSize = 10.mb, skipBlockCacheSeekSize = 30.bytes, actorConfig = None))

      def doSeek() =
        BlockCache.getOrSeek(
          sourceId = 0,
          paddingLeft = 0,
          position = 1,
          size = 10,
          source = source,
          state = state
        )

      val expected = bytes.drop(1).take(10)

      doSeek() shouldBe expected
      //called again but the mock does not get touched (no IO performed)
      //because data is fetched from the cache
      doSeek() shouldBe expected

      //doesn't matter what position we read it from as long as
      //sourceId remains the same and paddingLeft indicates the original
      //position we will get the same data from the cache and the BlockCacheSource
      //is never read again.
      runThis(10.times) {
        val filePosition = randomIntMax()

        BlockCache.getOrSeek(
          sourceId = 0,
          paddingLeft = filePosition - 1,
          position = filePosition,
          size = 10,
          source = source,
          state = state
        ) shouldBe expected
      }
    }
  }
}
