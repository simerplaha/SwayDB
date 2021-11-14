/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.block

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues._
import swaydb.core.PrivateMethodInvokers._
import swaydb.core.TestData._
import swaydb.core.sweeper.MemorySweeper
import swaydb.core.util.Bytes
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.slice.{Slice, SliceRO, Slices}
import swaydb.testkit.RunThis._
import swaydb.utils.StorageUnits._

import scala.jdk.CollectionConverters._

class BlockCacheSpec extends TestBase with MockFactory {

  "seekSize" should {
    val bytes = randomBytesSlice(1000)
    val blockSize = 10

    def createTestData(): (BlockCacheSource, BlockCacheState) = {
      val file: BlockCacheSource =
        new BlockCacheSource {
          override def blockCacheMaxBytes: Int =
            bytes.size

          override def readFromSource(filePosition: Int, size: Int): Slice[Byte] =
            bytes.drop(filePosition).take(size)

          override def readFromSource(filePosition: Int, size: Int, blockSize: Int): SliceRO[Byte] =
            Slices(bytes.drop(filePosition).take(size).split(blockSize))
        }

      val state =
        BlockCache.forSearch(maxCacheSizeOrZero = 0, memorySweeper = MemorySweeper.BlockSweeper(blockSize, 1.mb / 2, 100.mb, disableForSearchIO = false, actorConfig = None))

      (file, state.get)
    }

    "round size" when {
      "keyPosition <= (fileSize - blockSize)" when {

        "size == blockSize" in {
          val (source, state) = createTestData()

          (0 to source.blockCacheMaxBytes - blockSize).filter(_ % blockSize == 0) foreach {
            position =>
              val size =
                BlockCache.seekSize(
                  lowerFilePosition = position,
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

          BlockCache.seekSize(lowerFilePosition = 0, size = 11, source = source, state = state) shouldBe (blockSize * 2)
          BlockCache.seekSize(lowerFilePosition = 0, size = 21, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(lowerFilePosition = 0, size = 25, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(lowerFilePosition = 0, size = 29, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(lowerFilePosition = 0, size = 30, source = source, state = state) shouldBe (blockSize * 3) //multiple but still included in this test
          BlockCache.seekSize(lowerFilePosition = 0, size = 31, source = source, state = state) shouldBe (blockSize * 4)
          BlockCache.seekSize(lowerFilePosition = 0, size = 35, source = source, state = state) shouldBe (blockSize * 4)

          BlockCache.seekSize(lowerFilePosition = 5, size = 11, source = source, state = state) shouldBe (blockSize * 2)
          BlockCache.seekSize(lowerFilePosition = 5, size = 21, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(lowerFilePosition = 5, size = 25, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(lowerFilePosition = 5, size = 29, source = source, state = state) shouldBe (blockSize * 3)
          BlockCache.seekSize(lowerFilePosition = 5, size = 30, source = source, state = state) shouldBe (blockSize * 3) //multiple but still included in this test
          BlockCache.seekSize(lowerFilePosition = 5, size = 31, source = source, state = state) shouldBe (blockSize * 4)
          BlockCache.seekSize(lowerFilePosition = 5, size = 35, source = source, state = state) shouldBe (blockSize * 4)
        }
      }

      "keyPosition > (fileSize - blockSize)" when {

        "size == blockSize" in {
          val (file, state) = createTestData()
          //in reality position should be multiples of blockSize.
          //but this works for the test-case.
          ((file.blockCacheMaxBytes - blockSize + 1) to file.blockCacheMaxBytes) foreach {
            position =>
              val size =
                BlockCache.seekSize(
                  lowerFilePosition = position,
                  size = blockSize,
                  source = file,
                  state = state
                )

              println(s"position: $position -> size: $size")
              size shouldBe file.blockCacheMaxBytes - position
          }
        }
      }
    }

    "getOrSeek" in {
      TestCaseSweeper {
        implicit sweeper =>
          val bytes: Slice[Byte] = Slice.range(Bytes.zero, Byte.MaxValue)
          val file = createRandomFileReader(bytes).file.toBlockCacheSource

          val blockSize = 10

          val state =
            BlockCache.forSearch(0, Some(MemorySweeper.BlockSweeper(blockSize, 50000.bytes, 100.mb, disableForSearchIO = false, None))).get

          //0 -----------------------------------------> 1000
          //0 read 1
          BlockCache.getOrSeek(position = 0, size = 1, source = file, state = state) shouldBe bytes.take(1)
          val innerScalaMap = getJavaMap(state.mapCache.get().value).asScala

          innerScalaMap should have size 1
          innerScalaMap.head shouldBe(0, bytes.take(blockSize))
          val headBytesHashCode = innerScalaMap.head._2.hashCode()

          //0 -----------------------------------------> 1000
          //0 read 2
          BlockCache.getOrSeek(position = 0, size = 2, source = file, state = state)(null) shouldBe bytes.take(2)
          innerScalaMap should have size 1
          innerScalaMap.head shouldBe(0, bytes.take(blockSize))
          innerScalaMap.head._2.hashCode() shouldBe headBytesHashCode //no disk seek

          //0 -----------------------------------------> 1000
          //0 read 9
          BlockCache.getOrSeek(position = 0, size = 9, source = file, state = state)(null) shouldBe bytes.take(9)
          innerScalaMap should have size 1
          innerScalaMap.head shouldBe(0, bytes.take(blockSize))
          innerScalaMap.head._2.hashCode() shouldBe headBytesHashCode //no disk seek

          //0 -----------------------------------------> 1000
          //0 read 10
          BlockCache.getOrSeek(position = 0, size = 10, source = file, state = state)(null) shouldBe bytes.take(10)
          innerScalaMap should have size 1
          innerScalaMap.head shouldBe(0, bytes.take(blockSize))
          innerScalaMap.head._2.hashCode() shouldBe headBytesHashCode //no disk seek

          //0 -----------------------------------------> 1000
          //0 read 11
          BlockCache.getOrSeek(position = 0, size = 11, source = file, state = state) shouldBe bytes.take(11)
          innerScalaMap should have size 2
          innerScalaMap should contain(10, bytes.drop(blockSize).take(blockSize))

          //0 -----------------------------------------> 1000
          //0 read 15
          BlockCache.getOrSeek(position = 0, size = 15, source = file, state = state)(null) shouldBe bytes.take(15)
          innerScalaMap should have size 2
          innerScalaMap should contain(10, bytes.drop(blockSize).take(blockSize))

          //0 -----------------------------------------> 1000
          //0 read 19
          BlockCache.getOrSeek(position = 0, size = 19, source = file, state = state)(null) shouldBe bytes.take(19)
          innerScalaMap should have size 2
          innerScalaMap should contain(10, bytes.drop(blockSize).take(blockSize))


          //0 -----------------------------------------> 1000
          //0 read 20
          BlockCache.getOrSeek(position = 0, size = 20, source = file, state = state)(null) shouldBe bytes.take(20)
          innerScalaMap should have size 2
          innerScalaMap should contain(10, bytes.drop(blockSize).take(blockSize))
      }
    }
  }

  "randomAccess" in {
    TestCaseSweeper {
      implicit sweeper =>
        val bytes: Slice[Byte] = Slice.range(Bytes.zero, Byte.MaxValue)
        val file = createRandomFileReader(bytes).file
        runThis(500.times, log = true) {
          val blockSize = randomIntMax(bytes.size * 2)

          val state = BlockCache.forSearch(0, Some(MemorySweeper.BlockSweeper(blockSize, randomIntMax(Byte.MaxValue).bytes, 100.mb, false, None))).get

          runThis(1000.times) {
            val position = randomNextInt(bytes.size)

            val readSize = randomIntMax(bytes.size * 3)

            val seek =
              BlockCache.getOrSeek(
                position = position,
                size = readSize,
                source = file.toBlockCacheSource,
                state = state
              )

            seek shouldBe bytes.drop(position).take(readSize)
          }

          val innerScalaMap = getJavaMap(state.mapCache.get().value).asScala

          if (blockSize <= 0)
            innerScalaMap shouldBe empty
          else
            innerScalaMap should not be empty

          val (blockSized, small) =
            innerScalaMap partition {
              case (_, bytes) =>
                bytes.size == state.blockSize
            }

          //only one offset can be smaller
          small.size should be <= 1

          //byte values match the index so all the head bytes should match the index.
          innerScalaMap foreach {
            case (key, bytes) =>
              key shouldBe bytes.head
          }
        }
    }
  }
}
