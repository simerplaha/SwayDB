/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.core.TestData._
import swaydb.core.segment.block.BlockCache
import swaydb.core.segment.block.reader.BlockRefReader
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestCaseSweeper, TestSweeper}
import swaydb.effect.{Effect, IOStrategy}
import swaydb.utils.StorageUnits._

import scala.util.Random

class DBFileWriteReadPerformanceSpec extends TestBase {

  implicit val fileSweeper: FileSweeper = TestSweeper.createFileSweeper()
  implicit val bufferCleaner: ByteBufferSweeperActor = TestSweeper.createBufferCleaner()
  implicit val memorySweeper = TestSweeper.createMemorySweeperMax()
  implicit val forceSave = ForceSaveApplier.On

  //  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache

  "random access" in {
    TestCaseSweeper {
      implicit sweeper =>
        val bytes = randomBytesSlice(20.mb)

        implicit val blockCache: Option[BlockCache.State] =
          BlockCache.forSearch(bytes.size, MemorySweeper.BlockSweeper(blockSize = 4098.bytes, cacheSize = 1.gb, skipBlockCacheSeekSize = 1.mb, false, actorConfig = None))
        //          None

        val path = randomFilePath

        //        val mmapFile =
        //          DBFile.mmapInit(
        //            path = path,
        //            fileOpenIOStrategy = IOStrategy.SynchronisedIO(cacheOnAccess = true),
        //            bufferSize = bytes.size,
        //            blockCacheFileId = BlockCacheFileIDGenerator.next,
        //            autoClose = true,
        //            deleteAfterClean = false,
        //            forceSave = ForceSave.Off
        //          ).runRandomIO.right.value
        //
        //        mmapFile.append(bytes).runRandomIO.right.value
        //        mmapFile.isFull.runRandomIO.right.value shouldBe true
        //        val reader = BlockRefReader(mmapFile)

        //        mmapFile.forceSave()
        //        mmapFile.close()

        val channelFile =
          DBFile.channelRead(
            path = Effect.write(path, bytes.toByteBufferWrap),
            fileOpenIOStrategy = IOStrategy.SynchronisedIO(cacheOnAccess = true),
            autoClose = true
          )
        val reader = BlockRefReader(file = channelFile, blockCache = blockCache)

        val bytesToRead = 15

        //        case class SomeKey(int: Int)
        //        val map = new ConcurrentHashMap[SomeKey, Int]()
        //        (1 to 1000000) foreach {
        //          i =>
        //            map.put(SomeKey(i), i)
        //        }
        //
        //        val write = Slice.writeUnsignedInt[Byte](1000000)

        Benchmark("") {
          (1 to 1000000) foreach {
            i =>
              (1 to 20) foreach {
                _ =>
                  val index = Random.nextInt(bytes.size - bytesToRead + 1)
                  //              //          println(index)
                  val readBytes = reader.moveTo(index).read(bytesToRead)
                //              map.get(SomeKey(i))
                //              map.get(SomeKey(i))
                //              map.get(SomeKey(i))

                //              val write = Slice.writeUnsignedInt(i)
                //              Bytes.readUnsignedInt(write)
                //              write.readUnsignedInt().get

                //              val write = Slice.writeInt[Byte](i)
                //              write.readInt()
                //              Thread.sleep(1)
                //          println(readBytes)
                //                  channelFile.read(index, bytesToRead).get

                //                                  mmapFile.read(index, bytesToRead).get

                //          reader.moveTo(i * 4).read(4).get
              }

          }
        }

      //    println("reader.totalMiss: " + reader.totalMissed)
      //    println("reader.totalHit: " + reader.totalHit)
    }
  }

  //  "hash test" in {
  //    val bytes = (1 to 10000000) map {
  //      i =>
  //        //        val buff = ByteBuffer.allocate(4).putInt(i)
  //        //        buff.array()
  //        Slice.writeLong(i)
  //    }
  //
  //    Benchmark("") {
  //      bytes foreach {
  //        bytes =>
  //          bytes.hashCode()
  //
  //        //          MurmurHash3.arrayHash(bytes)
  //        //          MurmurHash3.orderedHash(bytes)
  //      }
  //    }
  //  }

  //  "DBFile" should {
  //    //use larger chunkSize to test on larger data-set
  //    //    val chunkSize = 100.kb
  //    val chunkSize = 4096
  //    val bytes = randomByteChunks(1000, chunkSize)
  //    val flattenBytes: Slice[Byte] = bytes.flatten.toSlice
  //
  //    /**
  //     * [[ChannelFile]] and [[FileChannel]] have nearly the same performance results but exactly the same
  //     * because [[ChannelFile]] has to maintain cache.
  //     */
  //    "Write performance" in {
  //      val path = randomFilePath
  //      val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
  //
  //      Benchmark("raw channel") {
  //        bytes.foldLeft(0) {
  //          case (position, bytes) =>
  //            channel.write(bytes.toByteBufferWrap, position)
  //            position + bytes.size
  //        }
  //      }
  //
  //      channel.close()
  //      //assert that bytes were
  //      Effect.readAll(path) shouldBe flattenBytes
  //
  //      /**
  //       * Benchmark file channel write
  //       * Round 1: 1.441824636 seconds
  //       * Round 2: 1.328009528 seconds
  //       * Round 3: 1.3148811 seconds
  //       */
  //      val channelFile = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //      Benchmark("FileChannel write Benchmark") {
  //        bytes foreach channelFile.append
  //      }
  //
  //      //check all the bytes were written
  //      val readChannelFile = DBFile.channelRead(channelFile.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //      readChannelFile.fileSize.runRandomIO.right.value shouldBe bytes.size * chunkSize
  //      Effect.readAll(channelFile.path) shouldBe flattenBytes
  //      channelFile.close().runRandomIO.right.value
  //      readChannelFile.close().runRandomIO.right.value
  //
  //      /**
  //       * Benchmark memory mapped files write
  //       *
  //       * Round 1: 0.535362744 seconds
  //       * Round 2: 0.58952584 seconds
  //       * Round 3: 0.542235514 seconds
  //       */
  //
  //      val mmapFile = DBFile.mmapInit(randomFilePath, IOStrategy.ConcurrentIO(true), bytes.size * chunkSize, autoClose = true, deleteAfterClean = OperatingSystem.isWindows, blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //      Benchmark("mmap write Benchmark") {
  //        bytes foreach mmapFile.append
  //      }
  //      mmapFile.fileSize.runRandomIO.right.value shouldBe bytes.size * chunkSize
  //      mmapFile.close.runRandomIO.right.value
  //      Effect.readAll(mmapFile.path) shouldBe flattenBytes
  //    }
  //
  //    "Get performance" in {
  //      val bytes = randomBytes(chunkSize)
  //      val file = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //      file.append(Slice(bytes))
  //      file.close().runRandomIO.right.value
  //
  //      /**
  //       * Benchmark file channel read
  //       * Round 1: 1.925951908 seconds
  //       * Round 2: 1.875866228 seconds
  //       * Round 3: 1.842739196 seconds
  //       */
  //
  //      val channelFile = DBFile.channelRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //      Benchmark("FileChannel value Benchmark") {
  //        bytes.indices foreach {
  //          index =>
  //            channelFile.get(index).runRandomIO.right.value shouldBe bytes(index)
  //        }
  //      }
  //      channelFile.close().runRandomIO.right.value
  //
  //      /**
  //       * Benchmark memory mapped file read
  //       *
  //       * Round 1: 0.991568638 seconds
  //       * Round 2: 0.965750206 seconds
  //       * Round 3: 1.044735106 seconds
  //       */
  //      val mmapFile = DBFile.mmapRead(file.path, autoClose = true, deleteAfterClean = OperatingSystem.isWindows, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //      Benchmark("mmap value Benchmark") {
  //        bytes.indices foreach {
  //          index =>
  //            mmapFile.get(index).runRandomIO.right.value shouldBe bytes(index)
  //        }
  //      }
  //      mmapFile.close.runRandomIO.right.value
  //    }
  //
  //    "Read 1 million bytes in chunks of 250.bytes performance" in {
  //      val chunkSize = 250.bytes
  //      val allBytes = Slice.create[Byte](1000000 * chunkSize)
  //      val bytes = (1 to 1000000) map {
  //        _ =>
  //          val bytes = randomBytesSlice(chunkSize)
  //          allBytes addAll bytes
  //          bytes
  //      }
  //      val file = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //      bytes foreach (file.append(_).runRandomIO.right.value)
  //      file.close().runRandomIO.right.value
  //
  //      /**
  //       * Benchmark file channel read
  //       * Round 1: 0.865503958 seconds
  //       * Round 2: 0.905543536 seconds
  //       * Round 3: 0.819253382 seconds
  //       */
  //
  //      val channelFile = DBFile.channelRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //      Benchmark("FileChannel read Benchmark") {
  //        bytes.foldLeft(0) {
  //          case (index, byteSlice) =>
  //            //            channelFile.read(index, chunkSize).runIO.array shouldBe byteSlice.array
  //            channelFile.read(index, chunkSize)
  //            index + chunkSize
  //        }
  //      }
  //      channelFile.close().runRandomIO.right.value
  //
  //      /**
  //       * Benchmark memory mapped file read
  //       *
  //       * Round 1: 0.55484872 seconds
  //       * Round 2: 0.54580672 seconds
  //       * Round 3: 0.463990916 seconds
  //       */
  //      val mmapFile = DBFile.mmapRead(file.path, autoClose = true, deleteAfterClean = OperatingSystem.isWindows, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
  //
  //      Benchmark("mmap read Benchmark") {
  //        bytes.foldLeft(0) {
  //          case (index, byteSlice) =>
  //            //            mmapFile.read(index, chunkSize).runIO.array shouldBe byteSlice.array
  //            mmapFile.read(index, chunkSize)
  //            index + chunkSize
  //        }
  //      }
  //
  //      /**
  //       * Benchmark memory mapped file read
  //       *
  //       * Round 1: 0.340598993 seconds
  //       * Round 2: 0.434818876 seconds
  //       * Round 3: 0.398627637 seconds
  //       */
  //      Benchmark("mmap read again Benchmark") {
  //        bytes.foldLeft(0) {
  //          case (index, byteSlice) =>
  //            //            mmapFile.read(index, chunkSize).runIO.array shouldBe byteSlice.array
  //            mmapFile.read(index, chunkSize)
  //            index + chunkSize
  //        }
  //      }
  //      mmapFile.close.runRandomIO.right.value
  //    }
  //  }
}
