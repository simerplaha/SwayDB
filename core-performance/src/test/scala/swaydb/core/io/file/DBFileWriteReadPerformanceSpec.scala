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

import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap

import swaydb.IOValues._
import swaydb.core.CommonAssertions.randomIOStrategy
import swaydb.core.TestData._
import swaydb.core.actor.FileSweeper
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.util.{Benchmark, BlockCacheFileIDGenerator}
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.config.IOStrategy
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

import scala.util.Random

class DBFileWriteReadPerformanceSpec extends TestBase {

  implicit val fileSweeper: FileSweeper.Enabled = TestLimitQueues.fileSweeper
  implicit val memorySweeper = TestLimitQueues.memorySweeper
  implicit def blockCache: Option[BlockCache.State] = TestLimitQueues.randomBlockCache

  "random access" in {
    val bytes = randomBytesSlice(20.mb)

    implicit val blockCache: Option[BlockCache.State] = TestLimitQueues.blockCache

    val mmapFile = DBFile.mmapInit(randomFilePath, randomIOStrategy(cacheOnAccess = true), bytes.size, autoClose = true, blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
    mmapFile.append(bytes).runRandomIO.right.value
    mmapFile.isFull.runRandomIO.right.value shouldBe true

    mmapFile.forceSave().get
    mmapFile.close.get

    val channelFile = DBFile.channelRead(mmapFile.path, randomIOStrategy(cacheOnAccess = true), true, blockCacheFileId = BlockCacheFileIDGenerator.nextID).get

    val reader = BlockRefReader(channelFile).get
    //    val reader = Reader(channelFile)

    val bytesToRead = 15

    Benchmark("") {
      (1 to 1000000) foreach {
        i =>
          val index = Random.nextInt(bytes.size - bytesToRead + 1)
//          println(index)
          val readBytes = reader.moveTo(index).read(bytesToRead).get
//          println(readBytes)
        //                  channelFile.read(index, bytesToRead).get

        //                                  mmapFile.read(index, bytesToRead).get

        //          reader.moveTo(i * 4).read(4).get
      }
    }

    //    println("reader.totalMiss: " + reader.totalMissed)
    //    println("reader.totalHit: " + reader.totalHit)
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

  "DBFile" should {
    //use larger chunkSize to test on larger data-set
    //    val chunkSize = 100.kb
    val chunkSize = 4096
    val bytes = randomByteChunks(1000, chunkSize)
    val flattenBytes: Slice[Byte] = bytes.flatten.toSlice

    /**
     * [[ChannelFile]] and [[FileChannel]] have nearly the same performance results but exactly the same
     * because [[ChannelFile]] has to maintain cache.
     */
    "Write performance" in {
      val path = randomFilePath
      val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)

      Benchmark("raw channel") {
        bytes.foldLeft(0) {
          case (position, bytes) =>
            channel.write(bytes.toByteBufferWrap, position)
            position + bytes.size
        }
      }

      channel.close()
      //assert that bytes were
      IOEffect.readAll(path).get shouldBe flattenBytes

      /**
       * Benchmark file channel write
       * Round 1: 1.441824636 seconds
       * Round 2: 1.328009528 seconds
       * Round 3: 1.3148811 seconds
       */
      val channelFile = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      Benchmark("FileChannel write Benchmark") {
        bytes foreach channelFile.append
      }

      //check all the bytes were written
      val readChannelFile = DBFile.channelRead(channelFile.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      readChannelFile.fileSize.runRandomIO.right.value shouldBe bytes.size * chunkSize
      IOEffect.readAll(channelFile.path).get shouldBe flattenBytes
      channelFile.close.runRandomIO.right.value
      readChannelFile.close.runRandomIO.right.value

      /**
       * Benchmark memory mapped files write
       *
       * Round 1: 0.535362744 seconds
       * Round 2: 0.58952584 seconds
       * Round 3: 0.542235514 seconds
       */

      val mmapFile = DBFile.mmapInit(randomFilePath, IOStrategy.ConcurrentIO(true), bytes.size * chunkSize, autoClose = true, blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      Benchmark("mmap write Benchmark") {
        bytes foreach mmapFile.append
      }
      mmapFile.fileSize.runRandomIO.right.value shouldBe bytes.size * chunkSize
      mmapFile.close.runRandomIO.right.value
      IOEffect.readAll(mmapFile.path).get shouldBe flattenBytes
    }

    "Get performance" in {
      val bytes = randomBytes(chunkSize)
      val file = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      file.append(Slice(bytes))
      file.close.runRandomIO.right.value

      /**
       * Benchmark file channel read
       * Round 1: 1.925951908 seconds
       * Round 2: 1.875866228 seconds
       * Round 3: 1.842739196 seconds
       */

      val channelFile = DBFile.channelRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      Benchmark("FileChannel value Benchmark") {
        bytes.indices foreach {
          index =>
            channelFile.get(index).runRandomIO.right.value shouldBe bytes(index)
        }
      }
      channelFile.close.runRandomIO.right.value

      /**
       * Benchmark memory mapped file read
       *
       * Round 1: 0.991568638 seconds
       * Round 2: 0.965750206 seconds
       * Round 3: 1.044735106 seconds
       */
      val mmapFile = DBFile.mmapRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      Benchmark("mmap value Benchmark") {
        bytes.indices foreach {
          index =>
            mmapFile.get(index).runRandomIO.right.value shouldBe bytes(index)
        }
      }
      mmapFile.close.runRandomIO.right.value
    }

    "Read 1 million bytes in chunks of 250.bytes performance" in {
      val chunkSize = 250.bytes
      val allBytes = Slice.create[Byte](1000000 * chunkSize)
      val bytes = (1 to 1000000) map {
        _ =>
          val bytes = randomBytesSlice(chunkSize)
          allBytes addAll bytes
          bytes
      }
      val file = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      bytes foreach (file.append(_).runRandomIO.right.value)
      file.close.runRandomIO.right.value

      /**
       * Benchmark file channel read
       * Round 1: 0.865503958 seconds
       * Round 2: 0.905543536 seconds
       * Round 3: 0.819253382 seconds
       */

      val channelFile = DBFile.channelRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      Benchmark("FileChannel read Benchmark") {
        bytes.foldLeft(0) {
          case (index, byteSlice) =>
            //            channelFile.read(index, chunkSize).runIO.array shouldBe byteSlice.array
            channelFile.read(index, chunkSize)
            index + chunkSize
        }
      }
      channelFile.close.runRandomIO.right.value

      /**
       * Benchmark memory mapped file read
       *
       * Round 1: 0.55484872 seconds
       * Round 2: 0.54580672 seconds
       * Round 3: 0.463990916 seconds
       */
      val mmapFile = DBFile.mmapRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(true), blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value

      Benchmark("mmap read Benchmark") {
        bytes.foldLeft(0) {
          case (index, byteSlice) =>
            //            mmapFile.read(index, chunkSize).runIO.array shouldBe byteSlice.array
            mmapFile.read(index, chunkSize)
            index + chunkSize
        }
      }

      /**
       * Benchmark memory mapped file read
       *
       * Round 1: 0.340598993 seconds
       * Round 2: 0.434818876 seconds
       * Round 3: 0.398627637 seconds
       */
      Benchmark("mmap read again Benchmark") {
        bytes.foldLeft(0) {
          case (index, byteSlice) =>
            //            mmapFile.read(index, chunkSize).runIO.array shouldBe byteSlice.array
            mmapFile.read(index, chunkSize)
            index + chunkSize
        }
      }
      mmapFile.close.runRandomIO.right.value
    }
  }
}
