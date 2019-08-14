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
import swaydb.core.TestData._
import swaydb.core.queue.FileLimiter
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.config.IOStrategy
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class DBFileWriteReadPerformanceSpec extends TestBase {

  implicit val fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter

  "DBFile" should {
    //use larger chunkSize to test on larger data-set
    val chunkSize = 100.kb
    //val chunkSize = 1.mb

    "Write performance" in {

      val bytes = (1 to 1000) map (_ => randomBytesSlice(chunkSize))
      /**
       * Benchmark file channel write
       * Round 1: 1.441824636 seconds
       * Round 2: 1.328009528 seconds
       * Round 3: 1.3148811 seconds
       */
      val channelFile = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(false)).runRandomIO.value
      Benchmark("FileChannel write Benchmark") {
        bytes foreach channelFile.append
      }
      //check all the bytes were written
      val readChannelFile = DBFile.channelRead(channelFile.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(false)).runRandomIO.value
      readChannelFile.fileSize.runRandomIO.value shouldBe bytes.size * chunkSize

      /**
       * Benchmark memory mapped files write
       *
       * Round 1: 0.535362744 seconds
       * Round 2: 0.58952584 seconds
       * Round 3: 0.542235514 seconds
       */

      val mmapFile = DBFile.mmapInit(randomFilePath, IOStrategy.ConcurrentIO(false), bytes.size * chunkSize, autoClose = true).runRandomIO.value
      Benchmark("mmap write Benchmark") {
        bytes foreach mmapFile.append
      }
      mmapFile.fileSize.runRandomIO.value shouldBe bytes.size * chunkSize

      channelFile.close.runRandomIO.value
      readChannelFile.close.runRandomIO.value
      mmapFile.close.runRandomIO.value
    }

    "Get performance" in {
      val bytes = randomBytes(chunkSize)
      val file = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(false)).runRandomIO.value
      file.append(Slice(bytes))
      file.close.runRandomIO.value

      /**
       * Benchmark file channel read
       * Round 1: 1.925951908 seconds
       * Round 2: 1.875866228 seconds
       * Round 3: 1.842739196 seconds
       */

      val channelFile = DBFile.channelRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(false)).runRandomIO.value
      Benchmark("FileChannel value Benchmark") {
        bytes.indices foreach {
          index =>
            channelFile.get(index).runRandomIO.value shouldBe bytes(index)
        }
      }
      channelFile.close.runRandomIO.value

      /**
       * Benchmark memory mapped file read
       *
       * Round 1: 0.991568638 seconds
       * Round 2: 0.965750206 seconds
       * Round 3: 1.044735106 seconds
       */
      val mmapFile = DBFile.mmapRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(false)).runRandomIO.value
      Benchmark("mmap value Benchmark") {
        bytes.indices foreach {
          index =>
            mmapFile.get(index).runRandomIO.value shouldBe bytes(index)
        }
      }
      mmapFile.close.runRandomIO.value
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
      val file = DBFile.channelWrite(randomFilePath, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(false)).runRandomIO.value
      bytes foreach (file.append(_).runRandomIO.value)
      file.close.runRandomIO.value

      /**
       * Benchmark file channel read
       * Round 1: 0.865503958 seconds
       * Round 2: 0.905543536 seconds
       * Round 3: 0.819253382 seconds
       */

      val channelFile = DBFile.channelRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(false)).runRandomIO.value
      Benchmark("FileChannel read Benchmark") {
        bytes.foldLeft(0) {
          case (index, byteSlice) =>
            //            channelFile.read(index, chunkSize).runIO.array shouldBe byteSlice.array
            channelFile.read(index, chunkSize)
            index + chunkSize
        }
      }
      channelFile.close.runRandomIO.value

      /**
       * Benchmark memory mapped file read
       *
       * Round 1: 0.55484872 seconds
       * Round 2: 0.54580672 seconds
       * Round 3: 0.463990916 seconds
       */
      val mmapFile = DBFile.mmapRead(file.path, autoClose = true, ioStrategy = IOStrategy.ConcurrentIO(false)).runRandomIO.value

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
      mmapFile.close.runRandomIO.value
    }
  }
}
