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

package swaydb.core.segment.format.a.block

import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.queue.FileLimiter
import swaydb.core.segment.format.a.block.reader.{BlockReader, BlockRefReader}
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.config.IOStrategy
import swaydb.data.util.StorageUnits._
import swaydb.core.io.reader.FileReader

class BlockReaderPerformanceSpec extends TestBase {

  implicit val fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter

  "random access" in {

    val bytes = randomBytesSlice(20.mb)

    val ioStrategy = IOStrategy.SynchronisedIO(cacheOnAccess = true)

    val file = DBFile.mmapInit(randomFilePath, blockSize = randomBlockSize(), ioStrategy, bytes.size, autoClose = true).runRandomIO.value
    file.append(bytes).runRandomIO.value
    file.isFull.runRandomIO.value shouldBe true
    file.forceSave().get
    file.close.get

    val readerFile = DBFile.mmapRead(path = file.path, ioStrategy = ioStrategy, autoClose = true, blockSize = randomBlockSize()).get

    /**
     * @note For randomReads:
     *       - [[FileReader]] seem to outperform [[BlockRefReader]] for random reads and
     *       [[BlockRefReader]] beats [[FileReader]] for sequential.
     *       [[BlockReader]] might need some more improvements to detect between random and sequential reads.
     *
     *       - [[FileReader]] has the same performance as reading from the [[file]] directly.
     */
    //        val reader = Reader(bytes)
    val reader = BlockRefReader(readerFile).get
    //    val reader = Reader(readerFile)

    Benchmark("") {
      (1 to 3000000) foreach {
        i =>
          //          val index = randomIntMax(bytes.size - 5) //random read
          val index = i * 4 //sequential read

          //                    file.read(index, 4).get
          reader.moveTo(index).read(4).get
      }
    }
  }
}