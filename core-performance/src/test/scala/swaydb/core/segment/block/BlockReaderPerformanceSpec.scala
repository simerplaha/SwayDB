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
//
//package swaydb.core.segment.block
//
//import swaydb.IOValues._
//import swaydb.core.TestData._
//import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
//import swaydb.core.file.sweeper.FileSweeper
//import swaydb.core.file.{BlockCache, CoreFile}
//import swaydb.core.file.reader.FileReader
//import swaydb.core.segment.block.reader.{BlockReader, BlockRefReader}
//import swaydb.core.util.{Benchmark, BlockCacheFileIDGenerator}
//import swaydb.core.{TestBase, TestSweeper}
//import swaydb.config.IOStrategy
//import swaydb.config.util.OperatingSystem
//import swaydb.config.util.StorageUnits._
//
//class BlockReaderPerformanceSpec extends TestBase {
//
//  implicit val fileSweeper: FileSweeper = TestSweeper.fileSweeper
//  implicit val bufferCleaner: ByteBufferSweeperActor  = TestSweeper.bufferCleaner
//  implicit val memorySweeper = TestSweeper.memorySweeperMax
//
//  "random access" in {
//
//    val bytes = randomBytesSlice(20.mb)
//
//    val ioStrategy = IOStrategy.SynchronisedIO(cacheOnAccess = true)
//
//    val file =
//      CoreFile.mmapInit(
//        path = randomFilePath,
//        ioStrategy = ioStrategy,
//        bufferSize = bytes.size,
//        blockCacheFileId = BlockCacheFileIDGenerator.nextID,
//        autoClose = true,
//        deleteAfterClean = OperatingSystem.isWindows
//      ).runRandomIO.right.value
//
//    file.append(bytes).runRandomIO.right.value
//    file.isFull.runRandomIO.right.value shouldBe true
//    file.forceSave()
//    file.close()
//
//    val readerFile =
//      CoreFile.mmapRead(
//        path = file.path,
//        ioStrategy = ioStrategy,
//        autoClose = true,
//        deleteAfterClean = OperatingSystem.isWindows,
//        blockCacheFileId = BlockCacheFileIDGenerator.nextID
//      )
//
//    /**
//     * @note For randomReads:
//     *       - [[FileReader]] seem to outperform [[BlockRefReader]] for random reads and
//     *         [[BlockRefReader]] beats [[FileReader]] for sequential.
//     *         [[BlockReader]] might need some more improvements to detect between random and sequential reads.
//     *
//     *       - [[FileReader]] has the same performance as reading from the [[file]] directly.
//     */
//    //        val reader = Reader(bytes)
//    val reader = BlockRefReader(readerFile)
//    //    val reader = Reader(readerFile)
//
//    Benchmark("") {
//      (1 to 3000000) foreach {
//        i =>
//          //          val index = randomIntMax(bytes.size - 5) //random read
//          val index = i * 4 //sequential read
//
//          //                    file.read(index, 4).get
//          reader.moveTo(index).read(4)
//      }
//    }
//  }
//}
