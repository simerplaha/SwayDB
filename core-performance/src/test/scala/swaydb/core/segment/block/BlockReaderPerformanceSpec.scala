///*
// * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// *
// * Additional permission under the GNU Affero GPL version 3 section 7:
// * If you modify this Program or any covered work, only by linking or
// * combining it with separate works, the licensors of this Program grant
// * you additional permission to convey the resulting work.
// */
//
//package swaydb.core.segment.block
//
//import swaydb.IOValues._
//import swaydb.core.TestData._
//import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
//import swaydb.core.sweeper.FileSweeper
//import swaydb.core.io.file.{BlockCache, DBFile}
//import swaydb.core.io.reader.FileReader
//import swaydb.core.segment.block.reader.{BlockReader, BlockRefReader}
//import swaydb.core.util.{Benchmark, BlockCacheFileIDGenerator}
//import swaydb.core.{TestBase, TestSweeper}
//import swaydb.data.config.IOStrategy
//import swaydb.data.util.OperatingSystem
//import swaydb.data.util.StorageUnits._
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
//      DBFile.mmapInit(
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
//      DBFile.mmapRead(
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
