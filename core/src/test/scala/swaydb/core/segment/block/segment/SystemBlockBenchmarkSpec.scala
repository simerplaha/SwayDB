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
//package swaydb.core.segment.block.segment
//
//import swaydb.Error.Segment.ExceptionHandler
//import swaydb.IO
//import swaydb.testkit.RunThis._
//import swaydb.core.TestBase
//import swaydb.core.util.Benchmark
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//
//import scala.concurrent.duration._
//
//class SystemBlockBenchmarkSpec extends TestBase {
//
//  "Benchmark reading different blockSize" in {
//    val blockSize = 4096.bytes
//    val iterations = 5000
//    //        val bytes = randomBytesSlice(blockSize * iterations)
//    val bytes = Slice.fill(blockSize * iterations)(1.toByte)
//    val fileReader = createFileStandardFileReader(bytes)
//
//    val blockSizeToReader = blockSize
//
//    //reading less than systems's default block size is slower.
//
//    //2.gb file
//    //val iterations = 500000
//    //3.41783634  seconds for blockSize / 3
//    //2.490256688 seconds for blockSize / 2
//    //1.644087263 seconds for blockSize
//    //1.259557135 seconds for blockSize * 2
//    //1.110646435 seconds for blockSize * 3
//    //1.077004249 seconds for blockSize * 4
//    //1.091550724 seconds for blockSize * 5
//    //1.208566587 & 1.684737016 seconds for blockSize * 6
//    //0.989324636 seconds for blockSize * 7
//
//    //20.mb file
//    //val iterations = 5000
//    //0.159195719 seconds for blockSize / 3
//    //0.118655509 seconds for blockSize / 2
//    //0.091687824 seconds for blockSize
//    //0.061914032 seconds for blockSize * 2
//    //0.067597028 seconds for blockSize * 3
//    //0.054243541 seconds for blockSize * 4
//
//    Benchmark("test") {
//      fileReader.foldLeftIO(0) {
//        case (offset, reader) =>
//          val readBytes = reader.read(blockSizeToReader)
//          val toOffset = offset + blockSizeToReader - 1
//          //          val expected = bytes.slice(offset, toOffset)
//          //          readBytes shouldBe expected
//          IO(toOffset + 1)
//      }
//    }
//    sleep(5.second)
//  }
//}
