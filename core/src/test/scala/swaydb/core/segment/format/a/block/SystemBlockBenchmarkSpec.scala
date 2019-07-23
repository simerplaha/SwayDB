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

import swaydb.IO
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.util.Benchmark
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._
import swaydb.ErrorHandler.CoreErrorHandler

class SystemBlockBenchmarkSpec extends TestBase {

  "Benchmark reading different blockSize" in {
    val blockSize = 4096.bytes
    val iterations = 5000
    //        val bytes = randomBytesSlice(blockSize * iterations)
    val bytes = Slice.fill(blockSize * iterations)(1.toByte)
    val fileReader = createFileChannelFileReader(bytes)

    val blockSizeToReader = blockSize

    //reading less than systems's default block size is slower.

    //2.gb file
    //val iterations = 500000
    //3.41783634  seconds for blockSize / 3
    //2.490256688 seconds for blockSize / 2
    //1.644087263 seconds for blockSize
    //1.259557135 seconds for blockSize * 2
    //1.110646435 seconds for blockSize * 3
    //1.077004249 seconds for blockSize * 4
    //1.091550724 seconds for blockSize * 5
    //1.208566587 & 1.684737016 seconds for blockSize * 6
    //0.989324636 seconds for blockSize * 7

    //20.mb file
    //val iterations = 5000
    //0.159195719 seconds for blockSize / 3
    //0.118655509 seconds for blockSize / 2
    //0.091687824 seconds for blockSize
    //0.061914032 seconds for blockSize * 2
    //0.067597028 seconds for blockSize * 3
    //0.054243541 seconds for blockSize * 4

    Benchmark("test") {
      fileReader.foldLeftIO(0) {
        case (offset, reader) =>
          val readBytes = reader.read(blockSizeToReader).get
          val toOffset = offset + blockSizeToReader - 1
          //          val expected = bytes.slice(offset, toOffset)
          //          readBytes shouldBe expected
          IO(toOffset + 1)
      }
    }
    sleep(5.second)
  }
}
