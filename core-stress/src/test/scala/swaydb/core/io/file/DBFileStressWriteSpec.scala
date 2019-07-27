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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.io.file

import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.TestLimitQueues.fileOpenLimiter
import swaydb.core.util.Benchmark
import swaydb.data.util.StorageUnits._

import scala.concurrent.Future
import scala.concurrent.duration._

class DBFileStressWriteSpec extends TestBase with Benchmark {

  implicit val limiter = fileOpenLimiter

  "DBFile" should {
    //use a larger size (200000) to test on larger data-set.
    val bytes = randomByteChunks(size = 20000, sizePerChunk = 50.bytes)

    "write key values to a ChannelFile" in {
      val path = randomFilePath

      val file = DBFile.channelWrite(path, autoClose = false).runIO
      benchmark("write 1 million key values to a ChannelFile") {
        bytes foreach {
          byteChunk =>
            file.append(byteChunk).runIO
        }
      }
      file.close.runIO
    }

    "write key values to a ChannelFile concurrently" in {
      val path = randomFilePath

      val file = DBFile.channelWrite(path, autoClose = false).runIO
      benchmark("write 1 million key values to a ChannelFile concurrently") {
        Future.sequence {
          bytes map {
            chunk =>
              Future(file.append(chunk).runIO)
          }
        } await 20.seconds
      }
      file.close.runIO
    }

    "write key values to a MMAPlFile" in {
      val path = randomFilePath

      val file = DBFile.mmapInit(path, bytes.size * 50, autoClose = false).runIO
      benchmark("write 1 million key values to a MMAPlFile") {
        bytes foreach {
          chunk =>
            file.append(chunk).runIO
        }
      }
      file.close.runIO
    }

    "write key values to a MMAPlFile concurrently" in {
      val path = randomFilePath

      val file = DBFile.mmapInit(path, bytes.size * 50, autoClose = false).runIO
      benchmark("write 1 million key values to a MMAPlFile concurrently") {
        Future.sequence {
          bytes map {
            chunk =>
              Future(file.append(chunk).runIO)
          }
        } await 20.seconds
      }
      file.close.runIO
    }
  }
}
