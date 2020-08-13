/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.TestSweeper.fileSweeper
import swaydb.core.actor.FileSweeper
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.util.{Benchmark, BlockCacheFileIDGenerator}
import swaydb.core.{TestBase, TestSweeper}
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._

import scala.concurrent.Future
import scala.concurrent.duration._

class DBFileStressWriteSpec extends TestBase {

  implicit val fileSweeper: FileSweeper.Enabled = TestSweeper.fileSweeper
  implicit val bufferCleaner: ByteBufferSweeperActor  = TestSweeper.bufferCleaner
  implicit val memorySweeper = TestSweeper.memorySweeperMax

  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache

  "DBFile" should {
    //use a larger size (200000) to test on larger data-set.
    val bytes = randomByteChunks(size = 20000, sizePerChunk = 50.bytes)

    "write key values to a ChannelFile" in {
      val path = randomFilePath

      val file = DBFile.channelWrite(path, randomIOAccess(true), autoClose = false, blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      Benchmark("write 1 million key values to a ChannelFile") {
        bytes foreach {
          byteChunk =>
            file.append(byteChunk).runRandomIO.right.value
        }
      }
      file.close().runRandomIO.right.value
    }

    "write key values to a ChannelFile concurrently" in {
      val path = randomFilePath

      val file = DBFile.channelWrite(path, randomIOAccess(true), autoClose = false, blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value
      Benchmark("write 1 million key values to a ChannelFile concurrently") {
        Future.sequence {
          bytes map {
            chunk =>
              Future(file.append(chunk).runRandomIO.right.value)
          }
        } await 20.seconds
      }
      file.close().runRandomIO.right.value
    }

    "write key values to a MMAPlFile" in {
      val path = randomFilePath

      val file =
        DBFile.mmapInit(
          path = path,
          ioStrategy = randomIOAccess(true),
          bufferSize = bytes.size * 50,
          blockCacheFileId = BlockCacheFileIDGenerator.nextID,
          autoClose = false,
          deleteOnClean = OperatingSystem.isWindows
        ).runRandomIO.right.value

      Benchmark("write 1 million key values to a MMAPlFile") {
        bytes foreach {
          chunk =>
            file.append(chunk).runRandomIO.right.value
        }
      }
      file.close().runRandomIO.right.value
    }

    "write key values to a MMAPlFile concurrently" in {
      val path = randomFilePath

      val file =
        DBFile.mmapInit(
          path = path,
          ioStrategy = randomIOAccess(true),
          bufferSize = bytes.size * 50,
          blockCacheFileId = BlockCacheFileIDGenerator.nextID,
          autoClose = false,
          deleteOnClean = OperatingSystem.isWindows
        ).runRandomIO.right.value

      Benchmark("write 1 million key values to a MMAPlFile concurrently") {
        Future.sequence {
          bytes map {
            chunk =>
              Future(file.append(chunk).runRandomIO.right.value)
          }
        } await 20.seconds
      }
      file.close().runRandomIO.right.value
    }
  }
}
