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
import java.nio.channels.FileChannel.MapMode
import java.nio.file.{NoSuchFileException, StandardOpenOption}

import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions.randomIOStrategy
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.FileSweeper
import swaydb.core.util.BlockCacheFileIDGenerator
import swaydb.core.{TestBase, TestExecutionContext, TestSweeper}
import swaydb.data.config.ActorConfig
import swaydb.data.slice.Slice

import scala.concurrent.Future
import scala.concurrent.duration._

class BufferCleanerSpec extends TestBase {

  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache
  implicit val memorySweeper = TestSweeper.memorySweeper10

  override def beforeEach(): Unit = {
    BufferCleaner.initialiseCleaner
    super.beforeEach()
  }

  "clear a MMAP file" in {
    implicit val limiter = FileSweeper(0, ActorConfig.Basic(TestExecutionContext.executionContext))
    val file: DBFile =
      DBFile.mmapWriteAndRead(
        path = randomDir,
        ioStrategy = randomIOStrategy(cacheOnAccess = true),
        autoClose = true,
        blockCacheFileId = BlockCacheFileIDGenerator.nextID,
        bytes = Slice(randomBytesSlice())
      )

    val innerFile = file.file.asInstanceOf[MMAPFile]

    eventual(2.seconds) {
      innerFile.isBufferEmpty shouldBe true
    }

    limiter.terminate()
  }

  "it should not fatal terminate" when {
    "concurrently reading a deleted MMAP file" in {

      implicit val limiter = FileSweeper(1, ActorConfig.Timer(1.second, TestExecutionContext.executionContext))

      val files =
        (1 to 20) map {
          _ =>
            val file =
              IO {
                DBFile.mmapWriteAndRead(
                  path = randomDir,
                  ioStrategy = randomIOStrategy(cacheOnAccess = true),
                  autoClose = true,
                  blockCacheFileId = BlockCacheFileIDGenerator.nextID,
                  bytes = Slice(randomBytesSlice())
                )
              }.get

            IO(file.delete()).get
            file
        }

      //deleting a memory mapped file (that performs unsafe Buffer cleanup)
      //and repeatedly reading from it should not cause fatal shutdown.
      files foreach {
        file =>
          Future {
            while (true)
              IO(file.get(0)).left.value shouldBe a[NoSuchFileException]
          }
      }

      //keep this test running for a few seconds.
      sleep(20.seconds)

      limiter.terminate()
    }
  }

  "clear ByteBuffer" in {
    val path = randomFilePath
    val file = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
    val buffer = file.map(MapMode.READ_WRITE, 0, 1000)
    val result = BufferCleaner.clean(BufferCleaner.State(None), buffer, path)
    result shouldBe a[IO.Right[_, _]]
    result.get.cleaner shouldBe defined
  }
}
