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
import swaydb.core.CommonAssertions.randomIOStrategy
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.queue.FileLimiter
import swaydb.data.slice.Slice

import scala.concurrent.Future
import scala.concurrent.duration._

class BufferCleanerSpec extends TestBase {

  override def beforeEach(): Unit = {
    BufferCleaner.initialiseCleaner
    super.beforeEach()
  }

  "clear a MMAP file" in {
    implicit val limiter: FileLimiter = FileLimiter(0, 1.second)
    val file: DBFile = DBFile.mmapWriteAndRead(randomDir, randomIOStrategy(cacheOnAccess = true), autoClose = true, Slice(randomBytesSlice())).get

    eventual(10.seconds) {
      file.file match {
        case IO.Success(file: MMAPFile) =>
          file.isBufferEmpty shouldBe true

        case IO.Success(file) =>
          fail(s"Didn't expect file type: ${file.getClass.getSimpleName}")

        case IO.Failure(_) =>
        //success it was null and removed.
      }
    }

    limiter.terminate()
  }

  "it should not fatal terminate" when {
    "concurrently reading a deleted MMAP file" in {

      implicit val limiter: FileLimiter = FileLimiter(0, 1.seconds)

      val files =
        (1 to 20) map {
          _ =>
            val file = DBFile.mmapWriteAndRead(randomDir, randomIOStrategy(cacheOnAccess = true), autoClose = true, Slice(randomBytesSlice())).get
            file.delete().get
            file
        }

      //deleting a memory mapped file (that performs unsafe Buffer cleanup)
      //and repeatedly reading from it should not cause fatal shutdown.
      files foreach {
        file =>
          Future {
            while (true)
              file.get(0).failed.get.exception shouldBe a[NoSuchFileException]
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
    result shouldBe a[IO.Success[_, _]]
    result.get.cleaner shouldBe defined
  }
}
