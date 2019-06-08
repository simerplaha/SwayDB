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
import scala.concurrent.Future
import scala.concurrent.duration._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.queue.FileLimiter
import swaydb.data.IO

class BufferCleanerSpec extends TestBase {

  override def beforeEach(): Unit = {
    BufferCleaner.initialiseCleaner
    super.beforeEach()
  }

  "clear a MMAP file" in {
    implicit val limiter: FileLimiter = FileLimiter(0, 1.second)
    val file: DBFile = DBFile.mmapWriteAndRead(randomBytesSlice(), randomDir, autoClose = true).get

    eventual(10.seconds) {
      file.file.get.asInstanceOf[MMAPFile].isBufferEmpty shouldBe true
    }

    sleep(2.second)

    limiter.terminate()
  }

  "it should not fatal terminate" when {
    "concurrently reading a deleted MMAP file" in {

      implicit val limiter: FileLimiter = FileLimiter(0, 1.seconds)

      val files =
        (1 to 20) map {
          _ =>
            val file = DBFile.mmapWriteAndRead(randomBytesSlice(), randomDir, autoClose = true).get
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
    result shouldBe a[IO.Success[_]]
    result.get.cleaner shouldBe defined
  }
}
