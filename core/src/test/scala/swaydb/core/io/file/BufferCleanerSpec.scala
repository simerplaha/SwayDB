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

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.{NoSuchFileException, Path, Paths, StandardOpenOption}

import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions.randomIOStrategy
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.FileSweeper
import swaydb.core.util.{BlockCacheFileIDGenerator, Counter}
import swaydb.core.{TestBase, TestExecutionContext, TestSweeper}
import swaydb.data.config.ActorConfig
import swaydb.data.slice.Slice
import org.scalatest.OptionValues._

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

class BufferCleanerSpec extends TestBase {

  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache

  implicit val memorySweeper = TestSweeper.memorySweeper10

  "clear a MMAP file" in {
    implicit val limiter = FileSweeper(0, ActorConfig.Basic("FileSweet test - clear a MMAP file", TestExecutionContext.executionContext))

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

      implicit val limiter = FileSweeper(1, ActorConfig.Timer("FileSweeper Test Timer", 1.second, TestExecutionContext.executionContext))

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

      val timeout = 20.seconds.fromNow

      //deleting a memory mapped file (that performs unsafe Buffer cleanup)
      //and repeatedly reading from it should not cause fatal shutdown.
      files foreach {
        file =>
          Future {
            while (timeout.hasTimeLeft())
              IO(file.get(0)).left.value shouldBe a[NoSuchFileException]
          }
      }

      //keep this test running for a few seconds.
      sleep(timeout)

      limiter.terminate()
      limiter.messageCount() shouldBe 0
    }
  }

  "recordCleanRequest & recordCleanSuccessful" should {
    "create a record on empty and remove on all clean" in {

      val path = Paths.get("test")
      val map = mutable.HashMap.empty[Path, Counter.IntCounter]
      BufferCleaner.recordCleanRequest(path, map) shouldBe 1

      map should have size 1
      map.get(path).value.get() shouldBe 1

      BufferCleaner.recordCleanSuccessful(path, map)
      map shouldBe empty
      map.get(path) shouldBe empty
    }

    "increment record if non-empty and decrement on success" in {

      val path = Paths.get("test")
      val map = mutable.HashMap.empty[Path, Counter.IntCounter]
      BufferCleaner.recordCleanRequest(path, map) shouldBe 1

      map should have size 1

      //submit clean request 100 times
      (1 to 100) foreach {
        i =>
          BufferCleaner.recordCleanRequest(path, map) shouldBe (i + 1)
          map.get(path).value.get() shouldBe (i + 1)
      }

      //results in 101 requests
      map.get(path).value.get() shouldBe 101

      (1 to 101).reverse foreach {
        i =>
          BufferCleaner.recordCleanSuccessful(path, map) shouldBe (i - 1)
          //if no pending cleans left then map should be cleared.
          if (i == 1)
            map shouldBe empty
          else
            map.get(path).value.get() shouldBe (i - 1)
      }
    }
  }

  "clean ByteBuffer" should {
    "initialise cleaner" in {
      val path = randomFilePath
      val file = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
      val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

      val cleanResult = BufferCleaner.clean(BufferCleaner.State(None, mutable.HashMap.empty), buffer, path)
      cleanResult shouldBe a[IO.Right[_, _]]
      cleanResult.value.cleaner shouldBe defined

      Effect.exists(path) shouldBe true
      Effect.delete(path)
      Effect.exists(path) shouldBe false
    }
  }

  "clean and delete" when {
    "delete after clean" in {
      val path = randomFilePath
      val file = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
      val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

      //clean first
      BufferCleaner.clean(buffer, path)
      //and then delete
      BufferCleaner.delete(path)

      eventual(2.minutes) {
        Effect.exists(path) shouldBe false
      }
    }

    "delete before clean" in {
      val path = randomFilePath
      val file = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
      val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

      //delete first this will result is delete reschedule on windows.
      BufferCleaner.delete(path)

      sleep(2.seconds)
      BufferCleaner.clean(buffer, path)

      eventual(2.minutes) {
        Effect.exists(path) shouldBe false
      }
    }
  }
}
