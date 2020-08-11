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

import swaydb.{Bag, IO}
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
import swaydb.data.util.OperatingSystem
import swaydb.test.TestActor

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

class BufferCleanerSpec extends TestBase {

  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache

  implicit val memorySweeper = TestSweeper.memorySweeper10
  implicit val futureBag = Bag.future
  implicit val terminateTimeout = 10.seconds

  "clear a MMAP file" in {
    implicit val fileSweeper = FileSweeper(0, ActorConfig.Basic("FileSweet test - clear a MMAP file", TestExecutionContext.executionContext))
    implicit val cleaner: BufferCleaner = BufferCleaner()

    val file: DBFile =
      DBFile.mmapWriteAndRead(
        path = randomDir,
        ioStrategy = randomIOStrategy(cacheOnAccess = true),
        autoClose = true,
        deleteOnClean = OperatingSystem.isWindows,
        blockCacheFileId = BlockCacheFileIDGenerator.nextID,
        bytes = Slice(randomBytesSlice())
      )

    val innerFile = file.file.asInstanceOf[MMAPFile]

    eventual(2.seconds) {
      innerFile.isBufferEmpty shouldBe true
    }

    fileSweeper.terminateAndRecover(terminateTimeout).await(terminateTimeout)
    cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
    cleaner.messageCount shouldBe 0
    cleaner.isTerminated shouldBe true
  }

  "it should not fatal terminate" when {
    "concurrently reading a deleted MMAP file" in {

      implicit val fileSweeper = FileSweeper(1, ActorConfig.Timer("FileSweeper Test Timer", 1.second, TestExecutionContext.executionContext))
      implicit val cleaner: BufferCleaner = BufferCleaner()

      val files =
        (1 to 20) map {
          _ =>
            val file =
              IO {
                DBFile.mmapWriteAndRead(
                  path = randomDir,
                  ioStrategy = randomIOStrategy(cacheOnAccess = true),
                  autoClose = true,
                  deleteOnClean = OperatingSystem.isWindows,
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

      fileSweeper.terminateAndRecover(terminateTimeout).await(terminateTimeout)
      fileSweeper.messageCount() shouldBe 0

      cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
      cleaner.messageCount shouldBe 0
      cleaner.isTerminated shouldBe true
    }
  }

  "recordCleanRequest & recordCleanSuccessful" should {
    "create a record on empty and remove on all clean" in {
      implicit val cleaner: BufferCleaner = BufferCleaner()

      val path = Paths.get("test")
      val map = mutable.HashMap.empty[Path, Counter.IntCounter]
      BufferCleaner.recordCleanRequest(path, map) shouldBe 1

      map should have size 1
      map.get(path).value.get() shouldBe 1

      BufferCleaner.recordCleanSuccessful(path, map)
      map shouldBe empty
      map.get(path) shouldBe empty

      cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
      cleaner.messageCount shouldBe 0
      cleaner.isTerminated shouldBe true
    }

    "increment record if non-empty and decrement on success" in {
      implicit val cleaner: BufferCleaner = BufferCleaner()

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

      cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
      cleaner.messageCount shouldBe 0
      cleaner.isTerminated shouldBe true
    }
  }

  "clean ByteBuffer" should {
    "initialise cleaner" in {
      implicit val cleaner: BufferCleaner = BufferCleaner()

      val path = randomFilePath
      val file = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
      val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

      val cleanResult = BufferCleaner.clean(BufferCleaner.State(None, mutable.HashMap.empty), buffer, path)
      cleanResult shouldBe a[IO.Right[_, _]]
      cleanResult.value.cleaner shouldBe defined

      Effect.exists(path) shouldBe true
      Effect.delete(path)
      Effect.exists(path) shouldBe false

      cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
      cleaner.messageCount shouldBe 0
      cleaner.isTerminated shouldBe true
    }
  }

  /**
   * These tests are slow because [[BufferCleaner]] is a timer actor and is processing messages
   * at [[BufferCleaner.actorInterval]] intervals.
   */

  "clean and delete" when {
    /**
     * Asserts the [[BufferCleaner.State]] contains a counter entry for the file path.
     */
    def assertStateContainsFileRequestCounter(filePath: Path)(implicit bufferCleaner: BufferCleaner): Unit = {
      val testActor = TestActor[BufferCleaner.State]()
      BufferCleaner.getState(testActor)
      val pending = testActor.getMessage(1.minute).pendingClean
      pending should have size 1
      pending.get(filePath).value.get() shouldBe 1
    }

    /**
     * Asserts the state is empty/cleared.
     */
    def assertStateIsCleared()(implicit bufferCleaner: BufferCleaner): Unit = {
      //state should be cleared
      val testActor = TestActor[BufferCleaner.State]()
      BufferCleaner.getState(testActor)
      testActor.getMessage(1.minute).pendingClean shouldBe empty
    }

    "deleteFile" when {
      "delete after clean" in {
        implicit val cleaner: BufferCleaner = BufferCleaner()

        val filePath = randomFilePath
        val folderPath = filePath.getParent

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

        //clean first
        BufferCleaner.clean(buffer, filePath)
        //and then delete
        BufferCleaner.deleteFile(filePath)

        //assert that the state of Actor increment the could of clean requests
        assertStateContainsFileRequestCounter(filePath)

        //file is eventually deleted but the folder is not deleted
        eventual(2.minutes) {
          Effect.exists(filePath) shouldBe false
          Effect.exists(folderPath) shouldBe true
        }

        //state should be cleared
        assertStateIsCleared()

        cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
        cleaner.messageCount shouldBe 0
        cleaner.isTerminated shouldBe true
      }

      "delete before clean" in {
        implicit val cleaner: BufferCleaner = BufferCleaner()

        val filePath = randomFilePath
        val folderPath = filePath.getParent

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

        //delete first this will result is delete reschedule on windows.
        BufferCleaner.deleteFile(filePath)
        BufferCleaner.clean(buffer, filePath)

        //assert that the state of Actor increment the could of clean requests
        assertStateContainsFileRequestCounter(filePath)

        //file is eventually deleted but the folder is not deleted
        eventual(2.minutes) {
          Effect.exists(filePath) shouldBe false
          Effect.exists(folderPath) shouldBe true
        }

        //state should be cleared
        assertStateIsCleared()

        cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
        cleaner.messageCount shouldBe 0
        cleaner.isTerminated shouldBe true
      }
    }

    "deleteFolder" when {
      "delete after clean" in {
        implicit val cleaner: BufferCleaner = BufferCleaner()

        val filePath = randomFilePath
        val folderPath = filePath.getParent

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

        //clean first
        BufferCleaner.clean(buffer, filePath)
        //and then delete
        BufferCleaner.deleteFolder(folderPath, filePath)

        //assert that the state of Actor increment the could of clean requests
        //state is created for filePath and not for folderPath
        assertStateContainsFileRequestCounter(filePath)

        eventual(2.minutes) {
          Effect.exists(folderPath) shouldBe false
          Effect.exists(filePath) shouldBe false
        }

        //state should be cleared
        assertStateIsCleared()

        cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
        cleaner.messageCount shouldBe 0
        cleaner.isTerminated shouldBe true
      }

      "delete before clean" in {
        implicit val cleaner: BufferCleaner = BufferCleaner()

        val filePath = randomFilePath
        val folderPath = filePath.getParent

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

        //delete first this will result is delete reschedule on windows.
        BufferCleaner.deleteFolder(folderPath, filePath)
        BufferCleaner.clean(buffer, filePath)

        //assert that the state of Actor increment the could of clean requests
        //state is created for filePath and not for folderPath
        assertStateContainsFileRequestCounter(filePath)

        eventual(2.minutes) {
          Effect.exists(folderPath) shouldBe false
          Effect.exists(filePath) shouldBe false
        }

        //state should be cleared
        assertStateIsCleared()

        cleaner.terminateAndRecover(terminateTimeout).await(terminateTimeout)
        cleaner.messageCount shouldBe 0
        cleaner.isTerminated shouldBe true
      }
    }
  }
}
