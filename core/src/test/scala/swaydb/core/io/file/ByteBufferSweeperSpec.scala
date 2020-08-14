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

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions.randomIOStrategy
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper}
import swaydb.core.actor.ByteBufferSweeper.{ByteBufferSweeperActor, Command}
import swaydb.core.util.{BlockCacheFileIDGenerator, Counter}
import swaydb.core.{TestBase, TestExecutionContext, TestSweeper}
import swaydb.data.config.ActorConfig
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.{Bag, IO}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class ByteBufferSweeperSpec extends TestBase {

  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache

  implicit val memorySweeper = TestSweeper.memorySweeper10
  implicit val futureBag = Bag.future
  implicit val terminateTimeout = 10.seconds

  "clear a MMAP file" in {
    implicit val fileSweeper = FileSweeper(0, ActorConfig.Basic("FileSweet test - clear a MMAP file", TestExecutionContext.executionContext)).actor
    implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()
    cleaner.actor.terminateAfter(10.seconds)

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

    fileSweeper.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
    cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
    cleaner.actor.messageCount shouldBe 0
    cleaner.actor.isTerminated shouldBe true
  }

  "it should not fatal terminate" when {
    "concurrently reading a deleted MMAP file" in {

      implicit val fileSweeper = FileSweeper(1, ActorConfig.Timer("FileSweeper Test Timer", 1.second, TestExecutionContext.executionContext)).actor
      implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()
      cleaner.actor.terminateAfter(10.seconds)

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

      fileSweeper.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
      fileSweeper.messageCount shouldBe 0

      cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
      cleaner.actor.messageCount shouldBe 0
      cleaner.actor.isTerminated shouldBe true
    }
  }

  "recordCleanRequest & recordCleanSuccessful" should {
    "create a record on empty and remove on all clean" in {
      implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()
      cleaner.actor

      val path = Paths.get("test")
      val map = mutable.HashMap.empty[Path, Counter.Request[ByteBufferSweeper.Command.Clean]]
      ByteBufferSweeper.recordCleanRequest(Command.Clean(null, path), map) shouldBe 1

      map should have size 1
      map.get(path).value.counter.get() shouldBe 1

      ByteBufferSweeper.recordCleanSuccessful(path, map)
      map shouldBe empty
      map.get(path) shouldBe empty

      cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
      cleaner.actor.messageCount shouldBe 0
      cleaner.actor.isTerminated shouldBe true
    }

    "increment record if non-empty and decrement on success" in {
      implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()
      cleaner.actor

      val path = Paths.get("test")
      val map = mutable.HashMap.empty[Path, Counter.Request[Command.Clean]]
      ByteBufferSweeper.recordCleanRequest(Command.Clean(null, path), map) shouldBe 1

      map should have size 1

      //submit clean request 100 times
      (1 to 100) foreach {
        i =>
          ByteBufferSweeper.recordCleanRequest(Command.Clean(null, path), map) shouldBe (i + 1)
          map.get(path).value.counter.get() shouldBe (i + 1)
      }

      //results in 101 requests
      map.get(path).value.counter.get() shouldBe 101

      (1 to 101).reverse foreach {
        i =>
          ByteBufferSweeper.recordCleanSuccessful(path, map) shouldBe (i - 1)
          //if no pending cleans left then map should be cleared.
          if (i == 1)
            map shouldBe empty
          else
            map.get(path).value.counter.get() shouldBe (i - 1)
      }

      cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
      cleaner.actor.messageCount shouldBe 0
      cleaner.actor.isTerminated shouldBe true
    }
  }

  "clean ByteBuffer" should {
    "initialise cleaner" in {
      implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()
      cleaner.actor

      val path = randomFilePath
      val file = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
      val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

      val cleanResult = ByteBufferSweeper.initCleanerAndPerformClean(ByteBufferSweeper.State(None, mutable.HashMap.empty), buffer, path)
      cleanResult shouldBe a[IO.Right[_, _]]
      cleanResult.value.cleaner shouldBe defined

      Effect.exists(path) shouldBe true
      Effect.delete(path)
      Effect.exists(path) shouldBe false

      cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
      cleaner.actor.messageCount shouldBe 0
      cleaner.actor.isTerminated shouldBe true
    }
  }

  /**
   * These tests are slow because [[ByteBufferSweeperActor]] is a timer actor.
   */

  "clean and delete" when {

    "deleteFile" when {
      "delete after clean" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()

        val filePath = randomFilePath
        val folderPath = filePath.getParent

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

        //clean first
        cleaner.actor send Command.Clean(buffer, filePath)
        //and then delete
        cleaner.actor send Command.DeleteFile(filePath)

        //file is eventually deleted but the folder is not deleted
        eventual(2.minutes) {
          Effect.exists(filePath) shouldBe false
          Effect.exists(folderPath) shouldBe true
        }

        //state should be cleared
        cleaner.actor.ask(Command.IsAllClean[Unit]).await(1.minute)

        cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
        cleaner.actor.messageCount shouldBe 0
        cleaner.actor.isTerminated shouldBe true
      }

      "delete before clean" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()

        val filePath = randomFilePath
        val folderPath = filePath.getParent

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

        //delete first this will result is delete reschedule on windows.
        cleaner.actor send Command.DeleteFile(filePath)
        cleaner.actor send Command.Clean(buffer, filePath)

        //file is eventually deleted but the folder is not deleted
        eventual(2.minutes) {
          Effect.exists(filePath) shouldBe false
          Effect.exists(folderPath) shouldBe true
        }

        //state should be cleared
        cleaner.actor.ask(Command.IsAllClean[Unit]).await(1.minute)

        cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
        cleaner.actor.messageCount shouldBe 0
        cleaner.actor.isTerminated shouldBe true
      }
    }

    "deleteFolder" when {
      "delete after clean" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()

        val filePath = randomFilePath
        val folderPath = filePath.getParent

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

        //clean first
        cleaner.actor send Command.Clean(buffer, filePath)
        //and then delete
        cleaner.actor send Command.DeleteFolder(folderPath, filePath)

        eventual(2.minutes) {
          Effect.exists(folderPath) shouldBe false
          Effect.exists(filePath) shouldBe false
        }

        //state should be cleared
        cleaner.actor.ask(Command.IsAllClean[Unit]).await(1.minute)

        cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
        cleaner.actor.messageCount shouldBe 0
        cleaner.actor.isTerminated shouldBe true
      }

      "delete before clean" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()

        val filePath = randomFilePath
        val folderPath = filePath.getParent

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

        //delete first this will result is delete reschedule on windows.
        cleaner.actor send Command.DeleteFolder(folderPath, filePath)
        cleaner.actor send Command.Clean(buffer, filePath)

        eventual(2.minutes) {
          Effect.exists(folderPath) shouldBe false
          Effect.exists(filePath) shouldBe false
        }

        //state should be cleared
        cleaner.actor.ask(Command.IsAllClean[Unit]).await(1.minute)

        cleaner.actor.terminateAndRecoverAsync(terminateTimeout).await(terminateTimeout)
        cleaner.actor.messageCount shouldBe 0
        cleaner.actor.isTerminated shouldBe true
      }
    }

    "IsClean" should {
      "return true if ByteBufferCleaner is empty" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper(0.seconds, 0, 0, 1.seconds)

        (cleaner.actor ask Command.IsClean(Paths.get("somePath"))).await(1.minute) shouldBe true

        cleaner.actor.terminate()
      }

      "return true if ByteBufferCleaner has cleaned the file" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper(actorInterval = 2.second, messageReschedule = 2.seconds)

        val filePath = randomFilePath

        val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
        val buffer = file.map(MapMode.READ_WRITE, 0, 1000)
        Effect.exists(filePath) shouldBe true

        //clean will get rescheduled first.
        cleaner.actor send Command.Clean(buffer, filePath)
        //since this is the second message and clean is rescheduled this will get processed.
        (cleaner.actor ask Command.IsClean(filePath)).await(10.seconds) shouldBe false

        //eventually clean is executed
        eventual(5.seconds) {
          (cleaner.actor ask Command.IsClean(filePath)).await(10.seconds) shouldBe true
        }

        cleaner.actor.isEmpty shouldBe true
        cleaner.actor.terminate()
      }

      "return true if ByteBufferCleaner has cleaned and delete the file" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()

        def sendRandomRequests(): Path = {
          val filePath = randomFilePath
          val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
          val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

          runThis(randomIntMax(10) max 1) {
            Seq(
              () => cleaner.actor send Command.Clean(buffer, filePath),
              () => cleaner.actor send Command.DeleteFolder(filePath, filePath)
            ).runThisRandomly
          }

          //also randomly terminate
          if (Random.nextDouble() < 0.0001)
            cleaner.actor.terminate()

          filePath
        }

        sendRandomRequests()

        val paths = (1 to 100) map (_ => sendRandomRequests())

        eventual(1.minute) {
          (cleaner.actor ask Command.IsAllClean[Unit]).await(20.seconds) shouldBe true
        }

        //execute all pending Delete commands.
        cleaner.actor.receiveAllBlocking(Int.MaxValue, 1.second).get

        //there might me some delete messages waiting to be scheduled.
        eventual(1.minute) {
          paths.forall(Effect.notExists) shouldBe true
        }

        cleaner.actor.terminate()
      }
    }

    "IsTerminatedAndCleaned" when {
      "ByteBufferCleaner is empty" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper(actorInterval = 2.second, messageReschedule = 2.seconds)

        cleaner.actor.terminate()
        cleaner.actor.isTerminated shouldBe true

        //its terminates and there are no clean commands so this returns true.
        (cleaner.actor ask Command.IsTerminatedAndCleaned[Unit]).await(2.seconds) shouldBe true
      }

      "return true if ByteBufferCleaner has cleaned and delete the file" in {
        implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper()

        def sendRandomRequests(): Path = {
          val filePath = randomFilePath
          val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
          val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

          //randomly submit clean and delete in any order and random number of times.
          runThis(randomIntMax(10) max 1) {
            Seq(
              () => cleaner.actor send Command.Clean(buffer, filePath),
              () => cleaner.actor send Command.DeleteFolder(filePath, filePath)
            ).runThisRandomly
          }

          filePath
        }

        sendRandomRequests()

        val paths = (1 to 100) map (_ => sendRandomRequests())

        //execute all pending Delete commands.
        cleaner.actor.terminateAndRecoverAsync[Future](1.second).await(1.minute)

        eventual(1.minute) {
          (cleaner.actor ask Command.IsTerminatedAndCleaned[Unit]).await(2.seconds) shouldBe true
        }

        //there might me some delete messages waiting to be scheduled.
        eventual(1.minute) {
          paths.forall(Effect.notExists) shouldBe true
        }
      }
    }
  }
}
