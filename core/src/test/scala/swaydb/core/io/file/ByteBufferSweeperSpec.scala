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
import java.nio.file.{Path, Paths, StandardOpenOption}
import java.util.concurrent.atomic.AtomicBoolean

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions.randomThreadSafeIOStrategy
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.actor.ByteBufferSweeper.{ByteBufferSweeperActor, Command}
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper}
import swaydb.core.util.BlockCacheFileIDGenerator
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave}
import swaydb.data.RunThis._
import swaydb.data.config.ActorConfig
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.{Bag, IO}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class ByteBufferSweeperSpec extends TestBase {

  implicit val ec = TestExecutionContext.executionContext
  implicit val futureBag = Bag.future

  "clear a MMAP file" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          implicit val fileSweeper: FileSweeperActor =
            FileSweeper(1, ActorConfig.Basic("FileSweet test - clear a MMAP file", TestExecutionContext.executionContext)).sweep().actor

          val file: DBFile =
            DBFile.mmapWriteAndRead(
              path = randomDir,
              fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
              autoClose = true,
              deleteAfterClean = OperatingSystem.isWindows,
              forceSave = TestForceSave.mmap(),
              blockCacheFileId = BlockCacheFileIDGenerator.nextID,
              bytes = Slice(randomBytesSlice())
            )

          val innerFile = file.file.asInstanceOf[MMAPFile]

          fileSweeper.terminateAndRecover[Bag.Less]()

          eventual(2.seconds) {
            innerFile.isBufferEmpty shouldBe true
          }
      }
    }
  }

  "it should not fatal terminate" when {
    "concurrently reading a closed MMAP file" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          implicit val ec = TestExecutionContext.executionContext

          implicit val fileSweeper = FileSweeper(1, ActorConfig.Timer("FileSweeper Test Timer", 0.second, TestExecutionContext.executionContext)).sweep().actor
          implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper(messageReschedule = 0.millisecond).sweep()
          val bytes = randomBytesSlice()

          val files =
            (1 to 10) map {
              _ =>
                DBFile.mmapWriteAndRead(
                  path = randomDir,
                  fileOpenIOStrategy = randomThreadSafeIOStrategy(cacheOnAccess = true),
                  autoClose = true,
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap(),
                  blockCacheFileId = BlockCacheFileIDGenerator.nextID,
                  bytes = bytes
                )
            }

          val timeout = 20.seconds.fromNow

          val readingFutures =
            files map {
              file =>
                Future {
                  while (timeout.hasTimeLeft())
                    file.get(0).runRandomIO.value shouldBe bytes.head
                }
            }

          //close memory mapped files (that performs unsafe Buffer cleanup)
          //and repeatedly reading from it should not cause fatal shutdown.

          val closing =
            Future {
              files map {
                file =>
                  while (timeout.hasTimeLeft())
                    file.close()
              }
            }

          //keep this test running for a few seconds.
          sleep(timeout)

          fileSweeper.terminateAndRecover().await(10.seconds)
          fileSweeper.messageCount shouldBe 0
          closing.await(1.second)
          Future.sequence(readingFutures).await(1.second)
      }
    }
  }

  "recordCleanRequest & recordCleanSuccessful" should {
    "create a record on empty and remove on all clean" in {
      val path = Paths.get("test")
      val map = mutable.HashMap.empty[Path, mutable.HashMap[Long, ByteBufferSweeper.Command.Clean]]

      val command = Command.Clean(null, () => false, path, TestForceSave.mmap())

      ByteBufferSweeper.recordCleanRequest(command, map)
      map should have size 1

      val storedCommand = map.get(path).value
      storedCommand should have size 1
      storedCommand.head._1 shouldBe command.id
      storedCommand.head._2 shouldBe command

      ByteBufferSweeper.recordCleanSuccessful(command, map)
      map shouldBe empty
      map.get(path) shouldBe empty
    }

    "increment record if non-empty and decrement on success" in {
      TestCaseSweeper {
        implicit sweeper =>

          val path = Paths.get("test")
          val map = mutable.HashMap.empty[Path, mutable.HashMap[Long, Command.Clean]]

          val command = Command.Clean(null, () => false, path, TestForceSave.mmap())

          ByteBufferSweeper.recordCleanRequest(command, map)
          map should have size 1
          ByteBufferSweeper.recordCleanSuccessful(command, map)
          map should have size 0

          //submit clean request 100 times
          val commands =
            (1 to 100) map {
              i =>
                val command = Command.Clean(null, () => false, path, TestForceSave.mmap())
                ByteBufferSweeper.recordCleanRequest(command, map)
                map.get(path).value.size shouldBe i
                command
            }

          //results in 100 requests
          val expectedSize = 100
          map.get(path).value.size shouldBe expectedSize

          commands.foldLeft(expectedSize) {
            case (expectedSize, command) =>
              map.get(path).value.size shouldBe expectedSize
              ByteBufferSweeper.recordCleanSuccessful(command, map)
              if (expectedSize == 1)
                map.get(path) shouldBe empty
              else
                map.get(path).value.size shouldBe (expectedSize - 1)

              expectedSize - 1
          }

          //at the end when all records are cleaned the map is set to empty.
          map shouldBe empty
      }
    }
  }

  "clean ByteBuffer" should {
    "initialise cleaner" in {
      TestCaseSweeper {
        implicit sweeper =>

          val path = randomFilePath
          val file = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
          val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

          val command = Command.Clean(buffer, () => false, path, TestForceSave.mmap())

          val cleanResult = ByteBufferSweeper.initCleanerAndPerformClean(ByteBufferSweeper.State.init, buffer, command)
          cleanResult shouldBe a[IO.Right[_, _]]
          cleanResult.value.cleaner shouldBe defined

          Effect.exists(path) shouldBe true
          Effect.delete(path)
          Effect.exists(path) shouldBe false
      }
    }
  }

  /**
   * These tests are slow because [[ByteBufferSweeperActor]] is a timer actor.
   */

  "clean and delete" when {

    "deleteFile" when {
      "delete after clean" in {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val filePath = randomFilePath
            val folderPath = filePath.getParent

            val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
            val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

            //clean first
            cleaner.actor send Command.Clean(buffer, () => false, filePath, TestForceSave.mmap())
            //and then delete
            cleaner.actor send Command.DeleteFile(filePath)

            //file is eventually deleted but the folder is not deleted
            eventual(2.minutes) {
              Effect.exists(filePath) shouldBe false
              Effect.exists(folderPath) shouldBe true
            }

            //state should be cleared
            cleaner.actor.ask(Command.IsAllClean[Unit]).await(1.minute)
        }
      }

      "delete before clean" in {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val filePath = randomFilePath
            val folderPath = filePath.getParent

            val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
            val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

            //delete first this will result is delete reschedule on windows.
            cleaner.actor send Command.DeleteFile(filePath)
            cleaner.actor send Command.Clean(buffer, () => false, filePath, TestForceSave.mmap())

            //file is eventually deleted but the folder is not deleted
            eventual(2.minutes) {
              Effect.exists(filePath) shouldBe false
              Effect.exists(folderPath) shouldBe true
            }

            //state should be cleared
            cleaner.actor.ask(Command.IsAllClean[Unit]).await(1.minute)
        }
      }
    }

    "deleteFolder" when {
      "delete after clean" in {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val filePath = randomFilePath
            val folderPath = filePath.getParent

            val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
            val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

            //clean first
            cleaner.actor send Command.Clean(buffer, () => false, filePath, TestForceSave.mmap())
            //and then delete
            cleaner.actor send Command.DeleteFolder(folderPath, filePath)

            eventual(2.minutes) {
              Effect.exists(folderPath) shouldBe false
              Effect.exists(filePath) shouldBe false
            }

            //state should be cleared
            cleaner.actor.ask(Command.IsAllClean[Unit]).await(1.minute)
        }
      }

      "delete before clean" in {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val filePath = randomFilePath
            val folderPath = filePath.getParent

            val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
            val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

            //delete first this will result is delete reschedule on windows.
            cleaner.actor send Command.DeleteFolder(folderPath, filePath)
            cleaner.actor send Command.Clean(buffer, () => false, filePath, TestForceSave.mmap())

            eventual(2.minutes) {
              Effect.exists(folderPath) shouldBe false
              Effect.exists(filePath) shouldBe false
            }

            //state should be cleared
            cleaner.actor.ask(Command.IsAllClean[Unit]).await(1.minute)
        }
      }
    }

    "IsClean" should {
      "return true if ByteBufferCleaner is empty" in {
        TestCaseSweeper {
          implicit sweeper =>

            implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper(0, 1.seconds).sweep()

            (cleaner.actor ask Command.IsClean(Paths.get("somePath"))).await(1.minute) shouldBe true

            cleaner.actor.terminate[Bag.Less]()
        }
      }

      "return true if ByteBufferCleaner has cleaned the file" in {
        TestCaseSweeper {
          implicit sweeper =>

            implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper(messageReschedule = 2.seconds).sweep()

            val filePath = randomFilePath

            val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
            val buffer = file.map(MapMode.READ_WRITE, 0, 1000)
            Effect.exists(filePath) shouldBe true

            val hasReference = new AtomicBoolean(true)
            //clean will get rescheduled first.
            cleaner.actor send Command.Clean(buffer, hasReference.get, filePath, TestForceSave.mmap())
            //since this is the second message and clean is rescheduled this will get processed.
            (cleaner.actor ask Command.IsClean(filePath)).await(10.seconds) shouldBe false

            hasReference.set(false)
            //eventually clean is executed
            eventual(5.seconds) {
              (cleaner.actor ask Command.IsClean(filePath)).await(10.seconds) shouldBe true
            }

            cleaner.actor.isEmpty shouldBe true
        }
      }

      "return true if ByteBufferCleaner has cleaned and delete the file" in {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            def sendRandomRequests(fileNumber: Int): Path = {
              val filePath = testClassDir.resolve(s"$fileNumber.test").sweep()
              val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
              val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

              runThis(randomIntMax(10) max 1) {
                Seq(
                  () => cleaner.actor send Command.Clean(buffer, () => false, filePath, TestForceSave.mmap()),
                  () => cleaner.actor send Command.DeleteFolder(filePath, filePath)
                ).runThisRandomly
              }

              //also randomly terminate
              if (Random.nextDouble() < 0.0001)
                cleaner.actor.terminate()

              filePath
            }

            sendRandomRequests(0)

            val paths = (1 to 100) map (i => sendRandomRequests(i))

            eventual(2.minutes) {
              //ByteBufferCleaner is a timer Actor with 5.seconds interval so await enough
              //seconds for the Actor to process stashed request/command.
              (cleaner.actor ask Command.IsAllClean[Unit]).await(30.seconds) shouldBe true
            }

            //execute all pending Delete commands.
            cleaner.actor.receiveAllForce[Bag.Less]()

            //there might me some delete messages waiting to be scheduled.
            eventual(1.minute) {
              paths.forall(Effect.notExists) shouldBe true
            }
        }
      }
    }

    "IsTerminatedAndCleaned" when {
      "ByteBufferCleaner is empty" in {
        TestCaseSweeper {
          implicit sweeper =>

            implicit val cleaner: ByteBufferSweeperActor = ByteBufferSweeper(messageReschedule = 2.seconds).sweep()

            cleaner.actor.terminate()
            cleaner.actor.isTerminated shouldBe true

            //its terminates and there are no clean commands so this returns true.
            (cleaner.actor ask Command.IsTerminated[Unit]).await(2.seconds) shouldBe true
        }
      }

      "return true if ByteBufferCleaner has cleaned and delete the file" in {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            def sendRandomRequests(): Path = {
              val filePath = randomFilePath
              val file = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
              val buffer = file.map(MapMode.READ_WRITE, 0, 1000)

              //randomly submit clean and delete in any order and random number of times.
              runThis(randomIntMax(100) max 1) {
                Seq(
                  () => runThis(randomIntMax(10) max 1)(cleaner.actor send Command.Clean(buffer, () => false, filePath, TestForceSave.mmap())),
                  () => runThis(randomIntMax(10) max 1)(cleaner.actor send Command.DeleteFolder(filePath, filePath))
                ).runThisRandomly
              }

              filePath
            }

            sendRandomRequests()

            val paths = (1 to 100) map (_ => sendRandomRequests())

            //execute all pending Delete commands.
            eventual(1.minute) {
              cleaner.actor.terminateAndRecover[Future]().await(1.minute)
            }

            eventual(1.minute) {
              (cleaner.actor ask Command.IsTerminated[Unit]).await(2.seconds) shouldBe true
            }

            //there might be some delete messages waiting to be scheduled.
            eventual(1.minute) {
              paths.forall(Effect.notExists) shouldBe true
            }
        }
      }
    }
  }
}
