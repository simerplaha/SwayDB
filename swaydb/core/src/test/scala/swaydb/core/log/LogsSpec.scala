/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.log

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.config.accelerate.Accelerator
import swaydb.config.{Atomic, MMAP, OptimiseWrites, RecoveryMode}
import swaydb.core.CommonAssertions._
import swaydb.core.CorePrivateMethodTester._
import swaydb.core.TestSweeper._
import swaydb.core.CoreTestData._
import swaydb.core.level.zero.LevelZeroLogCache
import swaydb.core.segment.data.{Memory, MemoryOption, Value}
import swaydb.core.{ACoreSpec, TestSweeper, TestForceSave, TestTimer}
import swaydb.effect.Effect._
import swaydb.effect.Effect
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.utils.{Extension, OperatingSystem}
import swaydb.utils.StorageUnits._

import java.nio.file.NoSuchFileException
import swaydb.testkit.TestKit._
import swaydb.core.file.CoreFileTestKit._

class LogsSpec extends ALogSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit def optimiseWrites: OptimiseWrites = OptimiseWrites.random
  implicit def atomic = Atomic.random

  import swaydb.core.log.serialiser.LevelZeroLogEntryReader._
  import swaydb.core.log.serialiser.LevelZeroLogEntryWriter._

  "Logs.persistent" should {
    "initialise and recover on reopen" in {
      runThis(10.times, log = true) {
        TestSweeper {
          implicit sweeper =>
            import sweeper._

            val path = createRandomDir()
            val logs: Logs[Slice[Byte], Memory, LevelZeroLogCache] =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.mb,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            logs.write(_ => LogEntry.Put(1, Memory.put(1)))
            logs.write(_ => LogEntry.Put(2, Memory.put(2)))
            logs.write(_ => LogEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1)))

            logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.remove(1)
            logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2)
            //since the size of the Log is 1.mb and entries are too small. No flushing will value executed and there should only be one folder.
            path.folders.map(_.folderId) should contain only 0

            if (logs.mmap.hasMMAP && OperatingSystem.isWindows()) {
              logs.close().value
              sweeper.receiveAll()
            }

            //reopen and it should contain the same entries as above and old log should value delete
            val reopen =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.mb,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            //adding more entries to reopened Log should contain all entries
            reopen.write(_ => LogEntry.Put(3, Memory.put(3)))
            reopen.write(_ => LogEntry.Put(4, Memory.put(4)))
            reopen.find[MemoryOption](Memory.Null, _.cache.skipList.get(3)) shouldBe Memory.put(3)
            reopen.find[MemoryOption](Memory.Null, _.cache.skipList.get(4)) shouldBe Memory.put(4)
            //old entries still exist in the reopened log
            reopen.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.remove(1)
            reopen.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2)

            //reopening will start the Log in recovery mode which will add all the existing logs in memory and initialise a new log for writing
            //so a new folder 1 is initialised.
            path.folders.map(_.folderId) shouldBe List(0, 1)
        }
      }
    }

    "delete empty logs on recovery" in {
      runThis(10.times, log = true) {
        TestSweeper {
          implicit sweeper =>
            import sweeper._
            val path = createRandomDir()
            val logs =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.mb,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            if (logs.mmap.hasMMAP && OperatingSystem.isWindows()) {
              logs.close().value
              sweeper.receiveAll()
            }

            logs.logsCount shouldBe 1
            val currentLogsPath = logs.log.asInstanceOf[PersistentLog[Slice[Byte], Memory, LevelZeroLogCache]].path
            //above creates a log without any entries

            //reopen should create a new log, delete the previous logs current log as it's empty
            val reopen =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.mb,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            reopen.logsCount shouldBe 1
            //since the old log is empty, it should value deleted

            if (reopen.mmap.hasMMAP && OperatingSystem.isWindows())
              sweeper.receiveAll()

            currentLogsPath.exists shouldBe false
        }
      }
    }
  }

  "Logs.memory" should {
    "initialise" in {
      TestSweeper {
        implicit sweeper =>
          import sweeper._
          val log: Logs[Slice[Byte], Memory, LevelZeroLogCache] =
            Logs.memory[Slice[Byte], Memory, LevelZeroLogCache](
              fileSize = 1.mb,
              acceleration = Accelerator.brake()
            )

          log.write(_ => LogEntry.Put(1, Memory.put(1)))
          log.write(_ => LogEntry.Put(2, Memory.put(2)))
          log.write(_ => LogEntry.Put[Slice[Byte], Memory](1, Memory.remove(1)))

          log.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.remove(1)
          log.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2)
      }
    }
  }

  "Logs" should {
    "initialise a new log if the current log is full" in {
      TestSweeper {
        implicit sweeper =>
          import sweeper._
          def test(logs: Logs[Slice[Byte], Memory, LevelZeroLogCache]) = {
            logs.write(_ => LogEntry.Put(1, Memory.put(1))) //entry size is 21.bytes
            logs.write(_ => LogEntry.Put(2: Slice[Byte], Memory.Range(2, 2, Value.FromValue.Null, Value.update(2)))) //another 31.bytes
            logs.logsCount shouldBe 1
            //another 32.bytes but log has total size of 82.bytes.
            //now since the Log is overflow a new should value created.
            logs.write(_ => LogEntry.Put[Slice[Byte], Memory](3, Memory.remove(3)))
            logs.logsCount shouldBe 2
          }

          //persistent
          val path = createRandomDir()
          val logs =
            Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              path = path,
              mmap = MMAP.randomForLog(),
              fileSize = 21.bytes + 31.bytes,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.ReportFailure
            ).value.sweep()

          test(logs)
          //new log 1 gets created since the 3rd entry is overflow entry.
          path.folders.map(_.folderId) should contain only(0, 1)

          //in memory
          test(
            Logs.memory(
              fileSize = 21.bytes + 31.bytes,
              acceleration = Accelerator.brake()
            )
          )
      }
    }

    "write a key value larger then the actual fileSize" in {
      runThis(10.times, log = true) {
        TestSweeper {
          implicit sweeper =>
            import sweeper._
            val largeValue = randomBytesSlice(1.mb)

            def test(logs: Logs[Slice[Byte], Memory, LevelZeroLogCache]): Logs[Slice[Byte], Memory, LevelZeroLogCache] = {
              //adding 1.mb key-value to log, the file size is 500.bytes, since this is the first entry in the log and the log is empty,
              // the entry will value added.
              logs.write(_ => LogEntry.Put(1, Memory.put(1, largeValue))) //large entry
              logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.put(1, largeValue)
              logs.logsCount shouldBe 1

              //now the log is overflown. Adding any other entry will create a new log
              logs.write(_ => LogEntry.Put(2, Memory.put(2, 2)))
              logs.logsCount shouldBe 2

              //a small entry of 24.bytes gets written to the same Log since the total size is 500.bytes
              logs.write(_ => LogEntry.Put[Slice[Byte], Memory.Remove](3, Memory.remove(3)))
              logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(3)) shouldBe Memory.remove(3)
              logs.logsCount shouldBe 2

              //write large entry again and a new Log is created again.
              logs.write(_ => LogEntry.Put(1, Memory.put(1, largeValue))) //large entry
              logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.put(1, largeValue)
              logs.logsCount shouldBe 3

              //now again the log is overflown. Adding any other entry will create a new log
              logs.write(_ => LogEntry.Put[Slice[Byte], Memory](4, Memory.remove(4)))
              logs.logsCount shouldBe 4
              logs
            }

            val path = createRandomDir()
            //persistent

            val originalLogs =
              test(
                Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                  path = path,
                  mmap = MMAP.randomForLog(),
                  fileSize = 500.bytes,
                  acceleration = Accelerator.brake(),
                  recovery = RecoveryMode.ReportFailure
                ).value
              )

            if (originalLogs.mmap.hasMMAP && OperatingSystem.isWindows())
              originalLogs.ensureClose()

            //in memory
            test(
              Logs.memory(
                fileSize = 500.bytes,
                acceleration = Accelerator.brake()
              )
            )

            //reopen start in recovery mode and existing logs are cached
            val logs =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 500.bytes,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            logs.logsCount shouldBe 5
            logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.put(1, largeValue)
            logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2, 2)
            logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(3)) shouldBe Memory.remove(3)
            logs.find[MemoryOption](Memory.Null, _.cache.skipList.get(4)) shouldBe Memory.remove(4)
        }
      }
    }

    "recover logs in newest to oldest order" in {
      runThis(10.times, log = true) {
        TestSweeper {
          implicit sweeper =>
            import sweeper._

            val path = createRandomDir()
            val logs =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            logs.write(_ => LogEntry.Put(1, Memory.put(1)))
            logs.write(_ => LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)))
            logs.write(_ => LogEntry.Put(3, Memory.put(3)))
            logs.write(_ => LogEntry.Put[Slice[Byte], Memory.Remove](4, Memory.remove(2)))
            logs.write(_ => LogEntry.Put(5, Memory.put(5)))

            logs.logsCount shouldBe 5
            //logs value added
            getLogs(logs).iterator.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(4, 3, 2, 1, 0)
            logs.last().value.pathOption.value.folderId shouldBe 0

            if (logs.mmap.hasMMAP && OperatingSystem.isWindows()) {
              logs.close().value
              sweeper.receiveAll()
            }

            val recovered1 =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            getLogs(recovered1).iterator.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(5, 4, 3, 2, 1, 0)
            recovered1.log.pathOption.value.folderId shouldBe 5
            recovered1.write(_ => LogEntry.Put[Slice[Byte], Memory.Remove](6, Memory.remove(6)))
            recovered1.last().value.pathOption.value.folderId shouldBe 0

            if (recovered1.mmap.hasMMAP && OperatingSystem.isWindows()) {
              recovered1.close().value
              sweeper.receiveAll()
            }

            val recovered2 =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            getLogs(recovered2).iterator.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(6, 5, 4, 3, 2, 1, 0)
            recovered2.log.pathOption.value.folderId shouldBe 6
            recovered2.last().value.pathOption.value.folderId shouldBe 0
        }
      }
    }

    "recover from existing logs" in {
      runThis(10.times, log = true) {
        TestSweeper {
          implicit sweeper =>
            import sweeper._

            val path = createRandomDir()
            val logs =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            logs.write(_ => LogEntry.Put(1, Memory.put(1)))
            logs.write(_ => LogEntry.Put(2, Memory.put(2)))
            logs.write(_ => LogEntry.Put[Slice[Byte], Memory](1, Memory.remove(1)))

            if (logs.mmap.hasMMAP && OperatingSystem.isWindows()) {
              logs.close().value
              sweeper.receiveAll()
            }

            val recoveredLogs =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            val recoveredLogsLogs = getLogs(recoveredLogs).iterator.toList
            recoveredLogsLogs should have size 4
            recoveredLogsLogs.map(_.pathOption.value.folderId) shouldBe List(3, 2, 1, 0)

            recoveredLogsLogs.head.cache.skipList.size shouldBe 0
            recoveredLogsLogs.tail.head.cache.skipList.get(1) shouldBe Memory.remove(1)
            recoveredLogsLogs.tail.drop(1).head.cache.skipList.get(2) shouldBe Memory.put(2)
            recoveredLogsLogs.last.cache.skipList.get(1) shouldBe Memory.put(1)
        }
      }
    }

    "fail recovery if one of the log is corrupted and recovery mode is ReportFailure" in {
      runThis(10.times, log = true) {
        TestSweeper {
          implicit sweeper =>
            import sweeper._
            val path = createRandomDir()
            val logs =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            logs.write(_ => LogEntry.Put(1, Memory.put(1)))
            logs.write(_ => LogEntry.Put(2, Memory.put(2)))
            logs.write(_ => LogEntry.Put[Slice[Byte], Memory](3, Memory.remove(3)))

            if (logs.mmap.hasMMAP && OperatingSystem.isWindows()) {
              logs.close().value
              sweeper.receiveAll()
            }

            val secondLogsPath = getLogs(logs).iterator.toList.tail.head.pathOption.value.files(Extension.Log).head
            val secondLogsBytes = Effect.readAllBytes(secondLogsPath)
            Effect.overwrite(secondLogsPath, secondLogsBytes.dropRight(1))

            Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              path = path,
              mmap = MMAP.randomForLog(),
              fileSize = 1.byte,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.ReportFailure
            ).left.value.exception shouldBe a[IllegalStateException]
        }
      }
    }

    "continue recovery if one of the log is corrupted and recovery mode is DropCorruptedTailEntries" in {
      runThis(10.times, log = true) {
        TestSweeper {
          implicit sweeper =>
            import sweeper._
            val path = createRandomDir()
            val logs =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.Off(TestForceSave.standard()),
                fileSize = 50.bytes,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            logs.write(_ => LogEntry.Put(1, Memory.put(1)))
            logs.write(_ => LogEntry.Put(2, Memory.put(2)))
            logs.write(_ => LogEntry.Put(3, Memory.put(3, 3)))
            logs.write(_ => LogEntry.Put(4, Memory.put(4)))
            logs.write(_ => LogEntry.Put(5, Memory.put(5)))
            logs.write(_ => LogEntry.Put(6, Memory.put(6, 6)))

            val secondLogsPath = getLogs(logs).iterator.toList.tail.head.pathOption.value.files(Extension.Log).head
            val secondLogsBytes = Effect.readAllBytes(secondLogsPath)
            Effect.overwrite(secondLogsPath, secondLogsBytes.dropRight(1))

            val recovered =
              Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                path = path,
                mmap = MMAP.randomForLog(),
                fileSize = 50.bytes,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.DropCorruptedTailEntries
              ).value.sweep()

            val recoveredLogs = getLogs(recovered).iterator.toList

            //recovered logs will still be 4 (including a new log) but since second logs second entry is corrupted, the first entry will still exists.
            recoveredLogs should have size 4

            recoveredLogs.head.cache.skipList.size shouldBe 0

            val recoveredLogsWithoutNew = recoveredLogs.tail

            //newest log contains all key-values
            recoveredLogsWithoutNew.head.cache.skipList.get(5) shouldBe Memory.put(5)
            recoveredLogsWithoutNew.head.cache.skipList.get(6) shouldBe Memory.put(6, 6)

            //second log is the corrupted log and will have the 2nd entry missing
            recoveredLogsWithoutNew.tail.head.cache.skipList.get(3) shouldBe Memory.put(3, 3)
            recoveredLogsWithoutNew.tail.head.cache.skipList.get(4).toOptionS shouldBe empty //4th entry is corrupted, it will not exist the Log

            //oldest log contains all key-values
            recoveredLogsWithoutNew.last.cache.skipList.get(1) shouldBe Memory.put(1)
            recoveredLogsWithoutNew.last.cache.skipList.get(2) shouldBe Memory.put(2)
        }
      }
    }

    "continue recovery if one of the log is corrupted and recovery mode is DropCorruptedTailEntriesAndLogs" in {
      TestSweeper {
        implicit sweeper =>
          import sweeper._
          val path = createRandomDir()
          val logs =
            Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              path = path,
              mmap = MMAP.Off(TestForceSave.standard()),
              fileSize = 50.bytes,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.ReportFailure
            ).value

          logs.write(_ => LogEntry.Put(1, Memory.put(1)))
          logs.write(_ => LogEntry.Put(2, Memory.put(2, 2)))
          logs.write(_ => LogEntry.Put(3, Memory.put(3)))
          logs.write(_ => LogEntry.Put(4, Memory.put(4)))
          logs.write(_ => LogEntry.Put(5, Memory.put(5)))
          logs.write(_ => LogEntry.Put(6, Memory.put(6)))

          val secondLogsPath = getLogs(logs).iterator.toList.tail.head.pathOption.value.files(Extension.Log).head
          val secondLogsBytes = Effect.readAllBytes(secondLogsPath)
          Effect.overwrite(secondLogsPath, secondLogsBytes.dropRight(1))

          val recoveredLogs =
            Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              path = path,
              mmap = MMAP.randomForLog(),
              fileSize = 50.bytes,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.DropCorruptedTailEntriesAndLogs
            ).value.sweep()

          getLogs(recoveredLogs) should have size 3
          //the last log is delete since the second last Log is found corrupted.
          getLogs(logs).iterator.toList.last.exists() shouldBe false

          //oldest log contains all key-values
          recoveredLogs.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.put(1)
          recoveredLogs.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2, 2)
          //second log is partially read but it's missing 4th entry
          recoveredLogs.find[MemoryOption](Memory.Null, _.cache.skipList.get(3)) shouldBe Memory.put(3)
          //third log is completely ignored.
          recoveredLogs.find[MemoryOption](Memory.Null, _.cache.skipList.get(4)).toOption shouldBe empty
          recoveredLogs.find[MemoryOption](Memory.Null, _.cache.skipList.get(5)).toOption shouldBe empty
          recoveredLogs.find[MemoryOption](Memory.Null, _.cache.skipList.get(6)).toOption shouldBe empty
      }
    }

    "start a new Log if writing an entry fails" in {
      TestSweeper {
        implicit sweeper =>
          import sweeper._
          val path = createRandomDir()
          val logs =
            Logs.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              path = path,
              mmap = MMAP.Off(TestForceSave.standard()),
              fileSize = 100.bytes,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.ReportFailure
            ).value

          logs.write(_ => LogEntry.Put(1, Memory.put(1)))
          logs.logsCount shouldBe 1
          //delete the log
          logs.log.delete()

          //failure because the file is deleted. The Log will NOT try to re-write this entry again because
          //it should be an indication that something is wrong with the file system permissions.
          assertThrows[NoSuchFileException] {
            logs.write(_ => LogEntry.Put(2, Memory.put(2)))
          }

          //new Log file is created. Now this write will succeed.
          logs.write(_ => LogEntry.Put(2, Memory.put(2)))
      }
    }
  }
}
