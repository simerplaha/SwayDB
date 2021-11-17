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
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.data.{Memory, MemoryOption, Value}
import swaydb.core.file.DBFile
import swaydb.core.level.AppendixLogCache
import swaydb.core.level.zero.LevelZeroLogCache
import swaydb.core.log.MapTestUtil._
import swaydb.core.log.serialiser._
import swaydb.core.segment.Segment
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.skiplist.SkipListConcurrent
import swaydb.config.MMAP
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.{Slice, SliceOption}
import swaydb.config.{Atomic, OptimiseWrites}
import swaydb.effect.Effect._
import swaydb.effect.{Effect, Extension}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import java.nio.file.{FileAlreadyExistsException, Path}

class MapSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO = SegmentReadIO.random
  implicit def optimiseWrites: OptimiseWrites = OptimiseWrites.random
  implicit def atomic = Atomic.random

  "Map" should {
    "initialise a memory level0" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryWriter._

          val log =
            Log.memory[Slice[Byte], Memory, LevelZeroLogCache](
              fileSize = 1.mb,
              flushOnOverflow = false
            ).sweep()

          log.writeSync(LogEntry.Put(1, Memory.put(1, 1))) shouldBe true
          log.writeSync(LogEntry.Put(2, Memory.put(2, 2))) shouldBe true
          log.cache.skipList.get(1) shouldBe Memory.put(1, 1)
          log.cache.skipList.get(2) shouldBe Memory.put(2, 2)

          log.cache.hasRange shouldBe false

          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2))) shouldBe true
          log.cache.skipList.get(1) shouldBe Memory.remove(1)
          log.cache.skipList.get(2) shouldBe Memory.remove(2)

          log.cache.hasRange shouldBe false

          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](1, Memory.Range(1, 10, Value.FromValue.Null, Value.remove(None)))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](11, Memory.Range(11, 20, Value.put(20), Value.update(20)))) shouldBe true

          log.cache.skipList.get(1) shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.remove(None))
          log.cache.skipList.get(11) shouldBe Memory.Range(11, 20, Value.put(20), Value.update(20))

          log.cache.hasRange shouldBe true
      }
    }

    "initialise a memory Appendix log" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixLogEntryWriter._

          val log =
            Log.memory[Slice[Byte], Segment, AppendixLogCache](
              fileSize = 1.mb,
              flushOnOverflow = false
            ).sweep()

          val segment1 = TestSegment()
          val segment2 = TestSegment()

          log.writeSync(LogEntry.Put[Slice[Byte], Segment](1, segment1)) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Segment](2, segment2)) shouldBe true
          log.cache.get(1) shouldBe segment1
          log.cache.get(2) shouldBe segment2

          log.writeSync(LogEntry.Remove[Slice[Byte]](1)) shouldBe true
          log.writeSync(LogEntry.Remove[Slice[Byte]](2)) shouldBe true
          log.cache.get(1).toOptionS shouldBe empty
          log.cache.get(2).toOptionS shouldBe empty
      }
    }

    "initialise a persistent Level0 log and recover from it when it's empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          val log =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          log.cache.skipList.isEmpty shouldBe true
          log.close()
          //recover from an empty log
          val recovered =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = log.path,
              mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          recovered.cache.isEmpty shouldBe true
          recovered.close()
      }
    }

    "initialise a persistent Appendix log and recover from it when it's empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixLogEntryWriter._
          import sweeper._

          val appendixReader =
            AppendixLogEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              segmentRefCacheLife = randomSegmentRefCacheLife()
            )

          import appendixReader._

          val log =
            Log.persistent[Slice[Byte], Segment, AppendixLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false, fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          log.cache.isEmpty shouldBe true
          log.close()
          //recover from an empty log
          val recovered =
            Log.persistent[Slice[Byte], Segment, AppendixLogCache](
              folder = log.path,
              mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          recovered.cache.isEmpty shouldBe true
          recovered.close()
      }
    }

    "initialise a persistent Level0 log and recover from it when it's contains data" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          def assertReads(log: PersistentLog[Slice[Byte], Memory, LevelZeroLogCache]) = {
            log.cache.skipList.get(1) shouldBe Memory.put(1, 1)
            log.cache.skipList.get(2) shouldBe Memory.remove(2)
            log.cache.skipList.get(10) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
            log.cache.skipList.get(15) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
            log.cache.hasRange shouldBe true
          }

          def doRecover(path: Path): PersistentLog[Slice[Byte], Memory, LevelZeroLogCache] = {
            val recovered =
              Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                folder = path,
                mmap = MMAP.randomForLog(),
                flushOnOverflow = false,
                fileSize = 1.mb,
                dropCorruptedTailEntries = false
              ).item.sweep()

            assertReads(recovered)

            if (recovered.mmap.isMMAP && OperatingSystem.isWindows) {
              recovered.close()
              sweeper.receiveAll()
            }

            recovered
          }

          val log =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, 1))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, 2))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))) shouldBe true

          assertReads(log)

          //recover maps 10 times
          (1 to 10).foldLeft(log) {
            case (log, _) =>
              doRecover(log.path)
          }
      }
    }

    "initialise a persistent Appendix log and recover from it when it's contains data" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader = AppendixLogEntryReader(
            mmapSegment =
              MMAP.On(
                deleteAfterClean = OperatingSystem.isWindows,
                forceSave = TestForceSave.mmap()
              ),
            segmentRefCacheLife = randomSegmentRefCacheLife()
          )
          import AppendixLogEntryWriter._
          import appendixReader._

          val segment1 = TestSegment(Slice(Memory.put(1, 1, None), Memory.put(2, 2, None)))
          val segment2 = TestSegment(Slice(Memory.put(3, 3, None), Memory.put(4, 4, None)))

          val log =
            Log.persistent[Slice[Byte], Segment, AppendixLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          log.writeSync(LogEntry.Put[Slice[Byte], Segment](1, segment1)) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Segment](2, segment2)) shouldBe true
          log.writeSync(LogEntry.Remove[Slice[Byte]](2)) shouldBe true
          log.cache.get(1) shouldBe segment1
          log.cache.get(2).toOptionS shouldBe empty

          def doRecover(path: Path): PersistentLog[Slice[Byte], Segment, AppendixLogCache] = {
            val recovered =
              Log.persistent[Slice[Byte], Segment, AppendixLogCache](
                folder = log.path,
                mmap = MMAP.randomForLog(),
                flushOnOverflow = false,
                fileSize = 1.mb,
                dropCorruptedTailEntries = false
              ).item.sweep()

            recovered.cache.get(1).getS shouldBe segment1
            recovered.cache.get(2).toOptionS shouldBe empty
            recovered.close()

            if (recovered.mmap.isMMAP && OperatingSystem.isWindows)
              sweeper.receiveAll()

            recovered
          }

          //recover and maps 10 times
          (1 to 10).foldLeft(log) {
            case (log, _) =>
              doRecover(log.path)
          }
      }
    }

    "initialise a Map that has two persistent Level0 log files (second file did not value deleted due to early JVM termination)" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          val map1 =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1.writeSync(LogEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map1.writeSync(LogEntry.Put(2, Memory.put(2, 2))) shouldBe true
          map1.writeSync(LogEntry.Put(3, Memory.put(3, 3))) shouldBe true
          map1.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)))) shouldBe true

          val map2 =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map2.writeSync(LogEntry.Put(4, Memory.put(4, 4))) shouldBe true
          map2.writeSync(LogEntry.Put(5, Memory.put(5, 5))) shouldBe true
          map2.writeSync(LogEntry.Put(2, Memory.put(2, 22))) shouldBe true //second file will override 2's value to be 22
          map2.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))) shouldBe true

          //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
          val map2sLogFile = map2.path.resolve(0.toLogFileId)
          Effect.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

          //recover log 1 and it should contain all entries of map1 and map2
          val map1Recovered =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = map1.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1Recovered.cache.skipList.get(1) shouldBe Memory.put(1, 1)
          map1Recovered.cache.skipList.get(2) shouldBe Memory.put(2, 22) //second file overrides 2's value to be 22
          map1Recovered.cache.skipList.get(3) shouldBe Memory.put(3, 3)
          map1Recovered.cache.skipList.get(4) shouldBe Memory.put(4, 4)
          map1Recovered.cache.skipList.get(5) shouldBe Memory.put(5, 5)
          map1Recovered.cache.skipList.get(6).toOptionS shouldBe empty
          map1Recovered.cache.skipList.get(10) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          map1Recovered.cache.skipList.get(15) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))

          //recovered file's id is 2.log
          map1Recovered.path.files(Extension.Log).map(_.fileId) should contain only ((2, Extension.Log))
      }
    }

    "initialise a Map that has two persistent Appendix log files (second file did not value deleted due to early JVM termination)" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader =
            AppendixLogEntryReader(
              mmapSegment =
                MMAP.On(
                  deleteAfterClean = OperatingSystem.isWindows,
                  forceSave = TestForceSave.mmap()
                ),
              segmentRefCacheLife = randomSegmentRefCacheLife()
            )

          import AppendixLogEntryWriter._
          import appendixReader._

          val segment1 = TestSegment(Slice(Memory.put(1, 1, None), Memory.put(2, 2, None)))
          val segment2 = TestSegment(Slice(Memory.put(2, 2, None), Memory.put(4, 4, None)))
          val segment3 = TestSegment(Slice(Memory.put(3, 3, None), Memory.put(6, 6, None)))
          val segment4 = TestSegment(Slice(Memory.put(4, 4, None), Memory.put(8, 8, None)))
          val segment5 = TestSegment(Slice(Memory.put(5, 5, None), Memory.put(10, 10, None)))
          val segment2Updated = TestSegment(Slice(Memory.put(2, 2, None), Memory.put(12, 12, None)))

          val map1 =
            Log.persistent[Slice[Byte], Segment, AppendixLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1.writeSync(LogEntry.Put(1, segment1)) shouldBe true
          map1.writeSync(LogEntry.Put(2, segment2)) shouldBe true
          map1.writeSync(LogEntry.Put(3, segment3)) shouldBe true

          val map2 =
            Log.persistent[Slice[Byte], Segment, AppendixLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map2.writeSync(LogEntry.Put(4, segment4)) shouldBe true
          map2.writeSync(LogEntry.Put(5, segment5)) shouldBe true
          map2.writeSync(LogEntry.Put(2, segment2Updated)) shouldBe true //second file will override 2's value to be segment2Updated

          //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
          val map2sLogFile = map2.path.resolve(0.toLogFileId)
          Effect.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

          //recover log 1 and it should contain all entries of map1 and map2
          val map1Recovered =
            Log.persistent[Slice[Byte], Segment, AppendixLogCache](
              folder = map1.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1Recovered.cache.get(1).getS shouldBe segment1
          map1Recovered.cache.get(2).getS shouldBe segment2Updated //second file overrides 2's value to be segment2Updated
          map1Recovered.cache.get(3).getS shouldBe segment3
          map1Recovered.cache.get(4).getS shouldBe segment4
          map1Recovered.cache.get(5).getS shouldBe segment5
          map1Recovered.cache.get(6).toOptionS shouldBe empty

          //recovered file's id is 2.log
          map1Recovered.path.files(Extension.Log).map(_.fileId) should contain only ((2, Extension.Log))
      }
    }

    "fail initialise if the Map exists but recovery is not provided" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          val log =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb
            ).sweep()

          //fails because the file already exists.
          assertThrows[FileAlreadyExistsException] {
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = log.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb
            ).sweep()
          }

          //recovers because the recovery is provided
          Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
            folder = log.path,
            mmap = MMAP.Off(TestForceSave.channel()),
            flushOnOverflow = false,
            fileSize = 1.mb,
            dropCorruptedTailEntries = false
          ).item.sweep()

      }
    }
  }

  "PersistentMap.recover" should {
    "recover from an empty PersistentMap folder" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          val cache = LevelZeroLogCache.builder.create()

          val file =
            PersistentLog.recover[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              fileSize = 4.mb,
              cache = cache,
              dropCorruptedTailEntries = false
            ).item

          file.isOpen shouldBe true
          file.isMemoryMapped shouldBe false
          file.existsOnDisk shouldBe true
          file.fileSize shouldBe 0
          file.path.fileId shouldBe(0, Extension.Log)

          cache.isEmpty shouldBe true
      }
    }

    "recover from an existing PersistentMap folder" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          //create a log
          val log =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          log.writeSync(LogEntry.Put(1, Memory.put(1, 1))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2))) shouldBe true
          log.writeSync(LogEntry.Put(3, Memory.put(3, 3))) shouldBe true
          log.writeSync(LogEntry.Put(3, Memory.put(3, 3))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))) shouldBe true

          log.currentFilePath.fileId shouldBe(0, Extension.Log)

          log.cache.hasRange shouldBe true

          val cache = LevelZeroLogCache.builder.create()

          //PersistentMap.recover below will delete this mmap file which on Windows
          //requires this log to be closed and cleaned before deleting.
          if (OperatingSystem.isWindows) {
            log.close()
            sweeper.receiveAll()
          }

          val recoveredFile =
            PersistentLog.recover[Slice[Byte], Memory, LevelZeroLogCache](
              folder = log.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              fileSize = 4.mb,
              cache = cache,
              dropCorruptedTailEntries = false
            ).item.sweep()

          recoveredFile.isOpen shouldBe true
          recoveredFile.isMemoryMapped shouldBe false
          recoveredFile.existsOnDisk shouldBe true
          recoveredFile.path.fileId shouldBe(1, Extension.Log) //file id gets incremented on recover

          recoveredFile.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

          cache.isEmpty shouldBe false
          cache.skipList.get(1: Slice[Byte]) shouldBe Memory.put(1, 1)
          cache.skipList.get(2: Slice[Byte]) shouldBe Memory.remove(2)
          cache.skipList.get(3: Slice[Byte]) shouldBe Memory.put(3, 3)
          cache.skipList.get(10: Slice[Byte]) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          cache.skipList.get(15: Slice[Byte]) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
      }
    }

    "recover from an existing PersistentMap folder when flushOnOverflow is true" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          //create a log
          val log =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = true,
              fileSize = 1.byte,
              dropCorruptedTailEntries = false
            ).item.sweep()

          log.writeSync(LogEntry.Put(1, Memory.put(1, 1))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2))) shouldBe true
          log.writeSync(LogEntry.Put(3, Memory.put(3, 3))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)))) shouldBe true
          log.writeSync(LogEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))) shouldBe true

          log.currentFilePath.fileId shouldBe(5, Extension.Log)
          log.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted
          log.path.resolveSibling(1.toLogFileId).exists shouldBe false //1.log gets deleted
          log.path.resolveSibling(2.toLogFileId).exists shouldBe false //2.log gets deleted
          log.path.resolveSibling(3.toLogFileId).exists shouldBe false //3.log gets deleted
          log.path.resolveSibling(4.toLogFileId).exists shouldBe false //4.log gets deleted

          if (OperatingSystem.isWindows) {
            log.close()
            sweeper.receiveAll()
          }

          //reopen file
          val cache = LevelZeroLogCache.builder.create()

          val recoveredFile =
            PersistentLog.recover[Slice[Byte], Memory, LevelZeroLogCache](
              folder = log.path,
              mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
              fileSize = 1.byte,
              cache = cache,
              dropCorruptedTailEntries = false
            ).item.sweep()

          recoveredFile.isOpen shouldBe true
          recoveredFile.isMemoryMapped shouldBe true
          recoveredFile.existsOnDisk shouldBe true
          recoveredFile.path.fileId shouldBe(6, Extension.Log) //file id gets incremented on recover
          recoveredFile.path.resolveSibling(5.toLogFileId).exists shouldBe false //5.log gets deleted

          cache.isEmpty shouldBe false
          cache.skipList.get(1: Slice[Byte]) shouldBe Memory.put(1, 1)
          cache.skipList.get(2: Slice[Byte]) shouldBe Memory.remove(2)
          cache.skipList.get(3: Slice[Byte]) shouldBe Memory.put(3, 3)
          cache.skipList.get(10: Slice[Byte]) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          cache.skipList.get(15: Slice[Byte]) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))

          if (OperatingSystem.isWindows) {
            recoveredFile.close()
            sweeper.receiveAll()
          }

          //reopen the recovered file
          val cache2 = LevelZeroLogCache.builder.create()
          val recoveredFile2 =
            PersistentLog.recover[Slice[Byte], Memory, LevelZeroLogCache](
              folder = log.path,
              mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
              fileSize = 1.byte,
              cache = cache2,
              dropCorruptedTailEntries = false
            ).item.sweep()

          recoveredFile2.isOpen shouldBe true
          recoveredFile2.isMemoryMapped shouldBe true
          recoveredFile2.existsOnDisk shouldBe true
          recoveredFile2.path.fileId shouldBe(7, Extension.Log) //file id gets incremented on recover
          recoveredFile2.path.resolveSibling(6.toLogFileId).exists shouldBe false //6.log gets deleted

          cache2.isEmpty shouldBe false
          cache2.skipList.get(1: Slice[Byte]) shouldBe Memory.put(1, 1)
          cache2.skipList.get(2: Slice[Byte]) shouldBe Memory.remove(2)
          cache2.skipList.get(3: Slice[Byte]) shouldBe Memory.put(3, 3)
          cache2.skipList.get(10: Slice[Byte]) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          cache2.skipList.get(15: Slice[Byte]) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
      }
    }

    "recover from an existing PersistentMap folder with empty memory log" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          //create a log
          val log =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = false
            ).item

          log.currentFilePath.fileId shouldBe(0, Extension.Log)
          log.close()

          if (OperatingSystem.isWindows)
            sweeper.receiveAll()

          val cache = LevelZeroLogCache.builder.create()
          val file =
            PersistentLog.recover[Slice[Byte], Memory, LevelZeroLogCache](
              folder = log.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              fileSize = 4.mb,
              cache = cache,
              dropCorruptedTailEntries = false
            ).item.sweep()

          file.isOpen shouldBe true
          file.isMemoryMapped shouldBe false
          file.existsOnDisk shouldBe true
          file.path.fileId shouldBe(1, Extension.Log) //file id gets incremented on recover
          file.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

          cache.isEmpty shouldBe true
      }
    }
  }

  "PersistentMap.nextFile" should {
    "creates a new file from the current file" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          val cache = LevelZeroLogCache.builder.create()
          cache.writeAtomic(LogEntry.Put(1, Memory.put(1, 1)))
          cache.writeAtomic(LogEntry.Put(2, Memory.put(2, 2)))
          cache.writeAtomic(LogEntry.Put(3, Memory.remove(3)))
          cache.writeAtomic(LogEntry.Put(3, Memory.remove(3)))
          cache.writeAtomic(LogEntry.Put(10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))))
          cache.writeAtomic(LogEntry.Put(15, Memory.Range(15, 20, Value.put(15), Value.update(14))))
          cache.writeAtomic(LogEntry.Put(15, Memory.Range(15, 20, Value.put(15), Value.update(14))))

          val currentFile =
            PersistentLog.recover[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              fileSize = 4.mb,
              cache = cache,
              dropCorruptedTailEntries = false
            ).item.sweep()

          val nextFile =
            PersistentLog.nextFile[Slice[Byte], Memory, LevelZeroLogCache](
              currentFile = currentFile,
              mmap = MMAP.Off(TestForceSave.channel()),
              size = 4.mb,
              cache = cache
            ).sweep()

          val nextFileSkipList = SkipListConcurrent[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
          val nextFileBytes = DBFile.standardRead(nextFile.path, randomThreadSafeIOStrategy(), autoClose = false).readAll
          nextFileBytes.size should be > 0
          val logEntries = LogEntrySerialiser.read(nextFileBytes, dropCorruptedTailEntries = false).value.item.value
          logEntries applyBatch nextFileSkipList

          nextFileSkipList.get(1: Slice[Byte]) shouldBe Memory.put(1, 1)
          nextFileSkipList.get(2: Slice[Byte]) shouldBe Memory.put(2, 2)
          nextFileSkipList.get(3: Slice[Byte]) shouldBe Memory.remove(3)
          nextFileSkipList.get(10: Slice[Byte]) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          nextFileSkipList.get(15: Slice[Byte]) shouldBe Memory.Range(15, 20, Value.put(15), Value.update(14))
      }
    }
  }

  "PersistentMap.recovery on corruption" should {
    import LevelZeroLogEntryReader._
    import LevelZeroLogEntryWriter._

    "fail if the WAL file is corrupted and and when dropCorruptedTailEntries = false" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val log =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          (1 to 100) foreach {
            i =>
              log.writeSync(LogEntry.Put(i, Memory.put(i, i))) shouldBe true
          }

          log.cache.skipList.size shouldBe 100
          val allBytes = Effect.readAllBytes(log.currentFilePath)

          def assertRecover =
            assertThrows[IllegalStateException] {
              Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                folder = log.currentFilePath.getParent,
                mmap = MMAP.Off(TestForceSave.channel()),
                flushOnOverflow = false,
                fileSize = 4.mb,
                dropCorruptedTailEntries = false
              )
            }

          //drop last byte
          Effect.overwrite(log.currentFilePath, allBytes.dropRight(1))
          assertRecover

          //drop first byte
          Effect.overwrite(log.currentFilePath, allBytes.drop(1))
          assertRecover
      }
    }

    "successfully recover partial data if WAL file is corrupted and when dropCorruptedTailEntries = true" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val log =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          (1 to 100) foreach {
            i =>
              log.writeSync(LogEntry.Put(i, Memory.put(i, i))) shouldBe true
          }
          log.cache.skipList.size shouldBe 100
          val allBytes = Effect.readAllBytes(log.currentFilePath)

          //recover again with SkipLogOnCorruption, since the last entry is corrupted, the first two entries will still value read
          Effect.overwrite(log.currentFilePath, allBytes.dropRight(1))

          val recoveredMap =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = log.currentFilePath.getParent,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = true
            ).item.sweep()

          (1 to 99) foreach {
            i =>
              recoveredMap.cache.skipList.get(i) shouldBe Memory.put(i, i)
          }
          recoveredMap.cache.skipList.contains(100) shouldBe false

          //if the top entry is corrupted.
          Effect.overwrite(recoveredMap.currentFilePath, allBytes.drop(1))

          val recoveredMap2 =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = recoveredMap.currentFilePath.getParent,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = true
            ).item.sweep()

          recoveredMap2.cache.skipList.isEmpty shouldBe true
      }
    }
  }

  "PersistentMap.recovery on corruption" when {
    "there are two WAL files and the first file is corrupted" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          val map1 =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1.writeSync(LogEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map1.writeSync(LogEntry.Put(2, Memory.put(2, 2))) shouldBe true
          map1.writeSync(LogEntry.Put(3, Memory.put(3, 3))) shouldBe true

          val map2 =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map2.writeSync(LogEntry.Put(4, Memory.put(4, 4))) shouldBe true
          map2.writeSync(LogEntry.Put(5, Memory.put(5, 5))) shouldBe true
          map2.writeSync(LogEntry.Put(6, Memory.put(6, 6))) shouldBe true

          val map2sLogFile = map2.path.resolve(0.toLogFileId)
          val copiedLogFileId = map1.path.resolve(1.toLogFileId)
          //move map2's log file into map1's log file folder named as 1.log.
          Effect.copy(map2sLogFile, copiedLogFileId)

          val log0 = map1.path.resolve(0.toLogFileId)
          val log0Bytes = Effect.readAllBytes(log0)

          val log1 = map1.path.resolve(1.toLogFileId)
          val log1Bytes = Effect.readAllBytes(log1)

          //fail recovery if first log is corrupted
          //corrupt 0.log bytes
          Effect.overwrite(log0, log0Bytes.drop(1))
          assertThrows[IllegalStateException] {
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = map1.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            )
          }
          Effect.overwrite(log0, log0Bytes) //fix log0 bytes

          //successfully recover Map by reading both WAL files if the first WAL file is corrupted
          //corrupt 0.log bytes
          Effect.overwrite(log0, log0Bytes.dropRight(1))
          val recoveredMapWith0LogCorrupted =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = map1.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = true
            )

          recoveredMapWith0LogCorrupted.item.sweep()

          //recovery state contains failure because the WAL file is partially recovered.
          recoveredMapWith0LogCorrupted.result.left.value.exception shouldBe a[IllegalStateException]
          //count instead of size because skipList's actual size can be higher.
          recoveredMapWith0LogCorrupted.item.cache.skipList.toIterable.count(_ => true) shouldBe 5 //5 because the 3rd entry in 0.log is corrupted

          //checking the recovered entries
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(1) shouldBe Memory.put(1, 1)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(2) shouldBe Memory.put(2, 2)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(3).toOptionS shouldBe empty //since the last byte of 0.log file is corrupted, the last entry is missing
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(4) shouldBe Memory.put(4, 4)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(5) shouldBe Memory.put(5, 5)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(6) shouldBe Memory.put(6, 6)
      }
    }
  }

  "PersistentMap.recovery on corruption" when {
    "there are two WAL files and the second file is corrupted" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          val map1 =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1.writeSync(LogEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map1.writeSync(LogEntry.Put(2, Memory.put(2))) shouldBe true
          map1.writeSync(LogEntry.Put(3, Memory.put(3, 3))) shouldBe true

          val map2 =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map2.writeSync(LogEntry.Put(4, Memory.put(4, 4))) shouldBe true
          map2.writeSync(LogEntry.Put(5, Memory.put(5, 5))) shouldBe true
          map2.writeSync(LogEntry.Put(6, Memory.put(6, 6))) shouldBe true

          val map2sLogFile = map2.path.resolve(0.toLogFileId)
          val copiedLogFileId = map1.path.resolve(1.toLogFileId)
          Effect.copy(map2sLogFile, copiedLogFileId)

          val log0 = map1.path.resolve(0.toLogFileId)
          val log0Bytes = Effect.readAllBytes(log0)

          val log1 = map1.path.resolve(1.toLogFileId)
          val log1Bytes = Effect.readAllBytes(log1)

          //fail recovery if one of two WAL files of the log is corrupted
          //corrupt 1.log bytes
          Effect.overwrite(log1, log1Bytes.drop(1))
          assertThrows[IllegalStateException] {
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = map1.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            )
          }
          Effect.overwrite(log1, log1Bytes) //fix log1 bytes

          //successfully recover Map by reading both WAL files if the second WAL file is corrupted
          //corrupt 1.log bytes
          Effect.overwrite(log1, log1Bytes.dropRight(1))
          val recoveredMapWith0LogCorrupted =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = map1.path,
              mmap = MMAP.Off(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = true
            )
          recoveredMapWith0LogCorrupted.item.sweep()

          //recovery state contains failure because the WAL file is partially recovered.
          recoveredMapWith0LogCorrupted.result.left.value.exception shouldBe a[IllegalStateException]
          //count instead of size because skipList's actual size can be higher.
          recoveredMapWith0LogCorrupted.item.cache.skipList.toIterable.count(_ => true) shouldBe 5 //5 because the 3rd entry in 1.log is corrupted

          //checking the recovered entries
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(1) shouldBe Memory.put(1, 1)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(2) shouldBe Memory.put(2)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(3) shouldBe Memory.put(3, 3)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(4) shouldBe Memory.put(4, 4)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(5) shouldBe Memory.put(5, 5)
          recoveredMapWith0LogCorrupted.item.cache.skipList.get(6).toOptionS shouldBe empty
      }
    }
  }

  "Randomly inserting data into Map and recovering the Map" should {
    "result in the recovered Map to have the same skipList as the Map before recovery" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          //run this test multiple times to randomly generate multiple combinations of overlapping key-value with optionally & randomly added Put, Remove, Range or Update.
          runThis(100.times, log = true) {
            implicit val testTimer: TestTimer = TestTimer.Incremental()

            //create a Map with randomly max size so that this test also covers when multiple maps are created. Also set flushOnOverflow to true so that the same Map gets written.
            val log =
              Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                folder = createRandomDir,
                mmap = MMAP.randomForLog(),
                flushOnOverflow = true,
                fileSize = randomIntMax(1.mb),
                dropCorruptedTailEntries = false
              ).item.sweep()

            //randomly create 100 key-values to insert into the Map. These key-values may contain range, update, or key-values deadlines randomly.
            val keyValues = randomizedKeyValues(10, addPut = true)
            //slice write them to that if log's randomly selected size is too small and multiple maps are written to.
            keyValues.groupedSlice(5) foreach {
              keyValues =>
                log.writeSync(keyValues.toLogEntry.value) shouldBe true
            }

            log.cache.skipList.values() shouldBe keyValues

            //write overlapping key-values to the same log which are randomly selected and may or may not contain range, update, or key-values deadlines.
            val updatedValues = randomizedKeyValues(10, startId = Some(keyValues.head.key.readInt()), addPut = true)
            val updatedEntries = updatedValues.toLogEntry.value
            log.writeSync(updatedEntries) shouldBe true

            //reopening the log should return in the original skipList.
            val reopened = log.reopen.sweep()

            reopened.cache.skipList.size shouldBe log.cache.skipList.size
            reopened.cache.skipList.toIterable.toList shouldBe log.cache.skipList.toIterable.toList

            reopened.delete
          }
      }
    }
  }

  "inserting data" when {
    "fileSize is too small that it overflows" should {
      "extend the fileSize & also log warn message" in {
        TestCaseSweeper {
          implicit sweeper =>

            runThis(100.times, log = true) {
              import LevelZeroLogEntryReader._
              import LevelZeroLogEntryWriter._
              import sweeper._

              //This test also shows how the merge handles situations when recovery happens on multiple log files
              //of the same log and also tests the importance of TimeOrder for Functions. Setting deleteAfterClean
              //to true ensure that the delete is not immediate and reopen will have to recover multiple log files.
              //Since the key-values are random, functions could also be generated and using TestTimer.Empty will
              //result in invalid recovery where reopen will result in different entries because functions without
              //time value set will result in duplicate merges.
              implicit val testTimer: TestTimer = TestTimer.Incremental()

              val mmap = TestForceSave.mmap()

              val log =
                Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
                  folder = createRandomDir,
                  //when deleteAfterClean is false, this will pass with TestTimer.Empty because the files are deleted
                  //immediately and reopen does not have to recovery multiple log files with functions. But we always
                  //use Timer when functions are enabled.
                  mmap = MMAP.On(deleteAfterClean = true, forceSave = mmap),
                  flushOnOverflow = true,
                  //setting t
                  fileSize = 1.byte,
                  dropCorruptedTailEntries = false
                ).item.sweep()

              //randomly create 100 key-values to insert into the Map. These key-values may contain range, update, or key-values deadlines randomly.
              val keyValues = randomizedKeyValues(100, addPut = true)
              val keyValueEntry = keyValues.toLogEntry.value
              //slice write them to that if log's randomly selected size is too small and multiple maps are written to.
              keyValues.groupedSlice(10) foreach {
                keyValues =>
                  log.writeSync(keyValues.toLogEntry.value) shouldBe true
              }
              log.cache.skipList.values() shouldBe keyValues

              //write overlapping key-values to the same log which are randomly selected and may or may not contain range, update, or key-values deadlines.
              val updatedValues = randomizedKeyValues(100, startId = Some(keyValues.head.key.readInt()), addPut = true)
              val updatedEntries = updatedValues.toLogEntry.value
              log.writeSync(updatedEntries) shouldBe true

              //reopening the log should return in the original skipList.
              val reopened = log.reopen.sweep()
              reopened.cache.skipList.size shouldBe log.cache.skipList.size

              reopened.cache.skipList.iterator.toList shouldBe log.cache.skipList.iterator.toList

              reopened.delete
            }
        }
      }
    }
  }

  "reopening after each write" in {
    runThis(1.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroLogEntryReader._
          import LevelZeroLogEntryWriter._
          import sweeper._

          val map1 =
            Log.persistent[Slice[Byte], Memory, LevelZeroLogCache](
              folder = createRandomDir,
              mmap = MMAP.randomForLog(),
              flushOnOverflow = true,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          (1 to 100) foreach {
            i =>
              map1.writeSync(LogEntry.Put(i, Memory.put(i, i))) shouldBe true
          }

          (1 to 100).foldLeft(map1) {
            case (log, i) =>
              log.cache.skipList.size shouldBe 100

              log.cache.skipList.get(i).getUnsafe shouldBe Memory.put(i: Slice[Byte], i: Slice[Byte])

              if (randomBoolean())
                log
              else
                log.reopen
          }
      }
    }
  }
}
