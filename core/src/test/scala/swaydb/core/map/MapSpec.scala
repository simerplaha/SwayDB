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

package swaydb.core.map

import java.nio.file.{FileAlreadyExistsException, Path}

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, MemoryOption, Value}
import swaydb.core.io.file.Effect._
import swaydb.core.io.file.{DBFile, Effect}
import swaydb.core.level.AppendixSkipListMerger
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.map.serializer._
import swaydb.core.segment.{Segment, SegmentIO, SegmentOption}
import swaydb.core.util.skiplist.SkipList
import swaydb.core.util.{BlockCacheFileIDGenerator, Extension}
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave, TestTimer}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.slice.Slice._

import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.RunThis._

import scala.jdk.CollectionConverters._

class MapSpec extends TestBase {

  implicit val ec = TestExecutionContext.executionContext
  implicit val keyOrder: KeyOrder[Sliced[Byte]] = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Sliced[Byte]] = TimeOrder.long
  implicit val skipListMerger = LevelZeroSkipListMerger()
  implicit val merger = AppendixSkipListMerger
  implicit def segmentIO = SegmentIO.random

  "Map" should {
    "initialise a memory level0" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryWriter._

          val map =
            Map.memory[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              fileSize = 1.mb,
              flushOnOverflow = false
            ).sweep()

          map.writeSync(MapEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map.writeSync(MapEntry.Put(2, Memory.put(2, 2))) shouldBe true
          map.get(1) shouldBe Memory.put(1, 1)
          map.get(2) shouldBe Memory.put(2, 2)

          map.hasRange shouldBe false

          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Remove](1, Memory.remove(1))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Remove](2, Memory.remove(2))) shouldBe true
          map.get(1) shouldBe Memory.remove(1)
          map.get(2) shouldBe Memory.remove(2)

          map.hasRange shouldBe false

          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](1, Memory.Range(1, 10, Value.FromValue.Null, Value.remove(None)))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](11, Memory.Range(11, 20, Value.put(20), Value.update(20)))) shouldBe true
          map.get(1) shouldBe Memory.Range(1, 10, Value.FromValue.Null, Value.remove(None))
          map.get(11) shouldBe Memory.Range(11, 20, Value.put(20), Value.update(20))

          map.hasRange shouldBe true
      }
    }

    "initialise a memory Appendix map" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixMapEntryWriter._

          val map =
            Map.memory[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment](
              nullKey = Slice.Null,
              nullValue = Segment.Null,
              fileSize = 1.mb,
              flushOnOverflow = false
            ).sweep()

          val segment1 = TestSegment()
          val segment2 = TestSegment()

          map.hasRange shouldBe false

          map.writeSync(MapEntry.Put[Sliced[Byte], Segment](1, segment1)) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Segment](2, segment2)) shouldBe true
          map.get(1) shouldBe segment1
          map.get(2) shouldBe segment2

          map.hasRange shouldBe false

          map.writeSync(MapEntry.Remove[Sliced[Byte]](1)) shouldBe true
          map.writeSync(MapEntry.Remove[Sliced[Byte]](2)) shouldBe true
          map.get(1).toOptionS shouldBe empty
          map.get(2).toOptionS shouldBe empty
      }
    }

    "initialise a persistent Level0 map and recover from it when it's empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          val map =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map.isEmpty shouldBe true
          map.close()
          //recover from an empty map
          val recovered =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = map.path,
              mmap = MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          recovered.isEmpty shouldBe true
          recovered.close()
      }
    }

    "initialise a persistent Appendix map and recover from it when it's empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          import AppendixMapEntryWriter._
          import sweeper._

          val appendixReader = AppendixMapEntryReader(MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()))
          import appendixReader._

          val map =
            Map.persistent[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment](
              nullKey = Slice.Null,
              nullValue = Segment.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false, fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map.isEmpty shouldBe true
          map.close()
          //recover from an empty map
          val recovered =
            Map.persistent[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment](
              nullKey = Slice.Null,
              nullValue = Segment.Null,
              folder = map.path,
              mmap = MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          recovered.isEmpty shouldBe true
          recovered.close()
      }
    }

    "initialise a persistent Level0 map and recover from it when it's contains data" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          def assertReads(map: PersistentMap[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory]) = {
            map.get(1) shouldBe Memory.put(1, 1)
            map.get(2) shouldBe Memory.remove(2)
            map.get(10) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
            map.get(15) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
            map.hasRange shouldBe true
          }

          def doRecover(path: Path): PersistentMap[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory] = {
            val recovered =
              Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
                nullKey = Slice.Null,
                nullValue = Memory.Null,
                folder = path,
                mmap = MMAP.randomForMap(),
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

          val map =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Put](1, Memory.put(1, 1))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Put](2, Memory.put(2, 2))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Remove](2, Memory.remove(2))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))) shouldBe true

          assertReads(map)

          //recover maps 10 times
          (1 to 10).foldLeft(map) {
            case (map, _) =>
              doRecover(map.path)
          }
      }
    }

    "initialise a persistent Appendix map and recover from it when it's contains data" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader = AppendixMapEntryReader(MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()))
          import AppendixMapEntryWriter._
          import appendixReader._

          val segment1 = TestSegment(Slice(Memory.put(1, 1, None), Memory.put(2, 2, None)))
          val segment2 = TestSegment(Slice(Memory.put(3, 3, None), Memory.put(4, 4, None)))

          val map =
            Map.persistent[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment](
              nullKey = Slice.Null,
              nullValue = Segment.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map.writeSync(MapEntry.Put[Sliced[Byte], Segment](1, segment1)) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Segment](2, segment2)) shouldBe true
          map.writeSync(MapEntry.Remove[Sliced[Byte]](2)) shouldBe true
          map.get(1) shouldBe segment1
          map.get(2).toOptionS shouldBe empty

          def doRecover(path: Path): PersistentMap[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment] = {
            val recovered =
              Map.persistent[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment](
                nullKey = Slice.Null,
                nullValue = Segment.Null,
                folder = map.path,
                mmap = MMAP.randomForMap(),
                flushOnOverflow = false,
                fileSize = 1.mb,
                dropCorruptedTailEntries = false
              ).item.sweep()

            recovered.get(1).getS shouldBe segment1
            recovered.get(2).toOptionS shouldBe empty
            recovered.close()

            if (recovered.mmap.isMMAP && OperatingSystem.isWindows)
              sweeper.receiveAll()

            recovered
          }

          //recover and maps 10 times
          (1 to 10).foldLeft(map) {
            case (map, _) =>
              doRecover(map.path)
          }
      }
    }

    "initialise a Map that has two persistent Level0 map files (second file did not value deleted due to early JVM termination)" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          val map1 =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1.writeSync(MapEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map1.writeSync(MapEntry.Put(2, Memory.put(2, 2))) shouldBe true
          map1.writeSync(MapEntry.Put(3, Memory.put(3, 3))) shouldBe true
          map1.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)))) shouldBe true

          val map2 =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map2.writeSync(MapEntry.Put(4, Memory.put(4, 4))) shouldBe true
          map2.writeSync(MapEntry.Put(5, Memory.put(5, 5))) shouldBe true
          map2.writeSync(MapEntry.Put(2, Memory.put(2, 22))) shouldBe true //second file will override 2's value to be 22
          map2.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))) shouldBe true

          //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
          val map2sLogFile = map2.path.resolve(0.toLogFileId)
          Effect.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

          //recover map 1 and it should contain all entries of map1 and map2
          val map1Recovered =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = map1.path,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1Recovered.get(1) shouldBe Memory.put(1, 1)
          map1Recovered.get(2) shouldBe Memory.put(2, 22) //second file overrides 2's value to be 22
          map1Recovered.get(3) shouldBe Memory.put(3, 3)
          map1Recovered.get(4) shouldBe Memory.put(4, 4)
          map1Recovered.get(5) shouldBe Memory.put(5, 5)
          map1Recovered.get(6).toOptionS shouldBe empty
          map1Recovered.get(10) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          map1Recovered.get(15) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))

          //recovered file's id is 2.log
          map1Recovered.path.files(Extension.Log).map(_.fileId) should contain only ((2, Extension.Log))
      }
    }

    "initialise a Map that has two persistent Appendix map files (second file did not value deleted due to early JVM termination)" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val appendixReader = AppendixMapEntryReader(MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()))
          import AppendixMapEntryWriter._
          import appendixReader._

          val segment1 = TestSegment(Slice(Memory.put(1, 1, None), Memory.put(2, 2, None)))
          val segment2 = TestSegment(Slice(Memory.put(2, 2, None), Memory.put(4, 4, None)))
          val segment3 = TestSegment(Slice(Memory.put(3, 3, None), Memory.put(6, 6, None)))
          val segment4 = TestSegment(Slice(Memory.put(4, 4, None), Memory.put(8, 8, None)))
          val segment5 = TestSegment(Slice(Memory.put(5, 5, None), Memory.put(10, 10, None)))
          val segment2Updated = TestSegment(Slice(Memory.put(2, 2, None), Memory.put(12, 12, None)))

          val map1 =
            Map.persistent[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment](
              nullKey = Slice.Null,
              nullValue = Segment.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1.writeSync(MapEntry.Put(1, segment1)) shouldBe true
          map1.writeSync(MapEntry.Put(2, segment2)) shouldBe true
          map1.writeSync(MapEntry.Put(3, segment3)) shouldBe true

          val map2 =
            Map.persistent[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment](
              nullKey = Slice.Null,
              nullValue = Segment.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map2.writeSync(MapEntry.Put(4, segment4)) shouldBe true
          map2.writeSync(MapEntry.Put(5, segment5)) shouldBe true
          map2.writeSync(MapEntry.Put(2, segment2Updated)) shouldBe true //second file will override 2's value to be segment2Updated

          //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
          val map2sLogFile = map2.path.resolve(0.toLogFileId)
          Effect.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

          //recover map 1 and it should contain all entries of map1 and map2
          val map1Recovered =
            Map.persistent[SliceOption[Byte], SegmentOption, Sliced[Byte], Segment](
              nullKey = Slice.Null,
              nullValue = Segment.Null,
              folder = map1.path,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1Recovered.get(1).getS shouldBe segment1
          map1Recovered.get(2).getS shouldBe segment2Updated //second file overrides 2's value to be segment2Updated
          map1Recovered.get(3).getS shouldBe segment3
          map1Recovered.get(4).getS shouldBe segment4
          map1Recovered.get(5).getS shouldBe segment5
          map1Recovered.get(6).toOptionS shouldBe empty

          //recovered file's id is 2.log
          map1Recovered.path.files(Extension.Log).map(_.fileId) should contain only ((2, Extension.Log))
      }
    }

    "fail initialise if the Map exists but recovery is not provided" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          val map =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb
            ).sweep()

          //fails because the file already exists.
          assertThrows[FileAlreadyExistsException] {
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = map.path,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb
            ).sweep()
          }

          //recovers because the recovery is provided
          Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
            nullKey = Slice.Null,
            nullValue = Memory.Null,
            folder = map.path,
            mmap = MMAP.Disabled(TestForceSave.channel()),
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
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

          val file =
            PersistentMap.recover[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              fileSize = 4.mb,
              skipList = skipList,
              dropCorruptedTailEntries = false
            )._1.item

          file.isOpen shouldBe true
          file.isMemoryMapped shouldBe false
          file.existsOnDisk shouldBe true
          file.fileSize shouldBe 0
          file.path.fileId shouldBe(0, Extension.Log)

          skipList.isEmpty shouldBe true
      }
    }

    "recover from an existing PersistentMap folder" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          //create a map
          val map =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map.writeSync(MapEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Remove](2, Memory.remove(2))) shouldBe true
          map.writeSync(MapEntry.Put(3, Memory.put(3, 3))) shouldBe true
          map.writeSync(MapEntry.Put(3, Memory.put(3, 3))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))) shouldBe true

          map.currentFilePath.fileId shouldBe(0, Extension.Log)

          map.hasRange shouldBe true

          val skipList =
            SkipList.concurrent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null
            )(keyOrder)

          //PersistentMap.recover below will delete this mmap file which on Windows
          //requires this map to be closed and cleaned before deleting.
          if (OperatingSystem.isWindows) {
            map.close()
            sweeper.receiveAll()
          }

          val recoveredFile =
            PersistentMap.recover(
              folder = map.path,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              fileSize = 4.mb,
              skipList = skipList,
              dropCorruptedTailEntries = false
            )._1.item.sweep()

          recoveredFile.isOpen shouldBe true
          recoveredFile.isMemoryMapped shouldBe false
          recoveredFile.existsOnDisk shouldBe true
          recoveredFile.path.fileId shouldBe(1, Extension.Log) //file id gets incremented on recover

          recoveredFile.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

          skipList.isEmpty shouldBe false
          skipList.get(1: Sliced[Byte]) shouldBe Memory.put(1, 1)
          skipList.get(2: Sliced[Byte]) shouldBe Memory.remove(2)
          skipList.get(3: Sliced[Byte]) shouldBe Memory.put(3, 3)
          skipList.get(10: Sliced[Byte]) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          skipList.get(15: Sliced[Byte]) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
      }
    }

    "recover from an existing PersistentMap folder when flushOnOverflow is true" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          //create a map
          val map =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = true,
              fileSize = 1.byte,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map.writeSync(MapEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Remove](2, Memory.remove(2))) shouldBe true
          map.writeSync(MapEntry.Put(3, Memory.put(3, 3))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](10, Memory.Range(10, 20, Value.FromValue.Null, Value.update(20)))) shouldBe true
          map.writeSync(MapEntry.Put[Sliced[Byte], Memory.Range](10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))) shouldBe true

          map.currentFilePath.fileId shouldBe(5, Extension.Log)
          map.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted
          map.path.resolveSibling(1.toLogFileId).exists shouldBe false //1.log gets deleted
          map.path.resolveSibling(2.toLogFileId).exists shouldBe false //2.log gets deleted
          map.path.resolveSibling(3.toLogFileId).exists shouldBe false //3.log gets deleted
          map.path.resolveSibling(4.toLogFileId).exists shouldBe false //4.log gets deleted

          if (OperatingSystem.isWindows) {
            map.close()
            sweeper.receiveAll()
          }

          //reopen file
          val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
          val recoveredFile = PersistentMap.recover(map.path, mmap = MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()), 1.byte, skipList, dropCorruptedTailEntries = false)._1.item.sweep()
          recoveredFile.isOpen shouldBe true
          recoveredFile.isMemoryMapped shouldBe true
          recoveredFile.existsOnDisk shouldBe true
          recoveredFile.path.fileId shouldBe(6, Extension.Log) //file id gets incremented on recover
          recoveredFile.path.resolveSibling(5.toLogFileId).exists shouldBe false //5.log gets deleted

          skipList.isEmpty shouldBe false
          skipList.get(1: Sliced[Byte]) shouldBe Memory.put(1, 1)
          skipList.get(2: Sliced[Byte]) shouldBe Memory.remove(2)
          skipList.get(3: Sliced[Byte]) shouldBe Memory.put(3, 3)
          skipList.get(10: Sliced[Byte]) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          skipList.get(15: Sliced[Byte]) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))

          if (OperatingSystem.isWindows) {
            recoveredFile.close()
            sweeper.receiveAll()
          }

          //reopen the recovered file
          val skipList2 = SkipList.concurrent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
          val recoveredFile2 = PersistentMap.recover(map.path, mmap = MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()), 1.byte, skipList2, dropCorruptedTailEntries = false)._1.item.sweep()
          recoveredFile2.isOpen shouldBe true
          recoveredFile2.isMemoryMapped shouldBe true
          recoveredFile2.existsOnDisk shouldBe true
          recoveredFile2.path.fileId shouldBe(7, Extension.Log) //file id gets incremented on recover
          recoveredFile2.path.resolveSibling(6.toLogFileId).exists shouldBe false //6.log gets deleted

          skipList2.isEmpty shouldBe false
          skipList2.get(1: Sliced[Byte]) shouldBe Memory.put(1, 1)
          skipList2.get(2: Sliced[Byte]) shouldBe Memory.remove(2)
          skipList2.get(3: Sliced[Byte]) shouldBe Memory.put(3, 3)
          skipList2.get(10: Sliced[Byte]) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          skipList2.get(15: Sliced[Byte]) shouldBe Memory.Range(15, 20, Value.FromValue.Null, Value.update(20))
      }
    }

    "recover from an existing PersistentMap folder with empty memory map" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          //create a map
          val map =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Enabled(OperatingSystem.isWindows, TestForceSave.mmap()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = false
            ).item

          map.currentFilePath.fileId shouldBe(0, Extension.Log)
          map.close()

          if (OperatingSystem.isWindows)
            sweeper.receiveAll()

          val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
          val file = PersistentMap.recover(map.path, MMAP.Disabled(TestForceSave.channel()), 4.mb, skipList, dropCorruptedTailEntries = false)._1.item.sweep()

          file.isOpen shouldBe true
          file.isMemoryMapped shouldBe false
          file.existsOnDisk shouldBe true
          file.path.fileId shouldBe(1, Extension.Log) //file id gets incremented on recover
          file.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

          skipList.isEmpty shouldBe true
      }
    }
  }

  "PersistentMap.nextFile" should {
    "creates a new file from the current file" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          val skipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
          skipList.put(1, Memory.put(1, 1))
          skipList.put(2, Memory.put(2, 2))
          skipList.put(3, Memory.remove(3))
          skipList.put(10, Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None)))
          skipList.put(15, Memory.Range(15, 20, Value.put(15), Value.update(14)))

          val currentFile = PersistentMap.recover(createRandomDir, MMAP.Disabled(TestForceSave.channel()), 4.mb, skipList, dropCorruptedTailEntries = false)._1.item.sweep()
          val nextFile = PersistentMap.nextFile(currentFile, MMAP.Disabled(TestForceSave.channel()), 4.mb, skipList).sweep()

          val nextFileSkipList = SkipList.concurrent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
          val nextFileBytes = DBFile.channelRead(nextFile.path, randomThreadSafeIOStrategy(), autoClose = false, blockCacheFileId = BlockCacheFileIDGenerator.nextID).readAll
          val mapEntries = MapCodec.read(nextFileBytes, dropCorruptedTailEntries = false).value.item.value
          mapEntries applyTo nextFileSkipList

          nextFileSkipList.get(1: Sliced[Byte]) shouldBe Memory.put(1, 1)
          nextFileSkipList.get(2: Sliced[Byte]) shouldBe Memory.put(2, 2)
          nextFileSkipList.get(3: Sliced[Byte]) shouldBe Memory.remove(3)
          nextFileSkipList.get(10: Sliced[Byte]) shouldBe Memory.Range(10, 15, Value.FromValue.Null, Value.remove(None))
          nextFileSkipList.get(15: Sliced[Byte]) shouldBe Memory.Range(15, 20, Value.put(15), Value.update(14))
      }
    }
  }

  "PersistentMap.recovery on corruption" should {
    import LevelZeroMapEntryReader._
    import LevelZeroMapEntryWriter._

    "fail if the WAL file is corrupted and and when dropCorruptedTailEntries = false" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val map =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          (1 to 100) foreach {
            i =>
              map.writeSync(MapEntry.Put(i, Memory.put(i, i))) shouldBe true
          }
          map.size shouldBe 100
          val allBytes = Effect.readAllBytes(map.currentFilePath)

          def assertRecover =
            assertThrows[IllegalStateException] {
              Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
                nullKey = Slice.Null,
                nullValue = Memory.Null,
                folder = map.currentFilePath.getParent,
                mmap = MMAP.Disabled(TestForceSave.channel()),
                flushOnOverflow = false,
                fileSize = 4.mb,
                dropCorruptedTailEntries = false
              )
            }

          //drop last byte
          Effect.overwrite(map.currentFilePath, allBytes.dropRight(1))
          assertRecover

          //drop first byte
          Effect.overwrite(map.currentFilePath, allBytes.drop(1))
          assertRecover
      }
    }

    "successfully recover partial data if WAL file is corrupted and when dropCorruptedTailEntries = true" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val map =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          (1 to 100) foreach {
            i =>
              map.writeSync(MapEntry.Put(i, Memory.put(i, i))) shouldBe true
          }
          map.size shouldBe 100
          val allBytes = Effect.readAllBytes(map.currentFilePath)

          //recover again with SkipLogOnCorruption, since the last entry is corrupted, the first two entries will still value read
          Effect.overwrite(map.currentFilePath, allBytes.dropRight(1))

          val recoveredMap =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = map.currentFilePath.getParent,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = true
            ).item.sweep()

          (1 to 99) foreach {
            i =>
              recoveredMap.get(i) shouldBe Memory.put(i, i)
          }
          recoveredMap.contains(100) shouldBe false

          //if the top entry is corrupted.
          Effect.overwrite(recoveredMap.currentFilePath, allBytes.drop(1))

          val recoveredMap2 =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              Slice.Null,
              Memory.Null,
              recoveredMap.currentFilePath.getParent,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 4.mb,
              dropCorruptedTailEntries = true
            ).item.sweep()

          recoveredMap2.isEmpty shouldBe true
      }
    }
  }

  "PersistentMap.recovery on corruption" when {
    "there are two WAL files and the first file is corrupted" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          val map1 =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1.writeSync(MapEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map1.writeSync(MapEntry.Put(2, Memory.put(2, 2))) shouldBe true
          map1.writeSync(MapEntry.Put(3, Memory.put(3, 3))) shouldBe true

          val map2 =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map2.writeSync(MapEntry.Put(4, Memory.put(4, 4))) shouldBe true
          map2.writeSync(MapEntry.Put(5, Memory.put(5, 5))) shouldBe true
          map2.writeSync(MapEntry.Put(6, Memory.put(6, 6))) shouldBe true

          val map2sLogFile = map2.path.resolve(0.toLogFileId)
          val copiedLogFileId = map1.path.resolve(1.toLogFileId)
          //move map2's log file into map1's log file folder named as 1.log.
          Effect.copy(map2sLogFile, copiedLogFileId)

          val log0 = map1.path.resolve(0.toLogFileId)
          val log0Bytes = Effect.readAllBytes(log0)

          val log1 = map1.path.resolve(1.toLogFileId)
          val log1Bytes = Effect.readAllBytes(log1)

          //fail recovery if first map is corrupted
          //corrupt 0.log bytes
          Effect.overwrite(log0, log0Bytes.drop(1))
          assertThrows[IllegalStateException] {
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = map1.path,
              mmap = MMAP.Disabled(TestForceSave.channel()),
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
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = map1.path,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = true
            )

          recoveredMapWith0LogCorrupted.item.sweep()

          //recovery state contains failure because the WAL file is partially recovered.
          recoveredMapWith0LogCorrupted.result.left.value.exception shouldBe a[IllegalStateException]
          //count instead of size because skipList's actual size can be higher.
          recoveredMapWith0LogCorrupted.item.asScala.count(_ => true) shouldBe 5 //5 because the 3rd entry in 0.log is corrupted

          //checking the recovered entries
          recoveredMapWith0LogCorrupted.item.get(1) shouldBe Memory.put(1, 1)
          recoveredMapWith0LogCorrupted.item.get(2) shouldBe Memory.put(2, 2)
          recoveredMapWith0LogCorrupted.item.get(3).toOptionS shouldBe empty //since the last byte of 0.log file is corrupted, the last entry is missing
          recoveredMapWith0LogCorrupted.item.get(4) shouldBe Memory.put(4, 4)
          recoveredMapWith0LogCorrupted.item.get(5) shouldBe Memory.put(5, 5)
          recoveredMapWith0LogCorrupted.item.get(6) shouldBe Memory.put(6, 6)
      }
    }
  }

  "PersistentMap.recovery on corruption" when {
    "there are two WAL files and the second file is corrupted" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          val map1 =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map1.writeSync(MapEntry.Put(1, Memory.put(1, 1))) shouldBe true
          map1.writeSync(MapEntry.Put(2, Memory.put(2))) shouldBe true
          map1.writeSync(MapEntry.Put(3, Memory.put(3, 3))) shouldBe true

          val map2 =
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = createRandomDir,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = false
            ).item.sweep()

          map2.writeSync(MapEntry.Put(4, Memory.put(4, 4))) shouldBe true
          map2.writeSync(MapEntry.Put(5, Memory.put(5, 5))) shouldBe true
          map2.writeSync(MapEntry.Put(6, Memory.put(6, 6))) shouldBe true

          val map2sLogFile = map2.path.resolve(0.toLogFileId)
          val copiedLogFileId = map1.path.resolve(1.toLogFileId)
          Effect.copy(map2sLogFile, copiedLogFileId)

          val log0 = map1.path.resolve(0.toLogFileId)
          val log0Bytes = Effect.readAllBytes(log0)

          val log1 = map1.path.resolve(1.toLogFileId)
          val log1Bytes = Effect.readAllBytes(log1)

          //fail recovery if one of two WAL files of the map is corrupted
          //corrupt 1.log bytes
          Effect.overwrite(log1, log1Bytes.drop(1))
          assertThrows[IllegalStateException] {
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = map1.path,
              mmap = MMAP.Disabled(TestForceSave.channel()),
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
            Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
              nullKey = Slice.Null,
              nullValue = Memory.Null,
              folder = map1.path,
              mmap = MMAP.Disabled(TestForceSave.channel()),
              flushOnOverflow = false,
              fileSize = 1.mb,
              dropCorruptedTailEntries = true
            )
          recoveredMapWith0LogCorrupted.item.sweep()

          //recovery state contains failure because the WAL file is partially recovered.
          recoveredMapWith0LogCorrupted.result.left.value.exception shouldBe a[IllegalStateException]
          //count instead of size because skipList's actual size can be higher.
          recoveredMapWith0LogCorrupted.item.asScala.count(_ => true) shouldBe 5 //5 because the 3rd entry in 1.log is corrupted

          //checking the recovered entries
          recoveredMapWith0LogCorrupted.item.get(1) shouldBe Memory.put(1, 1)
          recoveredMapWith0LogCorrupted.item.get(2) shouldBe Memory.put(2)
          recoveredMapWith0LogCorrupted.item.get(3) shouldBe Memory.put(3, 3)
          recoveredMapWith0LogCorrupted.item.get(4) shouldBe Memory.put(4, 4)
          recoveredMapWith0LogCorrupted.item.get(5) shouldBe Memory.put(5, 5)
          recoveredMapWith0LogCorrupted.item.get(6).toOptionS shouldBe empty
      }
    }
  }

  "Randomly inserting data into Map and recovering the Map" should {
    "result in the recovered Map to have the same skipList as the Map before recovery" in {
      TestCaseSweeper {
        implicit sweeper =>
          import LevelZeroMapEntryReader._
          import LevelZeroMapEntryWriter._
          import sweeper._

          //run this test multiple times to randomly generate multiple combinations of overlapping key-value with optionally & randomly added Put, Remove, Range or Update.
          (1 to 100) foreach {
            _ =>
              implicit val testTimer: TestTimer = TestTimer.Incremental()

              //create a Map with randomly max size so that this test also covers when multiple maps are created. Also set flushOnOverflow to true so that the same Map gets written.
              val map =
                Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
                  nullKey = Slice.Null,
                  nullValue = Memory.Null,
                  folder = createRandomDir,
                  mmap = MMAP.randomForMap(),
                  flushOnOverflow = true,
                  fileSize = randomIntMax(1.mb),
                  dropCorruptedTailEntries = false
                ).item.sweep()

              //randomly create 100 key-values to insert into the Map. These key-values may contain range, update, or key-values deadlines randomly.
              val keyValues = randomizedKeyValues(1000, addPut = true)
              //slice write them to that if map's randomly selected size is too small and multiple maps are written to.
              keyValues.groupedSlice(5) foreach {
                keyValues =>
                  map.writeSync(keyValues.toMapEntry.value) shouldBe true
              }
              map.values().asScala shouldBe keyValues

              //write overlapping key-values to the same map which are randomly selected and may or may not contain range, update, or key-values deadlines.
              val updatedValues = randomizedKeyValues(1000, startId = Some(keyValues.head.key.readInt()), addPut = true)
              val updatedEntries = updatedValues.toMapEntry.value
              map.writeSync(updatedEntries) shouldBe true

              //reopening the map should return in the original skipList.
              val reopened = map.reopen.sweep()
              reopened.size shouldBe map.size
              reopened.asScala shouldBe map.asScala
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
              import LevelZeroMapEntryReader._
              import LevelZeroMapEntryWriter._
              import sweeper._

              //This test also shows how the merge handles situations when recovery happens on multiple log files
              //of the same log and also tests the importance of TimeOrder for Functions. Setting deleteAfterClean
              //to true ensure that the delete is not immediate and reopen will have to recover multiple log files.
              //Since the key-values are random, functions could also be generated and using TestTimer.Empty will
              //result in invalid recovery where reopen will result in different entries because functions without
              //time value set will result in duplicate merges.
              implicit val testTimer: TestTimer = TestTimer.Incremental()

              val mmap = TestForceSave.mmap()

              val map =
                Map.persistent[SliceOption[Byte], MemoryOption, Sliced[Byte], Memory](
                  nullKey = Slice.Null,
                  nullValue = Memory.Null,
                  folder = createRandomDir,
                  //when deleteAfterClean is false, this will pass with TestTimer.Empty because the files are deleted
                  //immediately and reopen does not have to recovery multiple log files with functions. But we always
                  //use Timer when functions are enabled.
                  mmap = MMAP.Enabled(deleteAfterClean = true, forceSave = mmap),
                  flushOnOverflow = true,
                  //setting t
                  fileSize = 1.byte,
                  dropCorruptedTailEntries = false
                ).item.sweep()

              //randomly create 100 key-values to insert into the Map. These key-values may contain range, update, or key-values deadlines randomly.
              val keyValues = randomizedKeyValues(100, addPut = true)
              val keyValueEntry = keyValues.toMapEntry.value
              //slice write them to that if map's randomly selected size is too small and multiple maps are written to.
              keyValues.groupedSlice(10) foreach {
                keyValues =>
                  map.writeSync(keyValues.toMapEntry.value) shouldBe true
              }
              map.values().asScala shouldBe keyValues

              //write overlapping key-values to the same map which are randomly selected and may or may not contain range, update, or key-values deadlines.
              val updatedValues = randomizedKeyValues(100, startId = Some(keyValues.head.key.readInt()), addPut = true)
              val updatedEntries = updatedValues.toMapEntry.value
              map.writeSync(updatedEntries) shouldBe true

              //reopening the map should return in the original skipList.
              val reopened = map.reopen.sweep()
              reopened.size shouldBe map.size

              reopened.asScala shouldBe map.asScala

              reopened.delete
            }
        }
      }
    }
  }
}
