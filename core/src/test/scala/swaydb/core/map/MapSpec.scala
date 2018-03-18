/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.map

import java.nio.file.{FileAlreadyExistsException, Files, Path}
import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.data.{SegmentEntryReadOnly, Transient, Value}
import swaydb.core.io.file.DBFile
import swaydb.core.level.zero.SkipListRangeConflictResolver
import swaydb.core.map.serializer._
import swaydb.core.segment.Segment
import swaydb.core.util.Extension
import swaydb.core.util.FileUtil._
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class MapSpec extends TestBase {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: (SegmentEntryReadOnly, Segment) => Unit = TestLimitQueues.keyValueLimiter
  implicit val skipListRangeConflictResolver = SkipListRangeConflictResolver

  import swaydb.core.level.Level.SkipListConflictResolver

  val appendixReader = AppendixMapEntryReader(false, true, true, false)

  "Map" should {
    "initialise a memory level0" in {
      import LevelZeroMapEntryWriter._

      val map = Map.memory[Slice[Byte], Value](1.mb, flushOnOverflow = false)

      map.write(MapEntry.Put(1, Value.Put(Some(1)))).assertGet shouldBe true
      map.write(MapEntry.Put(2, Value.Put(Some(2)))).assertGet shouldBe true
      map.get(1).assertGet._2 shouldBe Value.Put(Some(1))
      map.get(2).assertGet._2 shouldBe Value.Put(Some(2))

      map.hasRange shouldBe false

      map.write(MapEntry.Put[Slice[Byte], Value.Remove](1, Value.Remove)).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Remove](2, Value.Remove)).assertGet shouldBe true
      map.get(1).assertGet._2 shouldBe Value.Remove
      map.get(2).assertGet._2 shouldBe Value.Remove

      map.hasRange shouldBe false

      map.write(MapEntry.Put[Slice[Byte], Value.Range](1, Value.Range(10, None, Value.Remove))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Range](11, Value.Range(20, Some(Value.Put(20)), Value.Put(20)))).assertGet shouldBe true
      map.get(1).assertGet._2 shouldBe Value.Range(10, None, Value.Remove)
      map.get(11).assertGet._2 shouldBe Value.Range(20, Some(Value.Put(20)), Value.Put(20))
      map.get(2) shouldBe None

      map.hasRange shouldBe true
    }

    "initialise a memory Appendix map" in {
      import AppendixMapEntryWriter._

      val map = Map.memory[Slice[Byte], Segment](1.mb, flushOnOverflow = false)
      val segment1 = TestSegment().assertGet
      val segment2 = TestSegment().assertGet

      map.hasRange shouldBe false

      map.write(MapEntry.Put[Slice[Byte], Segment](1, segment1)).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Segment](2, segment2)).assertGet shouldBe true
      map.get(1).assertGet._2 shouldBe segment1
      map.get(2).assertGet._2 shouldBe segment2

      map.hasRange shouldBe false

      map.write(MapEntry.Remove[Slice[Byte]](1)).assertGet shouldBe true
      map.write(MapEntry.Remove[Slice[Byte]](2)).assertGet shouldBe true
      map.get(1) shouldBe empty
      map.get(2) shouldBe empty
    }

    "initialise a persistent Level0 map and recover from it when it's empty" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map.isEmpty shouldBe true
      map.close().assertGet
      //recover from an empty map
      val recovered = Map.persistent[Slice[Byte], Value](map.path, mmap = true, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      recovered.isEmpty shouldBe true
      recovered.close().assertGet
    }

    "initialise a persistent Appendix map and recover from it when it's empty" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val map = Map.persistent[Slice[Byte], Segment](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map.isEmpty shouldBe true
      map.close().assertGet
      //recover from an empty map
      val recovered = Map.persistent[Slice[Byte], Segment](map.path, mmap = true, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      recovered.isEmpty shouldBe true
      recovered.close().assertGet
    }

    "initialise a persistent Level0 map and recover from it when it's contains data" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map.write(MapEntry.Put[Slice[Byte], Value.Put](1, Value.Put(Some(1)))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Put](2, Value.Put(Some(2)))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Remove](2, Value.Remove)).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Range](10, Value.Range(20, None, Value.Put(20)))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Range](10, Value.Range(15, None, Value.Remove))).assertGet shouldBe true

      map.get(1).assertGet._2 shouldBe Value.Put(Some(1))
      map.get(2).assertGet._2 shouldBe Value.Remove
      map.get(10).assertGet._2 shouldBe Value.Range(15, None, Value.Remove)
      map.get(15).assertGet._2 shouldBe Value.Range(20, None, Value.Put(20))

      def doRecover(path: Path): PersistentMap[Slice[Byte], Value] = {
        val recovered = Map.persistent[Slice[Byte], Value](map.path, mmap = Random.nextBoolean(), flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
        recovered.get(1).assertGet._2 shouldBe Value.Put(Some(1))
        recovered.get(2).assertGet._2 shouldBe Value.Remove
        recovered.get(10).assertGet._2 shouldBe Value.Range(15, None, Value.Remove)
        recovered.get(15).assertGet._2 shouldBe Value.Range(20, None, Value.Put(20))
        recovered.hasRange shouldBe true
        recovered.close().assertGet
        recovered
      }

      //recover the maps 10 times
      (1 to 10).foldLeft(map) {
        case (map, _) =>
          doRecover(map.path)
      }
      map.close().assertGet
    }

    "initialise a persistent Appendix map and recover from it when it's contains data" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val segment1 = TestSegment(Slice(Transient.Put(1, Some(1), 0.1, None), Transient.Put(2, Some(2), 0.1, None)).updateStats).assertGet
      val segment2 = TestSegment(Slice(Transient.Put(3, Some(3), 0.1, None), Transient.Put(4, Some(4), 0.1, None)).updateStats).assertGet

      val map = Map.persistent[Slice[Byte], Segment](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map.write(MapEntry.Put[Slice[Byte], Segment](1, segment1)).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Segment](2, segment2)).assertGet shouldBe true
      map.write(MapEntry.Remove[Slice[Byte]](2)).assertGet shouldBe true
      map.get(1).assertGet._2 shouldBe segment1
      map.get(2) shouldBe empty

      def doRecover(path: Path): PersistentMap[Slice[Byte], Segment] = {
        val recovered = Map.persistent[Slice[Byte], Segment](map.path, mmap = Random.nextBoolean(), flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
        recovered.get(1).assertGet._2 shouldBe segment1
        recovered.get(2) shouldBe empty
        recovered.close().assertGet
        recovered
      }

      //recover and maps 10 times
      (1 to 10).foldLeft(map) {
        case (map, _) =>
          doRecover(map.path)
      }
      map.close().assertGet
    }

    "initialise a Map that has two persistent Level0 map files (second file did not get deleted due to early JVM termination)" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map1 = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map1.write(MapEntry.Put(1, Value.Put(Some(1)))).assertGet shouldBe true
      map1.write(MapEntry.Put(2, Value.Put(Some(2)))).assertGet shouldBe true
      map1.write(MapEntry.Put(3, Value.Put(Some(3)))).assertGet shouldBe true
      map1.write(MapEntry.Put[Slice[Byte], Value.Range](10, Value.Range(20, None, Value.Put(20)))).assertGet shouldBe true

      val map2 = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map2.write(MapEntry.Put(4, Value.Put(Some(4)))).assertGet shouldBe true
      map2.write(MapEntry.Put(5, Value.Put(Some(5)))).assertGet shouldBe true
      map2.write(MapEntry.Put(2, Value.Put(Some(22)))).assertGet shouldBe true //second file will override 2's value to be 22
      map2.write(MapEntry.Put[Slice[Byte], Value.Range](10, Value.Range(15, None, Value.Remove))).assertGet shouldBe true

      //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
      val map2sLogFile = map2.path.resolve(0.toLogFileId)
      Files.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

      //recover map 1 and it should contain all entries of map1 and map2
      val map1Recovered = Map.persistent[Slice[Byte], Value](map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map1Recovered.get(1).assertGet._2 shouldBe Value.Put(Some(1))
      map1Recovered.get(2).assertGet._2 shouldBe Value.Put(Some(22)) //second file overrides 2's value to be 22
      map1Recovered.get(3).assertGet._2 shouldBe Value.Put(Some(3))
      map1Recovered.get(4).assertGet._2 shouldBe Value.Put(Some(4))
      map1Recovered.get(5).assertGet._2 shouldBe Value.Put(Some(5))
      map1Recovered.get(6) shouldBe empty
      map1Recovered.get(10).assertGet._2 shouldBe Value.Range(15, None, Value.Remove)
      map1Recovered.get(15).assertGet._2 shouldBe Value.Range(20, None, Value.Put(20))

      //recovered file's id is 2.log
      map1Recovered.path.files(Extension.Log).map(_.fileId.assertGet) should contain only ((2, Extension.Log))

      map1.close().assertGet
      map2.close().assertGet
      map1Recovered.close().assertGet
    }

    "initialise a Map that has two persistent Appendix map files (second file did not get deleted due to early JVM termination)" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val segment1 = TestSegment(Slice(Transient.Put(1, Some(1), 0.1, None), Transient.Put(2, Some(2), 0.1, None)).updateStats).assertGet
      val segment2 = TestSegment(Slice(Transient.Put(3, Some(3), 0.1, None), Transient.Put(4, Some(4), 0.1, None)).updateStats).assertGet
      val segment3 = TestSegment(Slice(Transient.Put(5, Some(5), 0.1, None), Transient.Put(6, Some(6), 0.1, None)).updateStats).assertGet
      val segment4 = TestSegment(Slice(Transient.Put(7, Some(7), 0.1, None), Transient.Put(8, Some(8), 0.1, None)).updateStats).assertGet
      val segment5 = TestSegment(Slice(Transient.Put(9, Some(9), 0.1, None), Transient.Put(10, Some(10), 0.1, None)).updateStats).assertGet
      val segment2Updated = TestSegment(Slice(Transient.Put(11, Some(11), 0.1, None), Transient.Put(12, Some(12), 0.1, None)).updateStats).assertGet

      val map1 = Map.persistent[Slice[Byte], Segment](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map1.write(MapEntry.Put(1, segment1)).assertGet shouldBe true
      map1.write(MapEntry.Put(2, segment2)).assertGet shouldBe true
      map1.write(MapEntry.Put(3, segment3)).assertGet shouldBe true

      val map2 = Map.persistent[Slice[Byte], Segment](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map2.write(MapEntry.Put(4, segment4)).assertGet shouldBe true
      map2.write(MapEntry.Put(5, segment5)).assertGet shouldBe true
      map2.write(MapEntry.Put(2, segment2Updated)).assertGet shouldBe true //second file will override 2's value to be segment2Updated

      //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
      val map2sLogFile = map2.path.resolve(0.toLogFileId)
      Files.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

      //recover map 1 and it should contain all entries of map1 and map2
      val map1Recovered = Map.persistent[Slice[Byte], Segment](map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map1Recovered.get(1).assertGet._2 shouldBe segment1
      map1Recovered.get(2).assertGet._2 shouldBe segment2Updated //second file overrides 2's value to be segment2Updated
      map1Recovered.get(3).assertGet._2 shouldBe segment3
      map1Recovered.get(4).assertGet._2 shouldBe segment4
      map1Recovered.get(5).assertGet._2 shouldBe segment5
      map1Recovered.get(6) shouldBe empty

      //recovered file's id is 2.log
      map1Recovered.path.files(Extension.Log).map(_.fileId.assertGet) should contain only ((2, Extension.Log))

      map1.close().assertGet
      map2.close().assertGet
      map1Recovered.close().assertGet
      segment1.close.assertGet
      segment2.close.assertGet
      segment3.close.assertGet
      segment4.close.assertGet
      segment5.close.assertGet
      segment2Updated.close.assertGet
    }

    "fail initialise if the Map exists but recovery is not provided" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb).assertGet

      //fails because the file already exists.
      Map.persistent[Slice[Byte], Value](map.path, mmap = false, flushOnOverflow = false, 1.mb).failed.assertGet shouldBe a[FileAlreadyExistsException]

      //recovers because the recovery is provided
      Map.persistent[Slice[Byte], Value](map.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet

      map.close().assertGet
    }
  }

  "PersistentMap.recover" should {
    "recover from an empty PersistentMap folder" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value]()
      val file = PersistentMap.recover[Slice[Byte], Value](createRandomDir, false, 4.mb, skipList, dropCorruptedTailEntries = false).assertGet._1.item

      file.isOpen shouldBe true
      file.isMemoryMapped.assertGet shouldBe false
      file.existsOnDisk shouldBe true
      file.fileSize.assertGet shouldBe 0
      file.path.fileId.assertGet shouldBe(0, Extension.Log)

      skipList.isEmpty shouldBe true
      file.close.assertGet
    }

    "recover from an existing PersistentMap folder" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      //create a map
      val map = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = true, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = false).assertGet.item
      map.write(MapEntry.Put(1, Value.Put(1))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Remove](2, Value.Remove)).assertGet shouldBe true
      map.write(MapEntry.Put(3, Value.Put(3))).assertGet shouldBe true
      map.write(MapEntry.Put(3, Value.Put(3))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Range](10, Value.Range(20, None, Value.Put(20)))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Range](10, Value.Range(15, None, Value.Remove))).assertGet shouldBe true

      map.currentFilePath.fileId.assertGet shouldBe(0, Extension.Log)

      map.hasRange shouldBe true

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      val recoveredFile = PersistentMap.recover(map.path, false, 4.mb, skipList, dropCorruptedTailEntries = false).assertGet._1.item

      recoveredFile.isOpen shouldBe true
      recoveredFile.isMemoryMapped.assertGet shouldBe false
      recoveredFile.existsOnDisk shouldBe true
      recoveredFile.path.fileId.assertGet shouldBe(1, Extension.Log) //file id gets incremented on recover
      recoveredFile.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

      skipList.isEmpty shouldBe false
      skipList.get(1: Slice[Byte]) shouldBe Value.Put(1)
      skipList.get(2: Slice[Byte]) shouldBe Value.Remove
      skipList.get(3: Slice[Byte]) shouldBe Value.Put(3)
      skipList.get(10: Slice[Byte]) shouldBe Value.Range(15, None, Value.Remove)
      skipList.get(15: Slice[Byte]) shouldBe Value.Range(20, None, Value.Put(20))
    }

    "recover from an existing PersistentMap folder when flushOnOverflow is true" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      //create a map
      val map = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = true, flushOnOverflow = true, fileSize = 1.byte, dropCorruptedTailEntries = false).assertGet.item

      map.write(MapEntry.Put(1, Value.Put(1))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Remove](2, Value.Remove)).assertGet shouldBe true
      map.write(MapEntry.Put(3, Value.Put(3))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Range](10, Value.Range(20, None, Value.Put(20)))).assertGet shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Value.Range](10, Value.Range(15, None, Value.Remove))).assertGet shouldBe true

      map.currentFilePath.fileId.assertGet shouldBe(5, Extension.Log)
      map.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted
      map.path.resolveSibling(1.toLogFileId).exists shouldBe false //1.log gets deleted
      map.path.resolveSibling(2.toLogFileId).exists shouldBe false //2.log gets deleted
      map.path.resolveSibling(3.toLogFileId).exists shouldBe false //3.log gets deleted
      map.path.resolveSibling(4.toLogFileId).exists shouldBe false //4.log gets deleted

      //reopen file
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      val recoveredFile = PersistentMap.recover(map.path, true, 1.byte, skipList, dropCorruptedTailEntries = false).assertGet._1.item
      recoveredFile.isOpen shouldBe true
      recoveredFile.isMemoryMapped.assertGet shouldBe true
      recoveredFile.existsOnDisk shouldBe true
      recoveredFile.path.fileId.assertGet shouldBe(6, Extension.Log) //file id gets incremented on recover
      recoveredFile.path.resolveSibling(5.toLogFileId).exists shouldBe false //5.log gets deleted

      skipList.isEmpty shouldBe false
      skipList.get(1: Slice[Byte]) shouldBe Value.Put(1)
      skipList.get(2: Slice[Byte]) shouldBe Value.Remove
      skipList.get(3: Slice[Byte]) shouldBe Value.Put(3)
      skipList.get(10: Slice[Byte]) shouldBe Value.Range(15, None, Value.Remove)
      skipList.get(15: Slice[Byte]) shouldBe Value.Range(20, None, Value.Put(20))

      //reopen the recovered file
      val skipList2 = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      val recoveredFile2 = PersistentMap.recover(map.path, true, 1.byte, skipList2, dropCorruptedTailEntries = false).assertGet._1.item
      recoveredFile2.isOpen shouldBe true
      recoveredFile2.isMemoryMapped.assertGet shouldBe true
      recoveredFile2.existsOnDisk shouldBe true
      recoveredFile2.path.fileId.assertGet shouldBe(7, Extension.Log) //file id gets incremented on recover
      recoveredFile2.path.resolveSibling(6.toLogFileId).exists shouldBe false //6.log gets deleted

      skipList2.isEmpty shouldBe false
      skipList2.get(1: Slice[Byte]) shouldBe Value.Put(1)
      skipList2.get(2: Slice[Byte]) shouldBe Value.Remove
      skipList2.get(3: Slice[Byte]) shouldBe Value.Put(3)
      skipList2.get(10: Slice[Byte]) shouldBe Value.Range(15, None, Value.Remove)
      skipList2.get(15: Slice[Byte]) shouldBe Value.Range(20, None, Value.Put(20))

      map.close().assertGet
      recoveredFile.close.assertGet
      recoveredFile2.close.assertGet
    }

    "recover from an existing PersistentMap folder with empty memory map" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      //create a map
      val map = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = true, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = false).assertGet.item
      map.currentFilePath.fileId.assertGet shouldBe(0, Extension.Log)
      map.close().assertGet

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      val file = PersistentMap.recover(map.path, false, 4.mb, skipList, dropCorruptedTailEntries = false).assertGet._1.item

      file.isOpen shouldBe true
      file.isMemoryMapped.assertGet shouldBe false
      file.existsOnDisk shouldBe true
      file.path.fileId.assertGet shouldBe(1, Extension.Log) //file id gets incremented on recover
      file.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

      skipList.isEmpty shouldBe true

      file.close.assertGet
    }
  }

  "PersistentMap.nextFile" should {
    "creates a new file from the current file" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val skipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      skipList.put(1, Value.Put(1))
      skipList.put(2, Value.Put(2))
      skipList.put(3, Value.Remove)
      skipList.put(10, Value.Range(15, None, Value.Remove))
      skipList.put(15, Value.Range(20, Some(Value.Put(15)), Value.Put(14)))

      val currentFile = PersistentMap.recover(createRandomDir, false, 4.mb, skipList, dropCorruptedTailEntries = false).assertGet._1.item
      val nextFile = PersistentMap.nextFile(currentFile, false, 4.mb, skipList).assertGet

      val nextFileSkipList = new ConcurrentSkipListMap[Slice[Byte], Value](ordering)
      val nextFileBytes = DBFile.channelRead(nextFile.path).assertGet.readAll.assertGet
      val mapEntries = MapCodec.read(nextFileBytes, dropCorruptedTailEntries = false).assertGet.item.assertGet
      mapEntries applyTo nextFileSkipList

      nextFileSkipList.get(1: Slice[Byte]) shouldBe Value.Put(1)
      nextFileSkipList.get(2: Slice[Byte]) shouldBe Value.Put(2)
      nextFileSkipList.get(3: Slice[Byte]) shouldBe Value.Remove
      nextFileSkipList.get(10: Slice[Byte]) shouldBe Value.Range(15, None, Value.Remove)
      nextFileSkipList.get(15: Slice[Byte]) shouldBe Value.Range(20, Some(Value.Put(15)), Value.Put(14))

      currentFile.close.assertGet
      nextFile.close.assertGet
    }
  }

  "PersistentMap.recovery on corruption" should {
    import LevelZeroMapEntryReader._
    import LevelZeroMapEntryWriter._

    val map = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = false).assertGet.item
    (1 to 100) foreach {
      i =>
        map.write(MapEntry.Put(i, Value.Put(i))).assertGet shouldBe true
    }
    map.size shouldBe 100
    val allBytes = Files.readAllBytes(map.currentFilePath)

    "fail if the WAL file is corrupted and and when dropCorruptedTailEntries = false" in {

      def assertRecover =
        Map.persistent[Slice[Byte], Value](map.currentFilePath.getParent, mmap = false, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = false).failed.assertGet shouldBe a[IllegalStateException]

      //drop last byte
      Files.write(map.currentFilePath, allBytes.dropRight(1))
      assertRecover

      //drop file byte
      Files.write(map.currentFilePath, allBytes.drop(1))
      assertRecover

      //shuffled
      Files.write(map.currentFilePath, Random.shuffle(allBytes.toList).toArray)
      assertRecover
    }

    "successfully recover partial data if WAL file is corrupted and when dropCorruptedTailEntries = true" in {
      //recover again with SkipLogOnCorruption, since the last entry is corrupted, the first two entries will still get read
      Files.write(map.currentFilePath, allBytes.dropRight(1))
      val recoveredMap = Map.persistent[Slice[Byte], Value](map.currentFilePath.getParent, mmap = false, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = true).assertGet.item
      (1 to 99) foreach {
        i =>
          recoveredMap.get(i).assertGet._2 shouldBe Value.Put(i)
      }
      recoveredMap.contains(100) shouldBe false

      //if the top entry is corrupted.
      Files.write(recoveredMap.currentFilePath, allBytes.drop(1))
      val recoveredMap2 = Map.persistent[Slice[Byte], Value](recoveredMap.currentFilePath.getParent, mmap = false, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = true).assertGet.item
      recoveredMap2.isEmpty shouldBe true
    }
  }

  "PersistentMap.recovery on corruption when there are two WAL files and the first file is corrupted" should {
    import LevelZeroMapEntryReader._
    import LevelZeroMapEntryWriter._

    val map1 = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
    map1.write(MapEntry.Put(1, Value.Put(1))).assertGet shouldBe true
    map1.write(MapEntry.Put(2, Value.Put(2))).assertGet shouldBe true
    map1.write(MapEntry.Put(3, Value.Put(3))).assertGet shouldBe true

    val map2 = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
    map2.write(MapEntry.Put(4, Value.Put(4))).assertGet shouldBe true
    map2.write(MapEntry.Put(5, Value.Put(5))).assertGet shouldBe true
    map2.write(MapEntry.Put(6, Value.Put(6))).assertGet shouldBe true

    val map2sLogFile = map2.path.resolve(0.toLogFileId)
    val copiedLogFileId = map1.path.resolve(1.toLogFileId)
    //move map2's log file into map1's log file folder named as 1.log.
    Files.copy(map2sLogFile, copiedLogFileId)

    val log0 = map1.path.resolve(0.toLogFileId)
    val log0Bytes = Files.readAllBytes(log0)

    val log1 = map1.path.resolve(1.toLogFileId)
    val log1Bytes = Files.readAllBytes(log1)

    "fail recovery if first map is corrupted" in {
      //corrupt 0.log bytes
      Files.write(log0, log0Bytes.drop(1))
      Map.persistent[Slice[Byte], Value](map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).failed.assertGet shouldBe a[IllegalStateException]
      Files.write(log0, log0Bytes) //fix log0 bytes
    }

    "successfully recover Map by reading both WAL files if the first WAL file is corrupted" in {
      //corrupt 0.log bytes
      Files.write(log0, log0Bytes.dropRight(1))
      val recoveredMapWith0LogCorrupted = Map.persistent[Slice[Byte], Value](map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = true).assertGet
      //recovery state contains failure because the WAL file is partially recovered.
      recoveredMapWith0LogCorrupted.result.failed.assertGet shouldBe a[IllegalStateException]
      recoveredMapWith0LogCorrupted.item.size shouldBe 5 //5 because the 3rd entry in 0.log is corrupted

      //checking the recovered entries
      recoveredMapWith0LogCorrupted.item.get(1).assertGet._2 shouldBe Value.Put(1)
      recoveredMapWith0LogCorrupted.item.get(2).assertGet._2 shouldBe Value.Put(2)
      recoveredMapWith0LogCorrupted.item.get(3) shouldBe empty //since the last byte of 0.log file is corrupted, the last entry is missing
      recoveredMapWith0LogCorrupted.item.get(4).assertGet._2 shouldBe Value.Put(4)
      recoveredMapWith0LogCorrupted.item.get(5).assertGet._2 shouldBe Value.Put(5)
      recoveredMapWith0LogCorrupted.item.get(6).assertGet._2 shouldBe Value.Put(6)
    }
  }

  "PersistentMap.recovery on corruption when there are two WAL files and the second file is corrupted" should {
    import LevelZeroMapEntryReader._
    import LevelZeroMapEntryWriter._

    val map1 = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
    map1.write(MapEntry.Put(1, Value.Put(1))).assertGet shouldBe true
    map1.write(MapEntry.Put(2, Value.Put(2))).assertGet shouldBe true
    map1.write(MapEntry.Put(3, Value.Put(3))).assertGet shouldBe true

    val map2 = Map.persistent[Slice[Byte], Value](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
    map2.write(MapEntry.Put(4, Value.Put(4))).assertGet shouldBe true
    map2.write(MapEntry.Put(5, Value.Put(5))).assertGet shouldBe true
    map2.write(MapEntry.Put(6, Value.Put(6))).assertGet shouldBe true

    val map2sLogFile = map2.path.resolve(0.toLogFileId)
    val copiedLogFileId = map1.path.resolve(1.toLogFileId)
    Files.copy(map2sLogFile, copiedLogFileId)

    val log0 = map1.path.resolve(0.toLogFileId)
    val log0Bytes = Files.readAllBytes(log0)

    val log1 = map1.path.resolve(1.toLogFileId)
    val log1Bytes = Files.readAllBytes(log1)

    "fail recovery if one of two WAL files of the map is corrupted" in {
      //corrupt 1.log bytes
      Files.write(log1, log1Bytes.drop(1))
      Map.persistent[Slice[Byte], Value](map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).failed.assertGet shouldBe a[IllegalStateException]
      Files.write(log1, log1Bytes) //fix log1 bytes
    }

    "successfully recover Map by reading both WAL files if the second WAL file is corrupted" in {
      //corrupt 1.log bytes
      Files.write(log1, log1Bytes.dropRight(1))
      val recoveredMapWith0LogCorrupted = Map.persistent[Slice[Byte], Value](map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = true).assertGet
      //recovery state contains failure because the WAL file is partially recovered.
      recoveredMapWith0LogCorrupted.result.failed.assertGet shouldBe a[IllegalStateException]
      recoveredMapWith0LogCorrupted.item.size shouldBe 5 //5 because the 3rd entry in 1.log is corrupted

      //checking the recovered entries
      recoveredMapWith0LogCorrupted.item.get(1).assertGet._2 shouldBe Value.Put(1)
      recoveredMapWith0LogCorrupted.item.get(2).assertGet._2 shouldBe Value.Put(2)
      recoveredMapWith0LogCorrupted.item.get(3).assertGet._2 shouldBe Value.Put(3)
      recoveredMapWith0LogCorrupted.item.get(4).assertGet._2 shouldBe Value.Put(4)
      recoveredMapWith0LogCorrupted.item.get(5).assertGet._2 shouldBe Value.Put(5)
      recoveredMapWith0LogCorrupted.item.get(6) shouldBe empty
    }
  }
}