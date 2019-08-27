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

package swaydb.core.map

import java.nio.file.{FileAlreadyExistsException, Files, Path}

import org.scalatest.OptionValues._
import swaydb.Error.Segment.ErrorHandler
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.{Memory, Transient, Value}
import swaydb.core.io.file.{BlockCache, DBFile}
import swaydb.core.io.file.IOEffect._
import swaydb.core.level.AppendixSkipListMerger
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.map.serializer._
import swaydb.core.actor.MemorySweeper
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.SegmentIO
import swaydb.core.util.{BlockCacheFileIDGenerator, Extension, SkipList}
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._
import scala.util.Random

class MapSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  implicit def testTimer: TestTimer = TestTimer.Empty

  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeper.Enabled = TestLimitQueues.fileSweeper
  implicit val memorySweeperImplicitSweeper: Option[MemorySweeper.Both] = TestLimitQueues.memorySweeper
  implicit val skipListMerger = LevelZeroSkipListMerger
  implicit def blockCache: Option[BlockCache.State] = TestLimitQueues.randomBlockCache

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  implicit val merger = AppendixSkipListMerger

  implicit def grouping = randomGroupByOption(randomNextInt(1000))

  implicit def segmentIO = SegmentIO.random

  val appendixReader = AppendixMapEntryReader(true, true)

  "Map" should {
    "initialise a memory level0" in {
      import LevelZeroMapEntryWriter._

      val map = Map.memory[Slice[Byte], Memory.SegmentResponse](1.mb, flushOnOverflow = false)

      map.write(MapEntry.Put(1, Memory.put(1, Some(1)))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put(2, Memory.put(2, Some(2)))).runRandomIO.right.value shouldBe true
      map.skipList.get(1).value shouldBe Memory.put(1, Some(1))
      map.skipList.get(2).value shouldBe Memory.put(2, Some(2))

      map.hasRange shouldBe false

      map.write(MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2))).runRandomIO.right.value shouldBe true
      map.skipList.get(1).value shouldBe Memory.remove(1)
      map.skipList.get(2).value shouldBe Memory.remove(2)

      map.hasRange shouldBe false

      map.write(MapEntry.Put[Slice[Byte], Memory.Range](1, Memory.Range(1, 10, None, Value.remove(None)))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Range](11, Memory.Range(11, 20, Some(Value.put(20)), Value.update(20)))).runRandomIO.right.value shouldBe true
      map.skipList.get(1).value shouldBe Memory.Range(1, 10, None, Value.remove(None))
      map.skipList.get(11).value shouldBe Memory.Range(11, 20, Some(Value.put(20)), Value.update(20))

      map.hasRange shouldBe true
    }

    "initialise a memory Appendix map" in {
      import AppendixMapEntryWriter._

      val map = Map.memory[Slice[Byte], Segment](1.mb, flushOnOverflow = false)
      val segment1 = TestSegment().runRandomIO.right.value
      val segment2 = TestSegment().runRandomIO.right.value

      map.hasRange shouldBe false

      map.write(MapEntry.Put[Slice[Byte], Segment](1, segment1)).right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Segment](2, segment2)).right.value shouldBe true
      map.skipList.get(1).value shouldBe segment1
      map.skipList.get(2).value shouldBe segment2

      map.hasRange shouldBe false

      map.write(MapEntry.Remove[Slice[Byte]](1)).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Remove[Slice[Byte]](2)).runRandomIO.right.value shouldBe true
      map.skipList.get(1) shouldBe empty
      map.skipList.get(2) shouldBe empty
    }

    "initialise a persistent Level0 map and recover from it when it's empty" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map.isEmpty shouldBe true
      map.close().runRandomIO.right.value
      //recover from an empty map
      val recovered = Map.persistent[Slice[Byte], Memory.SegmentResponse](map.path, mmap = true, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      recovered.isEmpty shouldBe true
      recovered.close().runRandomIO.right.value
    }

    "initialise a persistent Appendix map and recover from it when it's empty" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val map = Map.persistent[Slice[Byte], Segment](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map.isEmpty shouldBe true
      map.close().runRandomIO.right.value
      //recover from an empty map
      val recovered = Map.persistent[Slice[Byte], Segment](map.path, mmap = true, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      recovered.isEmpty shouldBe true
      recovered.close().runRandomIO.right.value
    }

    "initialise a persistent Level0 map and recover from it when it's contains data" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map.write(MapEntry.Put[Slice[Byte], Memory.Put](1, Memory.put(1, Some(1)))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Put](2, Memory.put(2, Some(2)))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 20, None, Value.update(20)))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 15, None, Value.remove(None)))).runRandomIO.right.value shouldBe true

      map.skipList.get(1).value shouldBe Memory.put(1, Some(1))
      map.skipList.get(2).value shouldBe Memory.remove(2)
      map.skipList.get(10).value shouldBe Memory.Range(10, 15, None, Value.remove(None))
      map.skipList.get(15).value shouldBe Memory.Range(15, 20, None, Value.update(20))

      def doRecover(path: Path): PersistentMap[Slice[Byte], Memory.SegmentResponse] = {
        val recovered = Map.persistent[Slice[Byte], Memory.SegmentResponse](map.path, mmap = Random.nextBoolean(), flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
        recovered.skipList.get(1).value shouldBe Memory.put(1, Some(1))
        recovered.skipList.get(2).value shouldBe Memory.remove(2)
        recovered.skipList.get(10).value shouldBe Memory.Range(10, 15, None, Value.remove(None))
        recovered.skipList.get(15).value shouldBe Memory.Range(15, 20, None, Value.update(20))
        recovered.hasRange shouldBe true
        recovered.close().runRandomIO.right.value
        recovered
      }

      //recover maps 10 times
      (1 to 10).foldLeft(map) {
        case (map, _) =>
          doRecover(map.path)
      }
      map.close().runRandomIO.right.value
    }

    "initialise a persistent Appendix map and recover from it when it's contains data" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val segment1 = TestSegment(Slice(Transient.put(1, Some(1), None), Transient.put(2, Some(2), None)).updateStats).runRandomIO.right.value
      val segment2 = TestSegment(Slice(Transient.put(3, Some(3), None), Transient.put(4, Some(4), None)).updateStats).runRandomIO.right.value

      val map = Map.persistent[Slice[Byte], Segment](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map.write(MapEntry.Put[Slice[Byte], Segment](1, segment1)).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Segment](2, segment2)).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Remove[Slice[Byte]](2)).runRandomIO.right.value shouldBe true
      map.skipList.get(1).value shouldBe segment1
      map.skipList.get(2) shouldBe empty

      def doRecover(path: Path): PersistentMap[Slice[Byte], Segment] = {
        val recovered = Map.persistent[Slice[Byte], Segment](map.path, mmap = Random.nextBoolean(), flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
        recovered.skipList.get(1).value shouldBe segment1
        recovered.skipList.get(2) shouldBe empty
        recovered.close().runRandomIO.right.value
        recovered
      }

      //recover and maps 10 times
      (1 to 10).foldLeft(map) {
        case (map, _) =>
          doRecover(map.path)
      }
      map.close().runRandomIO.right.value
    }

    "initialise a Map that has two persistent Level0 map files (second file did not value deleted due to early JVM termination)" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map1 = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map1.write(MapEntry.Put(1, Memory.put(1, Some(1)))).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put(2, Memory.put(2, Some(2)))).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put(3, Memory.put(3, Some(3)))).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 20, None, Value.update(20)))).runRandomIO.right.value shouldBe true

      val map2 = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map2.write(MapEntry.Put(4, Memory.put(4, Some(4)))).runRandomIO.right.value shouldBe true
      map2.write(MapEntry.Put(5, Memory.put(5, Some(5)))).runRandomIO.right.value shouldBe true
      map2.write(MapEntry.Put(2, Memory.put(2, Some(22)))).runRandomIO.right.value shouldBe true //second file will override 2's value to be 22
      map2.write(MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 15, None, Value.remove(None)))).runRandomIO.right.value shouldBe true

      //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
      val map2sLogFile = map2.path.resolve(0.toLogFileId)
      Files.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

      //recover map 1 and it should contain all entries of map1 and map2
      val map1Recovered = Map.persistent[Slice[Byte], Memory.SegmentResponse](map1.path, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map1Recovered.skipList.get(1).value shouldBe Memory.put(1, Some(1))
      map1Recovered.skipList.get(2).value shouldBe Memory.put(2, Some(22)) //second file overrides 2's value to be 22
      map1Recovered.skipList.get(3).value shouldBe Memory.put(3, Some(3))
      map1Recovered.skipList.get(4).value shouldBe Memory.put(4, Some(4))
      map1Recovered.skipList.get(5).value shouldBe Memory.put(5, Some(5))
      map1Recovered.skipList.get(6) shouldBe empty
      map1Recovered.skipList.get(10).value shouldBe Memory.Range(10, 15, None, Value.remove(None))
      map1Recovered.skipList.get(15).value shouldBe Memory.Range(15, 20, None, Value.update(20))

      //recovered file's id is 2.log
      map1Recovered.path.files(Extension.Log).map(_.fileId.runRandomIO.right.value) should contain only ((2, Extension.Log))

      map1.close().runRandomIO.right.value
      map2.close().runRandomIO.right.value
      map1Recovered.close().runRandomIO.right.value
    }

    "initialise a Map that has two persistent Appendix map files (second file did not value deleted due to early JVM termination)" in {
      import AppendixMapEntryWriter._
      import appendixReader._

      val segment1 = TestSegment(Slice(Transient.put(1, Some(1), None), Transient.put(2, Some(2), None)).updateStats).runRandomIO.right.value
      val segment2 = TestSegment(Slice(Transient.put(2, Some(2), None), Transient.put(4, Some(4), None)).updateStats).runRandomIO.right.value
      val segment3 = TestSegment(Slice(Transient.put(3, Some(3), None), Transient.put(6, Some(6), None)).updateStats).runRandomIO.right.value
      val segment4 = TestSegment(Slice(Transient.put(4, Some(4), None), Transient.put(8, Some(8), None)).updateStats).runRandomIO.right.value
      val segment5 = TestSegment(Slice(Transient.put(5, Some(5), None), Transient.put(10, Some(10), None)).updateStats).runRandomIO.right.value
      val segment2Updated = TestSegment(Slice(Transient.put(2, Some(2), None), Transient.put(12, Some(12), None)).updateStats).runRandomIO.right.value

      val map1 = Map.persistent[Slice[Byte], Segment](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map1.write(MapEntry.Put(1, segment1)).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put(2, segment2)).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put(3, segment3)).runRandomIO.right.value shouldBe true

      val map2 = Map.persistent[Slice[Byte], Segment](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map2.write(MapEntry.Put(4, segment4)).runRandomIO.right.value shouldBe true
      map2.write(MapEntry.Put(5, segment5)).runRandomIO.right.value shouldBe true
      map2.write(MapEntry.Put(2, segment2Updated)).runRandomIO.right.value shouldBe true //second file will override 2's value to be segment2Updated

      //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
      val map2sLogFile = map2.path.resolve(0.toLogFileId)
      Files.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

      //recover map 1 and it should contain all entries of map1 and map2
      val map1Recovered = Map.persistent[Slice[Byte], Segment](map1.path, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map1Recovered.skipList.get(1).value shouldBe segment1
      map1Recovered.skipList.get(2).value shouldBe segment2Updated //second file overrides 2's value to be segment2Updated
      map1Recovered.skipList.get(3).value shouldBe segment3
      map1Recovered.skipList.get(4).value shouldBe segment4
      map1Recovered.skipList.get(5).value shouldBe segment5
      map1Recovered.skipList.get(6) shouldBe empty

      //recovered file's id is 2.log
      map1Recovered.path.files(Extension.Log).map(_.fileId.runRandomIO.right.value) should contain only ((2, Extension.Log))

      map1.close().runRandomIO.right.value
      map2.close().runRandomIO.right.value
      map1Recovered.close().runRandomIO.right.value
      segment1.close.runRandomIO.right.value
      segment2.close.runRandomIO.right.value
      segment3.close.runRandomIO.right.value
      segment4.close.runRandomIO.right.value
      segment5.close.runRandomIO.right.value
      segment2Updated.close.runRandomIO.right.value
    }

    "fail initialise if the Map exists but recovery is not provided" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map = Map.persistent[Slice[Byte], Memory.SegmentResponse](
        folder = createRandomDir,
        mmap = false,
        flushOnOverflow = false,
        fileSize = 1.mb,
        initialWriteCount = 0
      ).runRandomIO.right.value

      //fails because the file already exists.
      Map.persistent[Slice[Byte], Memory.SegmentResponse](
        folder = map.path,
        mmap = false,
        flushOnOverflow = false,
        initialWriteCount = 0,
        fileSize = 1.mb
      ).left.runRandomIO.right.value.exception shouldBe a[FileAlreadyExistsException]

      //recovers because the recovery is provided
      Map.persistent[Slice[Byte], Memory.SegmentResponse](map.path, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value

      map.close().runRandomIO.right.value
    }
  }

  "PersistentMap.recover" should {
    "recover from an empty PersistentMap folder" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val skipList = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
      val file = PersistentMap.recover[Slice[Byte], Memory.SegmentResponse](createRandomDir, false, 4.mb, skipList, dropCorruptedTailEntries = false).runRandomIO.right.value._1.item

      file.isOpen shouldBe true
      file.isMemoryMapped.runRandomIO.right.value shouldBe false
      file.existsOnDisk shouldBe true
      file.fileSize.runRandomIO.right.value shouldBe 0
      file.path.fileId.runRandomIO.right.value shouldBe(0, Extension.Log)

      skipList.isEmpty shouldBe true
      file.close.runRandomIO.right.value
    }

    "recover from an existing PersistentMap folder" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      //create a map
      val map = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = true, flushOnOverflow = false, fileSize = 4.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map.write(MapEntry.Put(1, Memory.put(1, 1))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put(3, Memory.put(3, 3))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put(3, Memory.put(3, 3))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 20, None, Value.update(20)))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 15, None, Value.remove(None)))).runRandomIO.right.value shouldBe true

      map.currentFilePath.fileId.runRandomIO.right.value shouldBe(0, Extension.Log)

      map.hasRange shouldBe true

      val skipList = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
      val recoveredFile = PersistentMap.recover(map.path, false, 4.mb, skipList, dropCorruptedTailEntries = false).runRandomIO.right.value._1.item

      recoveredFile.isOpen shouldBe true
      recoveredFile.isMemoryMapped.runRandomIO.right.value shouldBe false
      recoveredFile.existsOnDisk shouldBe true
      recoveredFile.path.fileId.runRandomIO.right.value shouldBe(1, Extension.Log) //file id gets incremented on recover
      recoveredFile.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

      skipList.isEmpty shouldBe false
      skipList.get(1: Slice[Byte]).value shouldBe Memory.put(1, 1)
      skipList.get(2: Slice[Byte]).value shouldBe Memory.remove(2)
      skipList.get(3: Slice[Byte]).value shouldBe Memory.put(3, 3)
      skipList.get(10: Slice[Byte]).value shouldBe Memory.Range(10, 15, None, Value.remove(None))
      skipList.get(15: Slice[Byte]).value shouldBe Memory.Range(15, 20, None, Value.update(20))
    }

    "recover from an existing PersistentMap folder when flushOnOverflow is true" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      //create a map
      val map = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = true, flushOnOverflow = true, fileSize = 1.byte, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item

      map.write(MapEntry.Put(1, Memory.put(1, 1))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put(3, Memory.put(3, 3))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 20, None, Value.update(20)))).runRandomIO.right.value shouldBe true
      map.write(MapEntry.Put[Slice[Byte], Memory.Range](10, Memory.Range(10, 15, None, Value.remove(None)))).runRandomIO.right.value shouldBe true

      map.currentFilePath.fileId.runRandomIO.right.value shouldBe(5, Extension.Log)
      map.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted
      map.path.resolveSibling(1.toLogFileId).exists shouldBe false //1.log gets deleted
      map.path.resolveSibling(2.toLogFileId).exists shouldBe false //2.log gets deleted
      map.path.resolveSibling(3.toLogFileId).exists shouldBe false //3.log gets deleted
      map.path.resolveSibling(4.toLogFileId).exists shouldBe false //4.log gets deleted

      //reopen file
      val skipList = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
      val recoveredFile = PersistentMap.recover(map.path, true, 1.byte, skipList, dropCorruptedTailEntries = false).runRandomIO.right.value._1.item
      recoveredFile.isOpen shouldBe true
      recoveredFile.isMemoryMapped.runRandomIO.right.value shouldBe true
      recoveredFile.existsOnDisk shouldBe true
      recoveredFile.path.fileId.runRandomIO.right.value shouldBe(6, Extension.Log) //file id gets incremented on recover
      recoveredFile.path.resolveSibling(5.toLogFileId).exists shouldBe false //5.log gets deleted

      skipList.isEmpty shouldBe false
      skipList.get(1: Slice[Byte]).value shouldBe Memory.put(1, 1)
      skipList.get(2: Slice[Byte]).value shouldBe Memory.remove(2)
      skipList.get(3: Slice[Byte]).value shouldBe Memory.put(3, 3)
      skipList.get(10: Slice[Byte]).value shouldBe Memory.Range(10, 15, None, Value.remove(None))
      skipList.get(15: Slice[Byte]).value shouldBe Memory.Range(15, 20, None, Value.update(20))

      //reopen the recovered file
      val skipList2 = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
      val recoveredFile2 = PersistentMap.recover(map.path, true, 1.byte, skipList2, dropCorruptedTailEntries = false).runRandomIO.right.value._1.item
      recoveredFile2.isOpen shouldBe true
      recoveredFile2.isMemoryMapped.runRandomIO.right.value shouldBe true
      recoveredFile2.existsOnDisk shouldBe true
      recoveredFile2.path.fileId.runRandomIO.right.value shouldBe(7, Extension.Log) //file id gets incremented on recover
      recoveredFile2.path.resolveSibling(6.toLogFileId).exists shouldBe false //6.log gets deleted

      skipList2.isEmpty shouldBe false
      skipList2.get(1: Slice[Byte]).value shouldBe Memory.put(1, 1)
      skipList2.get(2: Slice[Byte]).value shouldBe Memory.remove(2)
      skipList2.get(3: Slice[Byte]).value shouldBe Memory.put(3, 3)
      skipList2.get(10: Slice[Byte]).value shouldBe Memory.Range(10, 15, None, Value.remove(None))
      skipList2.get(15: Slice[Byte]).value shouldBe Memory.Range(15, 20, None, Value.update(20))

      map.close().runRandomIO.right.value
      recoveredFile.close.runRandomIO.right.value
      recoveredFile2.close.runRandomIO.right.value
    }

    "recover from an existing PersistentMap folder with empty memory map" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      //create a map
      val map = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = true, flushOnOverflow = false, fileSize = 4.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map.currentFilePath.fileId.runRandomIO.right.value shouldBe(0, Extension.Log)
      map.close().runRandomIO.right.value

      val skipList = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
      val file = PersistentMap.recover(map.path, false, 4.mb, skipList, dropCorruptedTailEntries = false).runRandomIO.right.value._1.item

      file.isOpen shouldBe true
      file.isMemoryMapped.runRandomIO.right.value shouldBe false
      file.existsOnDisk shouldBe true
      file.path.fileId.runRandomIO.right.value shouldBe(1, Extension.Log) //file id gets incremented on recover
      file.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

      skipList.isEmpty shouldBe true

      file.close.runRandomIO.right.value
    }
  }

  "PersistentMap.nextFile" should {
    "creates a new file from the current file" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val skipList = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
      skipList.put(1, Memory.put(1, 1))
      skipList.put(2, Memory.put(2, 2))
      skipList.put(3, Memory.remove(3))
      skipList.put(10, Memory.Range(10, 15, None, Value.remove(None)))
      skipList.put(15, Memory.Range(15, 20, Some(Value.put(15)), Value.update(14)))

      val currentFile = PersistentMap.recover(createRandomDir, false, 4.mb, skipList, dropCorruptedTailEntries = false).runRandomIO.right.value._1.item
      val nextFile = PersistentMap.nextFile(currentFile, false, 4.mb, skipList).runRandomIO.right.value

      val nextFileSkipList = SkipList.concurrent[Slice[Byte], Memory.SegmentResponse]()(keyOrder)
      val nextFileBytes = DBFile.channelRead(nextFile.path, randomIOStrategy(), autoClose = false, blockCacheFileId = BlockCacheFileIDGenerator.nextID).runRandomIO.right.value.readAll.runRandomIO.right.value
      val mapEntries = MapCodec.read(nextFileBytes, dropCorruptedTailEntries = false).runRandomIO.right.value.item.value
      mapEntries applyTo nextFileSkipList

      nextFileSkipList.get(1: Slice[Byte]).value shouldBe Memory.put(1, 1)
      nextFileSkipList.get(2: Slice[Byte]).value shouldBe Memory.put(2, 2)
      nextFileSkipList.get(3: Slice[Byte]).value shouldBe Memory.remove(3)
      nextFileSkipList.get(10: Slice[Byte]).value shouldBe Memory.Range(10, 15, None, Value.remove(None))
      nextFileSkipList.get(15: Slice[Byte]).value shouldBe Memory.Range(15, 20, Some(Value.put(15)), Value.update(14))

      currentFile.close.runRandomIO.right.value
      nextFile.close.runRandomIO.right.value
    }
  }

  "PersistentMap.recovery on corruption" should {
    import LevelZeroMapEntryReader._
    import LevelZeroMapEntryWriter._

    "fail if the WAL file is corrupted and and when dropCorruptedTailEntries = false" in {
      val map = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, fileSize = 4.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      (1 to 100) foreach {
        i =>
          map.write(MapEntry.Put(i, Memory.put(i, i))).runRandomIO.right.value shouldBe true
      }
      map.size shouldBe 100
      val allBytes = Files.readAllBytes(map.currentFilePath)

      def assertRecover =
        Map.persistent[Slice[Byte], Memory.SegmentResponse](
          folder = map.currentFilePath.getParent,
          mmap = false,
          flushOnOverflow = false,
          fileSize = 4.mb,
          initialWriteCount = 0,
          dropCorruptedTailEntries = false
        ).left.runRandomIO.right.value.exception shouldBe a[IllegalStateException]

      //drop last byte
      Files.write(map.currentFilePath, allBytes.dropRight(1))
      assertRecover

      //drop first byte
      Files.write(map.currentFilePath, allBytes.drop(1))
      assertRecover
    }

    "successfully recover partial data if WAL file is corrupted and when dropCorruptedTailEntries = true" in {
      val map = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, fileSize = 4.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      (1 to 100) foreach {
        i =>
          map.write(MapEntry.Put(i, Memory.put(i, i))).runRandomIO.right.value shouldBe true
      }
      map.size shouldBe 100
      val allBytes = Files.readAllBytes(map.currentFilePath)

      //recover again with SkipLogOnCorruption, since the last entry is corrupted, the first two entries will still value read
      Files.write(map.currentFilePath, allBytes.dropRight(1))
      val recoveredMap = Map.persistent[Slice[Byte], Memory.SegmentResponse](map.currentFilePath.getParent, mmap = false, flushOnOverflow = false, fileSize = 4.mb, initialWriteCount = 0, dropCorruptedTailEntries = true).runRandomIO.right.value.item
      (1 to 99) foreach {
        i =>
          recoveredMap.skipList.get(i).value shouldBe Memory.put(i, i)
      }
      recoveredMap.skipList.contains(100) shouldBe false

      //if the top entry is corrupted.
      Files.write(recoveredMap.currentFilePath, allBytes.drop(1))
      val recoveredMap2 = Map.persistent[Slice[Byte], Memory.SegmentResponse](recoveredMap.currentFilePath.getParent, mmap = false, flushOnOverflow = false, fileSize = 4.mb, initialWriteCount = 0, dropCorruptedTailEntries = true).runRandomIO.right.value.item
      recoveredMap2.isEmpty shouldBe true
    }
  }

  "PersistentMap.recovery on corruption" when {
    "there are two WAL files and the first file is corrupted" in {
      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map1 = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map1.write(MapEntry.Put(1, Memory.put(1, 1))).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put(2, Memory.put(2, 2))).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put(3, Memory.put(3, 3))).runRandomIO.right.value shouldBe true

      val map2 = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map2.write(MapEntry.Put(4, Memory.put(4, 4))).runRandomIO.right.value shouldBe true
      map2.write(MapEntry.Put(5, Memory.put(5, 5))).runRandomIO.right.value shouldBe true
      map2.write(MapEntry.Put(6, Memory.put(6, 6))).runRandomIO.right.value shouldBe true

      val map2sLogFile = map2.path.resolve(0.toLogFileId)
      val copiedLogFileId = map1.path.resolve(1.toLogFileId)
      //move map2's log file into map1's log file folder named as 1.log.
      Files.copy(map2sLogFile, copiedLogFileId)

      val log0 = map1.path.resolve(0.toLogFileId)
      val log0Bytes = Files.readAllBytes(log0)

      val log1 = map1.path.resolve(1.toLogFileId)
      val log1Bytes = Files.readAllBytes(log1)

      //fail recovery if first map is corrupted
      //corrupt 0.log bytes
      Files.write(log0, log0Bytes.drop(1))
      Map.persistent[Slice[Byte], Memory.SegmentResponse](map1.path, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).left.runRandomIO.right.value.exception shouldBe a[IllegalStateException]
      Files.write(log0, log0Bytes) //fix log0 bytes

      //successfully recover Map by reading both WAL files if the first WAL file is corrupted
      //corrupt 0.log bytes
      Files.write(log0, log0Bytes.dropRight(1))
      val recoveredMapWith0LogCorrupted = Map.persistent[Slice[Byte], Memory.SegmentResponse](map1.path, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = true).runRandomIO.right.value
      //recovery state contains failure because the WAL file is partially recovered.
      recoveredMapWith0LogCorrupted.result.left.runRandomIO.right.value.exception shouldBe a[IllegalStateException]
      recoveredMapWith0LogCorrupted.item.size shouldBe 5 //5 because the 3rd entry in 0.log is corrupted

      //checking the recovered entries
      recoveredMapWith0LogCorrupted.item.skipList.get(1).value shouldBe Memory.put(1, 1)
      recoveredMapWith0LogCorrupted.item.skipList.get(2).value shouldBe Memory.put(2, 2)
      recoveredMapWith0LogCorrupted.item.skipList.get(3) shouldBe empty //since the last byte of 0.log file is corrupted, the last entry is missing
      recoveredMapWith0LogCorrupted.item.skipList.get(4).value shouldBe Memory.put(4, 4)
      recoveredMapWith0LogCorrupted.item.skipList.get(5).value shouldBe Memory.put(5, 5)
      recoveredMapWith0LogCorrupted.item.skipList.get(6).value shouldBe Memory.put(6, 6)
    }
  }

  "PersistentMap.recovery on corruption" when {
    "there are two WAL files and the second file is corrupted" in {

      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      val map1 = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map1.write(MapEntry.Put(1, Memory.put(1, 1))).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put(2, Memory.put(2))).runRandomIO.right.value shouldBe true
      map1.write(MapEntry.Put(3, Memory.put(3, 3))).runRandomIO.right.value shouldBe true

      val map2 = Map.persistent[Slice[Byte], Memory.SegmentResponse](createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).runRandomIO.right.value.item
      map2.write(MapEntry.Put(4, Memory.put(4, 4))).runRandomIO.right.value shouldBe true
      map2.write(MapEntry.Put(5, Memory.put(5, 5))).runRandomIO.right.value shouldBe true
      map2.write(MapEntry.Put(6, Memory.put(6, 6))).runRandomIO.right.value shouldBe true

      val map2sLogFile = map2.path.resolve(0.toLogFileId)
      val copiedLogFileId = map1.path.resolve(1.toLogFileId)
      Files.copy(map2sLogFile, copiedLogFileId)

      val log0 = map1.path.resolve(0.toLogFileId)
      val log0Bytes = Files.readAllBytes(log0)

      val log1 = map1.path.resolve(1.toLogFileId)
      val log1Bytes = Files.readAllBytes(log1)

      //fail recovery if one of two WAL files of the map is corrupted
      //corrupt 1.log bytes
      Files.write(log1, log1Bytes.drop(1))
      Map.persistent[Slice[Byte], Memory.SegmentResponse](map1.path, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = false).left.runRandomIO.right.value.exception shouldBe a[IllegalStateException]
      Files.write(log1, log1Bytes) //fix log1 bytes

      //successfully recover Map by reading both WAL files if the second WAL file is corrupted
      //corrupt 1.log bytes
      Files.write(log1, log1Bytes.dropRight(1))
      val recoveredMapWith0LogCorrupted = Map.persistent[Slice[Byte], Memory.SegmentResponse](map1.path, mmap = false, flushOnOverflow = false, 1.mb, initialWriteCount = 0, dropCorruptedTailEntries = true).runRandomIO.right.value
      //recovery state contains failure because the WAL file is partially recovered.
      recoveredMapWith0LogCorrupted.result.left.runRandomIO.right.value.exception shouldBe a[IllegalStateException]
      recoveredMapWith0LogCorrupted.item.size shouldBe 5 //5 because the 3rd entry in 1.log is corrupted

      //checking the recovered entries
      recoveredMapWith0LogCorrupted.item.skipList.get(1).value shouldBe Memory.put(1, 1)
      recoveredMapWith0LogCorrupted.item.skipList.get(2).value shouldBe Memory.put(2)
      recoveredMapWith0LogCorrupted.item.skipList.get(3).value shouldBe Memory.put(3, 3)
      recoveredMapWith0LogCorrupted.item.skipList.get(4).value shouldBe Memory.put(4, 4)
      recoveredMapWith0LogCorrupted.item.skipList.get(5).value shouldBe Memory.put(5, 5)
      recoveredMapWith0LogCorrupted.item.skipList.get(6) shouldBe empty
    }
  }

  "Randomly inserting data into Map and recovering the Map" should {
    "result in the recovered Map to have the same skipList as the Map before recovery" in {

      import LevelZeroMapEntryReader._
      import LevelZeroMapEntryWriter._

      //run this test multiple times to randomly generate multiple combinations of overlapping key-value with optionally & randomly added Put, Remove, Range or Update.
      (1 to 100) foreach {
        _ =>
          //create a Map with randomly max size so that this test also covers when multiple maps are created. Also set flushOnOverflow to true so that the same Map gets written.
          val map =
            Map.persistent[Slice[Byte], Memory.SegmentResponse](
              folder = createRandomDir,
              mmap = Random.nextBoolean(),
              flushOnOverflow = true,
              fileSize = randomIntMax(1.mb),
              initialWriteCount = 0,
              dropCorruptedTailEntries = false
            ).runRandomIO.right.value.item

          //randomly create 100 key-values to insert into the Map. These key-values may contain range, update, or key-values deadlines randomly.
          val keyValues = randomizedKeyValues(1000, addPut = true, addGroups = false).toMemory
          //slice write them to that if map's randomly selected size is too small and multiple maps are written to.
          keyValues.groupedSlice(5) foreach {
            keyValues =>
              map.write(keyValues.toMapEntry.value).runRandomIO.right.value shouldBe true
          }
          map.skipList.values().asScala shouldBe keyValues

          //write overlapping key-values to the same map which are randomly selected and may or may not contain range, update, or key-values deadlines.
          val updatedValues = randomizedKeyValues(1000, startId = Some(keyValues.head.key.readInt()), addPut = true, addGroups = false)
          val updatedEntries = updatedValues.toMapEntry.value
          map.write(updatedEntries).runRandomIO.right.value shouldBe true

          //reopening the map should return in the original skipList.
          val reopened = map.reopen
          reopened.skipList.size shouldBe map.skipList.size
          reopened.skipList.asScala shouldBe map.skipList.asScala
          reopened.delete.runRandomIO.right.value
      }
    }
  }
}
