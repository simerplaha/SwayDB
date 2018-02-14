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

import java.nio.file.{FileAlreadyExistsException, Files}
import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.TestBase
import swaydb.core.data.ValueType
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.map.serializer.{Level0KeyValuesSerializer, MapCodec}
import swaydb.core.util.Extension
import swaydb.core.util.FileUtil._
import swaydb.data.config.RecoveryMode
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class MapSpec extends TestBase {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  implicit val serializer = Level0KeyValuesSerializer(ordering)

  def test(map: Map[Slice[Byte], (ValueType, Option[Slice[Byte]])]) = {
    map.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true
    map.add(2, (ValueType.Add, Some(2))).assertGet shouldBe true
    map.get(1).assertGet._2 shouldBe ((ValueType.Add, Some(1)))
    map.get(2).assertGet._2 shouldBe ((ValueType.Add, Some(2)))

    map.add(1, (ValueType.Remove, Some(1))).assertGet shouldBe true
    map.add(2, (ValueType.Remove, Some(2))).assertGet shouldBe true
    map.get(1).assertGet._2 shouldBe ((ValueType.Remove, Some(1)))
    map.get(2).assertGet._2 shouldBe ((ValueType.Remove, Some(2)))

    map.remove(2).assertGet
    map.get(2).isEmpty shouldBe true
    map.isEmpty shouldBe false

    map.remove(1).assertGet
    map.get(1).isEmpty shouldBe true
    map.isEmpty shouldBe true
  }

  "Map" should {
    "initialise and recover from an already existing empty map" in {
      val map = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      //recover from an empty map
      test(Map.persistent(map.path, mmap = true, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item)
    }

    "initialise a map with data and recover" in {
      val map = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      test(map)
      //re-open
      test(Map.persistent(map.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item)
      //re-open as mmap
      test(Map.persistent(map.path, mmap = true, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item)
      //re-read
      //in-memory
      test(Map.memory(1.mb, flushOnOverflow = false))
    }

    "initialise a Map that has two persistent files (second file did not get deleted due to early JVM termination)" in {
      val map1 = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map1.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true
      map1.add(2, (ValueType.Add, Some(2))).assertGet shouldBe true
      map1.add(3, (ValueType.Add, Some(3))).assertGet shouldBe true

      val map2 = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map2.add(4, (ValueType.Add, Some(4))).assertGet shouldBe true
      map2.add(5, (ValueType.Add, Some(5))).assertGet shouldBe true
      map2.add(6, (ValueType.Add, Some(6))).assertGet shouldBe true

      //move map2's log file into map1's log file folder named as 1.log and reboot to test recovery.
      val map2sLogFile = map2.path.resolve(0.toLogFileId)
      Files.copy(map2sLogFile, map1.path.resolve(1.toLogFileId))

      //recover map 1 and it should contain all entries of map1 and map2
      val map1Recovered = Map.persistent(map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
      map1Recovered.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true
      map1Recovered.add(2, (ValueType.Add, Some(2))).assertGet shouldBe true
      map1Recovered.add(3, (ValueType.Add, Some(3))).assertGet shouldBe true
      map1Recovered.add(4, (ValueType.Add, Some(4))).assertGet shouldBe true
      map1Recovered.add(5, (ValueType.Add, Some(5))).assertGet shouldBe true
      map1Recovered.add(6, (ValueType.Add, Some(6))).assertGet shouldBe true

      //recovered file's id is 2.log
      map1Recovered.path.files(Extension.Log).map(_.fileId.assertGet) should contain only ((2, Extension.Log))
    }

    "fail initialise if the Map exists but recovery is not provided" in {
      val map = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb).assertGet

      //fails because the file already exists.
      Map.persistent(map.path, mmap = false, flushOnOverflow = false, 1.mb).failed.assertGet shouldBe a[FileAlreadyExistsException]

      //recovers because the recovery is provided
      Map.persistent(map.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet
    }
  }

  "PersistentMap.add and remove" should {
    "add and remove key values" in {
      test(Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.persistent(createRandomDir, mmap = true, flushOnOverflow = false, fileSize = 1.mb, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.memory(1.mb, flushOnOverflow = false))
    }

    "return true if entry is too large for the map and flushOnOverflow is false" in {
      def test(map: Map[Slice[Byte], (ValueType, Option[Slice[Byte]])]) = {
        //since this is the first entry and it's byte size is 33.bytes, this entry will get added to the map.
        map.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true

        //now map is not empty but the next entry is too large. This will return false
        map.add(2, (ValueType.Add, Some(2))).assertGet shouldBe false

        map.get(1).assertGet._2 shouldBe ((ValueType.Add, Some(1)))
        map.get(2).isEmpty shouldBe true //second entry does not exists as it was too large for the map.
        map
      }

      //file size is 33.byte which is too small so every write entry require flushing.
      val path1 = createRandomDir
      test(Map.persistent(path1, mmap = false, flushOnOverflow = false, 33.byte, dropCorruptedTailEntries = false).assertGet.item)
      //re-open as mmap
      test(Map.persistent(path1, mmap = true, flushOnOverflow = false, 33.byte, dropCorruptedTailEntries = false).assertGet.item)
      //
      val path2 = createRandomDir
      test(Map.persistent(path2, mmap = true, flushOnOverflow = false, 33.byte, dropCorruptedTailEntries = false).assertGet.item)
      //re-open
      test(Map.persistent(path2, mmap = false, flushOnOverflow = false, 33.byte, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.memory(33.byte, flushOnOverflow = false))
      //test when every entry is smaller then the map size. Map size is 1.byte so
      test(Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.byte, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.persistent(createRandomDir, mmap = true, flushOnOverflow = false, 1.byte, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.memory(1.byte, flushOnOverflow = false))
    }

    "flush the map and write new entry if the new entry is too large for the map and flushOnOverflow is true" in {
      def test(map: Map[Slice[Byte], (ValueType, Option[Slice[Byte]])]) = {
        //since this is the first entry and it's byte size is 33.bytes, this entry will get added to the map.
        map.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true

        //now map is not empty but the next entry is too large. But since flushOnOverFlow is true, this will
        //successfully get written
        map.add(2, (ValueType.Remove, Some(2))).assertGet shouldBe true

        map.get(1).assertGet._2 shouldBe ((ValueType.Add, Some(1)))
        map.get(2).assertGet._2 shouldBe ((ValueType.Remove, Some(2)))
        map
      }

      val path1 = createRandomDir
      val path2 = createRandomDir
      //path1
      //file size is 1.byte which is too small so every write entry require flushing.
      test(Map.persistent(path1, mmap = false, flushOnOverflow = true, 1.byte, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.persistent(path1, mmap = false, flushOnOverflow = true, 1.byte, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.persistent(path1, mmap = true, flushOnOverflow = true, 1.byte, dropCorruptedTailEntries = false).assertGet.item)
      //path2
      test(Map.persistent(path2, mmap = true, flushOnOverflow = true, 1.byte, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.persistent(path2, mmap = true, flushOnOverflow = true, 1.byte, dropCorruptedTailEntries = false).assertGet.item)
      test(Map.persistent(path2, mmap = false, flushOnOverflow = true, 1.byte, dropCorruptedTailEntries = false).assertGet.item)
      //memory
      test(Map.memory(1.byte, flushOnOverflow = true))
    }
  }

  "PersistentMap.recover" should {
    "recover from an empty PersistentMap folder" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])]()
      val file = PersistentMap.recover(createRandomDir, false, 4.mb, skipList, dropCorruptedTailEntries = false).assertGet.item

      file.isOpen shouldBe true
      file.isMemoryMapped.assertGet shouldBe false
      file.existsOnDisk shouldBe true
      file.fileSize.assertGet shouldBe 0
      file.path.fileId.assertGet shouldBe(0, Extension.Log)

      skipList.isEmpty shouldBe true
    }

    "recover from an existing PersistentMap folder" in {
      //create a map
      val map = Map.persistent(createRandomDir, mmap = true, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = false).assertGet.item
      map.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true
      map.add(2, (ValueType.Remove, Some(2))).assertGet shouldBe true
      map.add(3, (ValueType.Add, Some(3))).assertGet shouldBe true
      map.currentFilePath.fileId.assertGet shouldBe(0, Extension.Log)

      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      val recoveredFile = PersistentMap.recover(map.path, false, 4.mb, skipList, dropCorruptedTailEntries = false).assertGet.item

      recoveredFile.isOpen shouldBe true
      recoveredFile.isMemoryMapped.assertGet shouldBe false
      recoveredFile.existsOnDisk shouldBe true
      recoveredFile.path.fileId.assertGet shouldBe(1, Extension.Log) //file id gets incremented on recover
      recoveredFile.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

      skipList.isEmpty shouldBe false
      skipList.get(1: Slice[Byte]) shouldBe ((ValueType.Add, Some(1)))
      skipList.get(2: Slice[Byte]) shouldBe ((ValueType.Remove, Some(2)))
      skipList.get(3: Slice[Byte]) shouldBe ((ValueType.Add, Some(3)))
    }

    "recover from an existing PersistentMap folder when flushOnOverflow is true" in {
      //create a map
      val map = Map.persistent(createRandomDir, mmap = true, flushOnOverflow = true, fileSize = 1.byte, dropCorruptedTailEntries = false).assertGet.item
      map.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true
      map.add(2, (ValueType.Remove, Some(2))).assertGet shouldBe true
      map.add(3, (ValueType.Add, Some(3))).assertGet shouldBe true
      map.currentFilePath.fileId.assertGet shouldBe(3, Extension.Log)

      //reopen file
      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      val recoveredFile = PersistentMap.recover(map.path, true, 1.byte, skipList, dropCorruptedTailEntries = false).assertGet.item
      recoveredFile.isOpen shouldBe true
      recoveredFile.isMemoryMapped.assertGet shouldBe true
      recoveredFile.existsOnDisk shouldBe true
      recoveredFile.path.fileId.assertGet shouldBe(4, Extension.Log) //file id gets incremented on recover
      recoveredFile.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted
      recoveredFile.path.resolveSibling(1.toLogFileId).exists shouldBe false //1.log gets deleted
      recoveredFile.path.resolveSibling(2.toLogFileId).exists shouldBe false //2.log gets deleted
      recoveredFile.path.resolveSibling(3.toLogFileId).exists shouldBe false //3.log gets deleted

      skipList.isEmpty shouldBe false
      skipList.get(1: Slice[Byte]) shouldBe ((ValueType.Add, Some(1)))
      skipList.get(2: Slice[Byte]) shouldBe ((ValueType.Remove, Some(2)))
      skipList.get(3: Slice[Byte]) shouldBe ((ValueType.Add, Some(3)))

      //reopen the recovered file
      val skipList2 = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      val recoveredFile2 = PersistentMap.recover(map.path, true, 1.byte, skipList2, dropCorruptedTailEntries = false).assertGet.item
      recoveredFile2.isOpen shouldBe true
      recoveredFile2.isMemoryMapped.assertGet shouldBe true
      recoveredFile2.existsOnDisk shouldBe true
      recoveredFile2.path.fileId.assertGet shouldBe(5, Extension.Log) //file id gets incremented on recover
      recoveredFile2.path.resolveSibling(4.toLogFileId).exists shouldBe false //4.log gets deleted

      skipList2.isEmpty shouldBe false
      skipList2.get(1: Slice[Byte]) shouldBe ((ValueType.Add, Some(1)))
      skipList2.get(2: Slice[Byte]) shouldBe ((ValueType.Remove, Some(2)))
      skipList2.get(3: Slice[Byte]) shouldBe ((ValueType.Add, Some(3)))
    }

    "recover from an existing PersistentMap folder with empty memory map" in {
      //create a map
      val map = Map.persistent(createRandomDir, mmap = true, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = false).assertGet.item
      map.currentFilePath.fileId.assertGet shouldBe(0, Extension.Log)
      map.close().assertGet

      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      val file = PersistentMap.recover(map.path, false, 4.mb, skipList, dropCorruptedTailEntries = false).assertGet.item

      file.isOpen shouldBe true
      file.isMemoryMapped.assertGet shouldBe false
      file.existsOnDisk shouldBe true
      file.path.fileId.assertGet shouldBe(1, Extension.Log) //file id gets incremented on recover
      file.path.resolveSibling(0.toLogFileId).exists shouldBe false //0.log gets deleted

      skipList.isEmpty shouldBe true
    }
  }

  "PersistentMap.nextFile" should {
    "creates a new file from the current file" in {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      skipList.put(1, (ValueType.Add, Some(1)))
      skipList.put(2, (ValueType.Add, Some(2)))
      skipList.put(3, (ValueType.Remove, Some(3)))

      val currentFile = PersistentMap.recover(createRandomDir, false, 4.mb, skipList, dropCorruptedTailEntries = false).assertGet.item
      val nextFile = PersistentMap.nextFile(currentFile, false, 4.mb, skipList).assertGet

      val nextFileSkipList = new ConcurrentSkipListMap[Slice[Byte], (ValueType, Option[Slice[Byte]])](ordering)
      val nextFileBytes = DBFile.channelRead(nextFile.path).assertGet.readAll.assertGet
      val mapEntries = MapCodec.read(nextFileBytes, dropCorruptedTailEntries = false).assertGet.item.assertGet
      mapEntries applyTo nextFileSkipList

      nextFileSkipList.get(1: Slice[Byte]) shouldBe ((ValueType.Add, Some(1)))
      nextFileSkipList.get(2: Slice[Byte]) shouldBe ((ValueType.Add, Some(2)))
      nextFileSkipList.get(3: Slice[Byte]) shouldBe ((ValueType.Remove, Some(3)))
    }
  }

  "PersistentMap.recovery on corruption" should {
    val map = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = false).assertGet.item
    (1 to 100) foreach {
      i =>
        map.add(i, (ValueType.Add, Some(i))).assertGet shouldBe true
    }
    val allBytes = Files.readAllBytes(map.currentFilePath)

    "fail if the WAL file is corrupted and and when dropCorruptedTailEntries = false" in {
      def assertRecover =
        Map.persistent(map.currentFilePath.getParent, mmap = false, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = false).failed.assertGet shouldBe a[IllegalStateException]

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
      val recoveredMap = Map.persistent(map.currentFilePath.getParent, mmap = false, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = true).assertGet.item
      (1 to 99) foreach {
        i =>
          recoveredMap.get(i).assertGet._2 shouldBe ((ValueType.Add, Some(i)))
      }
      recoveredMap.contains(100) shouldBe false

      //if the top entry is corrupted.
      Files.write(recoveredMap.currentFilePath, allBytes.drop(1))
      val recoveredMap2 = Map.persistent(recoveredMap.currentFilePath.getParent, mmap = false, flushOnOverflow = false, fileSize = 4.mb, dropCorruptedTailEntries = true).assertGet.item
      recoveredMap2.isEmpty shouldBe true
    }
  }

  "PersistentMap.recovery on corruption when there are two WAL files and the first file is corrupted" should {
    val map1 = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
    map1.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true
    map1.add(2, (ValueType.Add, Some(2))).assertGet shouldBe true
    map1.add(3, (ValueType.Add, Some(3))).assertGet shouldBe true

    val map2 = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
    map2.add(4, (ValueType.Add, Some(4))).assertGet shouldBe true
    map2.add(5, (ValueType.Add, Some(5))).assertGet shouldBe true
    map2.add(6, (ValueType.Add, Some(6))).assertGet shouldBe true

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
      Map.persistent(map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).failed.assertGet shouldBe a[IllegalStateException]
      Files.write(log0, log0Bytes) //fix log0 bytes
    }

    "successfully recover Map by reading both WAL files if the first WAL file is corrupted" in {
      //corrupt 0.log bytes
      Files.write(log0, log0Bytes.dropRight(1))
      val recoveredMapWith0LogCorrupted: RecoveryResult[PersistentMap[Slice[Byte], (ValueType, Option[Slice[Byte]])]] = Map.persistent(map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = true).assertGet
      //recovery state contains failure because the WAL file is partially recovered.
      recoveredMapWith0LogCorrupted.result.failed.assertGet shouldBe a[IllegalStateException]
      recoveredMapWith0LogCorrupted.item.size shouldBe 5 //5 because the 3rd entry in 0.log is corrupted

      //checking the recovered entries
      recoveredMapWith0LogCorrupted.item.get(1).assertGet._2 shouldBe ((ValueType.Add, Some(1)))
      recoveredMapWith0LogCorrupted.item.get(2).assertGet._2 shouldBe ((ValueType.Add, Some(2)))
      recoveredMapWith0LogCorrupted.item.get(3) shouldBe empty //since the last byte of 0.log file is corrupted, the last entry is missing
      recoveredMapWith0LogCorrupted.item.get(4).assertGet._2 shouldBe ((ValueType.Add, Some(4)))
      recoveredMapWith0LogCorrupted.item.get(5).assertGet._2 shouldBe ((ValueType.Add, Some(5)))
      recoveredMapWith0LogCorrupted.item.get(6).assertGet._2 shouldBe ((ValueType.Add, Some(6)))
    }
  }

  "PersistentMap.recovery on corruption when there are two WAL files and the second file is corrupted" should {
    val map1 = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
    map1.add(1, (ValueType.Add, Some(1))).assertGet shouldBe true
    map1.add(2, (ValueType.Add, Some(2))).assertGet shouldBe true
    map1.add(3, (ValueType.Add, Some(3))).assertGet shouldBe true

    val map2 = Map.persistent(createRandomDir, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).assertGet.item
    map2.add(4, (ValueType.Add, Some(4))).assertGet shouldBe true
    map2.add(5, (ValueType.Add, Some(5))).assertGet shouldBe true
    map2.add(6, (ValueType.Add, Some(6))).assertGet shouldBe true

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
      Map.persistent(map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = false).failed.assertGet shouldBe a[IllegalStateException]
      Files.write(log1, log1Bytes) //fix log1 bytes
    }

    "successfully recover Map by reading both WAL files if the second WAL file is corrupted" in {
      //corrupt 1.log bytes
      Files.write(log1, log1Bytes.dropRight(1))
      val recoveredMapWith0LogCorrupted: RecoveryResult[PersistentMap[Slice[Byte], (ValueType, Option[Slice[Byte]])]] = Map.persistent(map1.path, mmap = false, flushOnOverflow = false, 1.mb, dropCorruptedTailEntries = true).assertGet
      //recovery state contains failure because the WAL file is partially recovered.
      recoveredMapWith0LogCorrupted.result.failed.assertGet shouldBe a[IllegalStateException]
      recoveredMapWith0LogCorrupted.item.size shouldBe 5 //5 because the 3rd entry in 1.log is corrupted

      //checking the recovered entries
      recoveredMapWith0LogCorrupted.item.get(1).assertGet._2 shouldBe ((ValueType.Add, Some(1)))
      recoveredMapWith0LogCorrupted.item.get(2).assertGet._2 shouldBe ((ValueType.Add, Some(2)))
      recoveredMapWith0LogCorrupted.item.get(3).assertGet._2 shouldBe ((ValueType.Add, Some(3)))
      recoveredMapWith0LogCorrupted.item.get(4).assertGet._2 shouldBe ((ValueType.Add, Some(4)))
      recoveredMapWith0LogCorrupted.item.get(5).assertGet._2 shouldBe ((ValueType.Add, Some(5)))
      recoveredMapWith0LogCorrupted.item.get(6) shouldBe empty
    }
  }

}
