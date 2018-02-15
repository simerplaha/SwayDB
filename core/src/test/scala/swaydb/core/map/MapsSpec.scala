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

import java.nio.file.{Files, NoSuchFileException}

import swaydb.core.TestBase
import swaydb.core.data.ValueType
import swaydb.core.map.serializer.Level0KeyValuesSerializer
import swaydb.core.util.Extension
import swaydb.core.util.FileUtil._
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.RecoveryMode
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._

class MapsSpec extends TestBase {

  implicit val ordering = KeyOrder.default
  implicit val serializer = Level0KeyValuesSerializer(ordering)

  //  override def deleteFiles = false

  "Maps.persistent" should {
    "initialise and recover on reopen" in {
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = true, 1.mb, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.add(1, (ValueType.Add, Some(1))).assertGet
      maps.add(2, (ValueType.Add, Some(2))).assertGet
      maps.add(1, (ValueType.Remove, Some(11))).assertGet

      maps.get(1).assertGet._2 shouldBe(ValueType.Remove, Some(11))
      maps.get(2).assertGet._2 shouldBe(ValueType.Add, Some(2))
      //since the size of the Map is 1.mb and entries are too small. No flushing will get executed and there should only be one folder.
      path.folders.map(_.folderId) should contain only 0

      //reopen and it should contain the same entries as above and old map should get delete
      val reopen = Maps.persistent(path, mmap = false, 1.mb, Accelerator.brake(), RecoveryMode.Report).assertGet
      //adding more entries to reopened Map should contain all entries
      reopen.add(3, (ValueType.Add, Some(3))).assertGet
      reopen.add(4, (ValueType.Add, Some(4))).assertGet
      reopen.get(3).assertGet._2 shouldBe(ValueType.Add, Some(3))
      reopen.get(4).assertGet._2 shouldBe(ValueType.Add, Some(4))
      //old entries still exist in the reopened map
      reopen.get(1).assertGet._2 shouldBe(ValueType.Remove, Some(11))
      reopen.get(2).assertGet._2 shouldBe(ValueType.Add, Some(2))

      //reopening will start the Map in recovery mode which will add all the existing maps in memory and initialise a new map for writing
      //so a new folder 1 is initialised.
      path.folders.map(_.folderId) shouldBe List(0, 1)

      maps.close.assertGet
      reopen.close.assertGet
    }

    "delete empty maps on recovery" in {
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = true, 1.mb, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.queuedMapsCountWithCurrent shouldBe 1
      val currentMapsPath = maps.map.asInstanceOf[PersistentMap[Slice[Byte], Option[Slice[Byte]]]].path
      //above creates a map without any entries

      //reopen should create a new map, delete the previous maps current map as it's empty
      val reopen = Maps.persistent(path, mmap = true, 1.mb, Accelerator.brake(), RecoveryMode.Report).assertGet
      reopen.queuedMapsCountWithCurrent shouldBe 1
      //since the old map is empty, it should get deleted
      currentMapsPath.exists shouldBe false
    }
  }

  "Maps.memory" should {
    "initialise" in {
      val map = Maps.memory(1.mb, Accelerator.brake())
      map.add(1, (ValueType.Add, Some(1))).assertGet
      map.add(2, (ValueType.Add, Some(2))).assertGet
      map.add(1, (ValueType.Remove, Some(11))).assertGet

      map.get(1).assertGet._2 shouldBe(ValueType.Remove, Some(11))
      map.get(2).assertGet._2 shouldBe(ValueType.Add, Some(2))
    }
  }

  "Maps" should {
    "initialise a new map if the current map is full" in {
      def test(maps: Maps[Slice[Byte], (ValueType, Option[Slice[Byte]])]) = {
        maps.add(1, (ValueType.Add, Some(1))).assertGet //entry size is 33.bytes
        maps.add(2, (ValueType.Add, Some(2))).assertGet //another 33.bytes
        //another 33.bytes but map has total size of 66.bytes.
        //now since the Map is overflow a new should get created.
        maps.add(3, (ValueType.Add, Some(3))).assertGet
        maps.queuedMapsCount shouldBe 1
        maps.queuedMapsCountWithCurrent shouldBe 2
      }

      //persistent
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = false, 33.bytes * 2, Accelerator.brake(), RecoveryMode.Report).assertGet
      test(maps)
      //new map 1 gets created since the 3rd entry is overflow entry.
      path.folders.map(_.folderId) should contain only(0, 1)

      //in memory
      test(Maps.memory(33.bytes * 2, Accelerator.brake()))
    }

    "write a key value larger then the actual fileSize" in {
      val largeValue = randomBytesSlice(1.mb)

      def test(maps: Maps[Slice[Byte], (ValueType, Option[Slice[Byte]])]) = {
        //adding 1.mb key-value to map, the file size is 500.bytes, since this is the first entry in the map and the map is empty,
        // the entry will get added.
        maps.add(1, (ValueType.Add, Some(largeValue))).assertGet //large entry
        maps.get(1).assertGet._2 shouldBe(ValueType.Add, Some(largeValue))
        maps.queuedMapsCount shouldBe 0
        maps.queuedMapsCountWithCurrent shouldBe 1

        //now the map is overflown. Adding any other entry will create a new map
        maps.add(2, (ValueType.Add, Some(2))).assertGet
        maps.queuedMapsCount shouldBe 1
        maps.queuedMapsCountWithCurrent shouldBe 2

        //add another large entry
        maps.add(3, (ValueType.Remove, Some(largeValue))).assertGet
        maps.get(3).assertGet._2 shouldBe(ValueType.Remove, Some(largeValue))
        maps.queuedMapsCount shouldBe 2
        maps.queuedMapsCountWithCurrent shouldBe 3

        //now again the map is overflown. Adding any other entry will create a new map
        maps.add(4, (ValueType.Remove, Some(4))).assertGet
        maps.queuedMapsCount shouldBe 3
        maps.queuedMapsCountWithCurrent shouldBe 4
      }

      val path = createRandomDir
      //persistent
      test(Maps.persistent(path, mmap = false, fileSize = 500.bytes, Accelerator.brake(), RecoveryMode.Report).assertGet)
      //in memory
      test(Maps.memory(500.bytes, Accelerator.brake()))

      //reopen start in recovery mode and existing maps are cached
      val maps = Maps.persistent(path, mmap = false, fileSize = 500.bytes, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.queuedMapsCount shouldBe 4
      maps.queuedMapsCountWithCurrent shouldBe 5
      maps.get(1).assertGet._2 shouldBe(ValueType.Add, Some(largeValue))
      maps.get(2).assertGet._2 shouldBe(ValueType.Add, Some(2))
      maps.get(3).assertGet._2 shouldBe(ValueType.Remove, Some(largeValue))
      maps.get(4).assertGet._2 shouldBe(ValueType.Remove, Some(4))
    }

    "recover maps in newest to oldest order" in {
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = true, fileSize = 1.byte, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.add(1, (ValueType.Add, Some(1)))
      maps.add(2, (ValueType.Remove, Some(2)))
      maps.add(3, (ValueType.Add, Some(3)))
      maps.add(4, (ValueType.Remove, Some(4)))
      maps.add(5, (ValueType.Add, Some(5)))

      maps.queuedMapsCountWithCurrent shouldBe 5
      //maps get added
      maps.maps.asScala.toList.map(_.pathOption.assertGet.folderId) should contain inOrderOnly(3, 2, 1, 0)
      maps.last().assertGet.pathOption.assertGet.folderId shouldBe 0

      val recovered1 = Maps.persistent(path, mmap = true, fileSize = 1.byte, Accelerator.brake(), RecoveryMode.Report).assertGet
      recovered1.maps.asScala.toList.map(_.pathOption.assertGet.folderId) should contain inOrderOnly(4, 3, 2, 1, 0)
      recovered1.map.pathOption.assertGet.folderId shouldBe 5
      recovered1.add(6, (ValueType.Remove, Some(6)))
      recovered1.last().assertGet.pathOption.assertGet.folderId shouldBe 0

      val recovered2 = Maps.persistent(path, mmap = true, fileSize = 1.byte, Accelerator.brake(), RecoveryMode.Report).assertGet
      recovered2.maps.asScala.toList.map(_.pathOption.assertGet.folderId) should contain inOrderOnly(5, 4, 3, 2, 1, 0)
      recovered2.map.pathOption.assertGet.folderId shouldBe 6
      recovered2.last().assertGet.pathOption.assertGet.folderId shouldBe 0
    }

    "recover from existing maps" in {
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = true, 1.byte, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.add(1, (ValueType.Add, Some(1))).assertGet
      maps.add(2, (ValueType.Add, Some(2))).assertGet
      maps.add(1, (ValueType.Remove, Some(11))).assertGet

      val recoveredMaps = Maps.persistent(path, mmap = true, 1.byte, Accelerator.brake(), RecoveryMode.Report).assertGet.maps.asScala

      recoveredMaps should have size 3
      recoveredMaps.map(_.pathOption.assertGet.folderId) shouldBe List(2, 1, 0)

      recoveredMaps.head.get(1).assertGet._2 shouldBe ((ValueType.Remove, Some(11)))
      recoveredMaps.tail.head.get(2).assertGet._2 shouldBe ((ValueType.Add, Some(2)))
      recoveredMaps.last.get(1).assertGet._2 shouldBe ((ValueType.Add, Some(1)))
    }

    "fail recovery if one of the map is corrupted and recovery mode is Report" in {
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = true, 1.byte, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.add(1, (ValueType.Add, Some(1))).assertGet
      maps.add(2, (ValueType.Add, Some(2))).assertGet
      maps.add(3, (ValueType.Remove, Some(3))).assertGet

      val secondMapsPath = maps.maps.asScala.tail.head.pathOption.assertGet.files(Extension.Log).head
      val secondMapsBytes = Files.readAllBytes(secondMapsPath)
      Files.write(secondMapsPath, secondMapsBytes.dropRight(1))

      Maps.persistent(path, mmap = true, 1.byte, Accelerator.brake(), RecoveryMode.Report).failed.assertGet shouldBe a[IllegalStateException]
    }

    "continue recovery if one of the map is corrupted and recovery mode is DropCorruptedTailEntries" in {
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = false, 70.byte, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.add(1, (ValueType.Add, Some(1))).assertGet
      maps.add(2, (ValueType.Add, Some(2))).assertGet
      maps.add(3, (ValueType.Add, Some(3))).assertGet
      maps.add(4, (ValueType.Add, Some(4))).assertGet
      maps.add(5, (ValueType.Add, Some(5))).assertGet
      maps.add(6, (ValueType.Add, Some(6))).assertGet

      val secondMapsPath = maps.maps.asScala.head.pathOption.assertGet.files(Extension.Log).head
      val secondMapsBytes = Files.readAllBytes(secondMapsPath)
      Files.write(secondMapsPath, secondMapsBytes.dropRight(1))

      val recoveredMaps = Maps.persistent(path, mmap = false, 70.byte, Accelerator.brake(), RecoveryMode.DropCorruptedTailEntries).assertGet.maps.asScala
      //recovered maps will still be 3 but since second maps second entry is corrupted, the first entry will still exists.
      recoveredMaps should have size 3

      //newest map contains all key-values
      recoveredMaps.head.get(5).assertGet._2 shouldBe ((ValueType.Add, Some(5)))
      recoveredMaps.head.get(6).assertGet._2 shouldBe ((ValueType.Add, Some(6)))

      //second map is the corrupted map and will have the 2nd entry missing
      recoveredMaps.tail.head.get(3).assertGet._2 shouldBe ((ValueType.Add, Some(3)))
      recoveredMaps.tail.head.get(4) shouldBe empty //4th entry is corrupted, it will not exist the Map

      //oldest map contains all key-values
      recoveredMaps.last.get(1).assertGet._2 shouldBe ((ValueType.Add, Some(1)))
      recoveredMaps.last.get(2).assertGet._2 shouldBe ((ValueType.Add, Some(2)))
    }

    "continue recovery if one of the map is corrupted and recovery mode is DropCorruptedTailEntriesAndMaps" in {
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = false, 70.bytes, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.add(1, (ValueType.Add, Some(1))).assertGet
      maps.add(2, (ValueType.Add, Some(2))).assertGet
      maps.add(3, (ValueType.Add, Some(3))).assertGet
      maps.add(4, (ValueType.Add, Some(4))).assertGet
      maps.add(5, (ValueType.Add, Some(5))).assertGet
      maps.add(6, (ValueType.Add, Some(6))).assertGet

      val secondMapsPath = maps.maps.asScala.head.pathOption.assertGet.files(Extension.Log).head
      val secondMapsBytes = Files.readAllBytes(secondMapsPath)
      Files.write(secondMapsPath, secondMapsBytes.dropRight(1))

      val recoveredMaps = Maps.persistent(path, mmap = false, 70.bytes, Accelerator.brake(), RecoveryMode.DropCorruptedTailEntriesAndMaps).assertGet
      recoveredMaps.maps should have size 2
      //the last map is delete since the second last Map is found corrupted.
      maps.maps.asScala.last.exists shouldBe false

      //oldest map contains all key-values
      recoveredMaps.get(1).assertGet._2 shouldBe ((ValueType.Add, Some(1)))
      recoveredMaps.get(2).assertGet._2 shouldBe ((ValueType.Add, Some(2)))
      //second map is partially read but it's missing 4th entry
      recoveredMaps.get(3).assertGet._2 shouldBe ((ValueType.Add, Some(3)))
      //third map is completely ignored.
      recoveredMaps.get(4) shouldBe empty
      recoveredMaps.get(5) shouldBe empty
      recoveredMaps.get(6) shouldBe empty
    }

    "start a new Map if writing an entry fails" in {
      val path = createRandomDir
      val maps = Maps.persistent(path, mmap = false, 70.bytes, Accelerator.brake(), RecoveryMode.Report).assertGet
      maps.add(1, (ValueType.Add, Some(1))).assertGet
      maps.queuedMapsCountWithCurrent shouldBe 1
      //delete the map
      maps.map.delete.assertGet

      //failure because the file is deleted. The Map will NOT try to re-write this entry again because
      //it should be an indication that something is wrong with the file system permissions.
      maps.add(2, (ValueType.Add, Some(2))).failed.assertGet shouldBe a[NoSuchFileException]

      //new Map file is created. Now this write will succeed.
      maps.add(2, (ValueType.Add, Some(2))).assertGet

    }

  }

}
