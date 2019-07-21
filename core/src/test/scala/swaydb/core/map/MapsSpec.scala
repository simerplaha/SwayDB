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

import java.nio.file.{Files, NoSuchFileException}

import swaydb.core.CommonAssertions._
import swaydb.core.IOValues._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, Value}
import swaydb.core.io.file.IOEffect._
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.queue.FileLimiter
import swaydb.core.util.Extension
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.RecoveryMode
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._
import org.scalatest.OptionValues._

import scala.collection.JavaConverters._

class MapsSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit def testTimer: TestTimer = TestTimer.Empty

  import swaydb.core.map.serializer.LevelZeroMapEntryReader._
  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

  implicit val skipListMerger = LevelZeroSkipListMerger

  "Maps.persistent" should {
    "initialise and recover on reopen" in {
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, 1.mb, Accelerator.brake(), RecoveryMode.ReportFailure).runIO

      maps.write(time => MapEntry.Put(1, Memory.put(1))).runIO
      maps.write(_ => MapEntry.Put(2, Memory.put(2))).runIO
      maps.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1))).runIO

      maps.get(1).value shouldBe Memory.remove(1)
      maps.get(2).value shouldBe Memory.put(2)
      //since the size of the Map is 1.mb and entries are too small. No flushing will value executed and there should only be one folder.
      path.folders.map(_.folderId) should contain only 0

      //reopen and it should contain the same entries as above and old map should value delete
      val reopen = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, 1.mb, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      //adding more entries to reopened Map should contain all entries
      reopen.write(_ => MapEntry.Put(3, Memory.put(3))).runIO
      reopen.write(_ => MapEntry.Put(4, Memory.put(4))).runIO
      reopen.get(3).value shouldBe Memory.put(3)
      reopen.get(4).value shouldBe Memory.put(4)
      //old entries still exist in the reopened map
      reopen.get(1).value shouldBe Memory.remove(1)
      reopen.get(2).value shouldBe Memory.put(2)

      //reopening will start the Map in recovery mode which will add all the existing maps in memory and initialise a new map for writing
      //so a new folder 1 is initialised.
      path.folders.map(_.folderId) shouldBe List(0, 1)

      maps.close.runIO
      reopen.close.runIO
    }

    "delete empty maps on recovery" in {
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, 1.mb, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      maps.queuedMapsCountWithCurrent shouldBe 1
      val currentMapsPath = maps.map.asInstanceOf[PersistentMap[Slice[Byte], Option[Slice[Byte]]]].path
      //above creates a map without any entries

      //reopen should create a new map, delete the previous maps current map as it's empty
      val reopen = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, 1.mb, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      reopen.queuedMapsCountWithCurrent shouldBe 1
      //since the old map is empty, it should value deleted
      currentMapsPath.exists shouldBe false
    }
  }

  "Maps.memory" should {
    "initialise" in {
      val map = Maps.memory[Slice[Byte], Memory.SegmentResponse](1.mb, Accelerator.brake())
      map.write(_ => MapEntry.Put(1, Memory.put(1))).runIO
      map.write(_ => MapEntry.Put(2, Memory.put(2))).runIO
      map.write(_ => MapEntry.Put[Slice[Byte], Memory.SegmentResponse](1, Memory.remove(1))).runIO

      map.get(1).value shouldBe Memory.remove(1)
      map.get(2).value shouldBe Memory.put(2)
    }
  }

  "Maps" should {
    "initialise a new map if the current map is full" in {
      def test(maps: Maps[Slice[Byte], Memory.SegmentResponse]) = {
        maps.write(_ => MapEntry.Put(1, Memory.put(1))).runIO //entry size is 40.bytes
        maps.write(_ => MapEntry.Put(2: Slice[Byte], Memory.Range(2, 2, None, Value.update(2)))).runIO //another 43.bytes
        maps.queuedMapsCountWithCurrent shouldBe 1
        //another 32.bytes but map has total size of 82.bytes.
        //now since the Map is overflow a new should value created.
        maps.write(_ => MapEntry.Put[Slice[Byte], Memory.SegmentResponse](3, Memory.remove(3))).runIO
        maps.queuedMapsCount shouldBe 1
        maps.queuedMapsCountWithCurrent shouldBe 2
      }

      //persistent
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, 40.bytes + 43.bytes, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      test(maps)
      //new map 1 gets created since the 3rd entry is overflow entry.
      path.folders.map(_.folderId) should contain only(0, 1)

      //in memory
      test(Maps.memory(40.bytes + 43.bytes, Accelerator.brake()))
    }

    "write a key value larger then the actual fileSize" in {
      val largeValue = randomBytesSlice(1.mb)

      def test(maps: Maps[Slice[Byte], Memory.SegmentResponse]) = {
        //adding 1.mb key-value to map, the file size is 500.bytes, since this is the first entry in the map and the map is empty,
        // the entry will value added.
        maps.write(_ => MapEntry.Put(1, Memory.put(1, largeValue))).runIO //large entry
        maps.get(1).value shouldBe Memory.put(1, largeValue)
        maps.queuedMapsCount shouldBe 0
        maps.queuedMapsCountWithCurrent shouldBe 1

        //now the map is overflown. Adding any other entry will create a new map
        maps.write(_ => MapEntry.Put(2, Memory.put(2, 2))).runIO
        maps.queuedMapsCount shouldBe 1
        maps.queuedMapsCountWithCurrent shouldBe 2

        //a small entry of 24.bytes gets written to the same Map since the total size is 500.bytes
        maps.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](3, Memory.remove(3))).runIO
        maps.get(3).value shouldBe Memory.remove(3)
        maps.queuedMapsCount shouldBe 1
        maps.queuedMapsCountWithCurrent shouldBe 2

        //write large entry again and a new Map is created again.
        maps.write(_ => MapEntry.Put(1, Memory.put(1, largeValue))).runIO //large entry
        maps.get(1).value shouldBe Memory.put(1, largeValue)
        maps.queuedMapsCount shouldBe 2
        maps.queuedMapsCountWithCurrent shouldBe 3

        //now again the map is overflown. Adding any other entry will create a new map
        maps.write(_ => MapEntry.Put[Slice[Byte], Memory.SegmentResponse](4, Memory.remove(4))).runIO
        maps.queuedMapsCount shouldBe 3
        maps.queuedMapsCountWithCurrent shouldBe 4
      }

      val path = createRandomDir
      //persistent
      test(Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, fileSize = 500.bytes, Accelerator.brake(), RecoveryMode.ReportFailure).runIO)
      //in memory
      test(Maps.memory(500.bytes, Accelerator.brake()))

      //reopen start in recovery mode and existing maps are cached
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, fileSize = 500.bytes, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      maps.queuedMapsCount shouldBe 4
      maps.queuedMapsCountWithCurrent shouldBe 5
      maps.get(1).value shouldBe Memory.put(1, largeValue)
      maps.get(2).value shouldBe Memory.put(2, 2)
      maps.get(3).value shouldBe Memory.remove(3)
      maps.get(4).value shouldBe Memory.remove(4)
    }

    "recover maps in newest to oldest order" in {
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, fileSize = 1.byte, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      maps.write(_ => MapEntry.Put(1, Memory.put(1)))
      maps.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)))
      maps.write(_ => MapEntry.Put(3, Memory.put(3)))
      maps.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](4, Memory.remove(2)))
      maps.write(_ => MapEntry.Put(5, Memory.put(5)))

      maps.queuedMapsCountWithCurrent shouldBe 5
      //maps value added
      maps.maps.asScala.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(3, 2, 1, 0)
      maps.last().value.pathOption.value.folderId shouldBe 0

      val recovered1 = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, fileSize = 1.byte, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      recovered1.maps.asScala.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(4, 3, 2, 1, 0)
      recovered1.map.pathOption.value.folderId shouldBe 5
      recovered1.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](6, Memory.remove(6)))
      recovered1.last().value.pathOption.value.folderId shouldBe 0

      val recovered2 = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, fileSize = 1.byte, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      recovered2.maps.asScala.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(5, 4, 3, 2, 1, 0)
      recovered2.map.pathOption.value.folderId shouldBe 6
      recovered2.last().value.pathOption.value.folderId shouldBe 0
    }

    "recover from existing maps" in {
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, 1.byte, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      maps.write(_ => MapEntry.Put(1, Memory.put(1))).runIO
      maps.write(_ => MapEntry.Put(2, Memory.put(2))).runIO
      maps.write(_ => MapEntry.Put[Slice[Byte], Memory.SegmentResponse](1, Memory.remove(1))).runIO

      val recoveredMaps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, 1.byte, Accelerator.brake(), RecoveryMode.ReportFailure).runIO.maps.asScala

      recoveredMaps should have size 3
      recoveredMaps.map(_.pathOption.value.folderId) shouldBe List(2, 1, 0)

      recoveredMaps.head.get(1).value shouldBe Memory.remove(1)
      recoveredMaps.tail.head.get(2).value shouldBe Memory.put(2)
      recoveredMaps.last.get(1).value shouldBe Memory.put(1)
    }

    "fail recovery if one of the map is corrupted and recovery mode is ReportFailure" in {
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, 1.byte, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      maps.write(_ => MapEntry.Put(1, Memory.put(1))).runIO
      maps.write(_ => MapEntry.Put(2, Memory.put(2))).runIO
      maps.write(_ => MapEntry.Put[Slice[Byte], Memory.SegmentResponse](3, Memory.remove(3))).runIO

      val secondMapsPath = maps.maps.asScala.tail.head.pathOption.value.files(Extension.Log).head
      val secondMapsBytes = Files.readAllBytes(secondMapsPath)
      Files.write(secondMapsPath, secondMapsBytes.dropRight(1))

      Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = true, 1.byte, Accelerator.brake(), RecoveryMode.ReportFailure).failed.runIO.exception shouldBe a[IllegalStateException]
    }

    "continue recovery if one of the map is corrupted and recovery mode is DropCorruptedTailEntries" in {
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, 100.bytes, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      maps.write(_ => MapEntry.Put(1, Memory.put(1))).runIO
      maps.write(_ => MapEntry.Put(2, Memory.put(2))).runIO
      maps.write(_ => MapEntry.Put(3, Memory.put(3, 3))).runIO
      maps.write(_ => MapEntry.Put(4, Memory.put(4))).runIO
      maps.write(_ => MapEntry.Put(5, Memory.put(5))).runIO
      maps.write(_ => MapEntry.Put(6, Memory.put(6, 6))).runIO

      val secondMapsPath = maps.maps.asScala.head.pathOption.value.files(Extension.Log).head
      val secondMapsBytes = Files.readAllBytes(secondMapsPath)
      Files.write(secondMapsPath, secondMapsBytes.dropRight(1))

      val recoveredMaps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, 100.bytes, Accelerator.brake(), RecoveryMode.DropCorruptedTailEntries).runIO.maps.asScala
      //recovered maps will still be 3 but since second maps second entry is corrupted, the first entry will still exists.
      recoveredMaps should have size 3

      //newest map contains all key-values
      recoveredMaps.head.get(5).value shouldBe Memory.put(5)
      recoveredMaps.head.get(6).value shouldBe Memory.put(6, 6)

      //second map is the corrupted map and will have the 2nd entry missing
      recoveredMaps.tail.head.get(3).value shouldBe Memory.put(3, 3)
      recoveredMaps.tail.head.get(4) shouldBe empty //4th entry is corrupted, it will not exist the Map

      //oldest map contains all key-values
      recoveredMaps.last.get(1).value shouldBe Memory.put(1)
      recoveredMaps.last.get(2).value shouldBe Memory.put(2)
    }

    "continue recovery if one of the map is corrupted and recovery mode is DropCorruptedTailEntriesAndMaps" in {
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, 100.bytes, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      maps.write(_ => MapEntry.Put(1, Memory.put(1))).runIO
      maps.write(_ => MapEntry.Put(2, Memory.put(2, 2))).runIO
      maps.write(_ => MapEntry.Put(3, Memory.put(3))).runIO
      maps.write(_ => MapEntry.Put(4, Memory.put(4))).runIO
      maps.write(_ => MapEntry.Put(5, Memory.put(5))).runIO
      maps.write(_ => MapEntry.Put(6, Memory.put(6))).runIO

      val secondMapsPath = maps.maps.asScala.head.pathOption.value.files(Extension.Log).head
      val secondMapsBytes = Files.readAllBytes(secondMapsPath)
      Files.write(secondMapsPath, secondMapsBytes.dropRight(1))

      val recoveredMaps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, 100.bytes, Accelerator.brake(), RecoveryMode.DropCorruptedTailEntriesAndMaps).runIO
      recoveredMaps.maps should have size 2
      //the last map is delete since the second last Map is found corrupted.
      maps.maps.asScala.last.exists shouldBe false

      //oldest map contains all key-values
      recoveredMaps.get(1).value shouldBe Memory.put(1)
      recoveredMaps.get(2).value shouldBe Memory.put(2, 2)
      //second map is partially read but it's missing 4th entry
      recoveredMaps.get(3).value shouldBe Memory.put(3)
      //third map is completely ignored.
      recoveredMaps.get(4) shouldBe empty
      recoveredMaps.get(5) shouldBe empty
      recoveredMaps.get(6) shouldBe empty
    }

    "start a new Map if writing an entry fails" in {
      val path = createRandomDir
      val maps = Maps.persistent[Slice[Byte], Memory.SegmentResponse](path, mmap = false, 100.bytes, Accelerator.brake(), RecoveryMode.ReportFailure).runIO
      maps.write(_ => MapEntry.Put(1, Memory.put(1))).runIO
      maps.queuedMapsCountWithCurrent shouldBe 1
      //delete the map
      maps.map.delete.runIO

      //failure because the file is deleted. The Map will NOT try to re-write this entry again because
      //it should be an indication that something is wrong with the file system permissions.
      maps.write(_ => MapEntry.Put(2, Memory.put(2))).failed.runIO.exception shouldBe a[NoSuchFileException]

      //new Map file is created. Now this write will succeed.
      maps.write(_ => MapEntry.Put(2, Memory.put(2))).runIO
    }
  }
}
