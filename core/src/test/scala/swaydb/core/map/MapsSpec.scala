/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.PrivateMethodInvokers._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, MemoryOption, Value}
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestTimer}
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.{MMAP, RecoveryMode}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.effect.Effect._
import swaydb.effect.{Effect, Extension}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import java.nio.file.NoSuchFileException

class MapsSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit def optimiseWrites: OptimiseWrites = OptimiseWrites.random
  implicit def atomic = Atomic.random

  import swaydb.core.map.serializer.LevelZeroMapEntryReader._
  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

  "Maps.persistent" should {
    "initialise and recover on reopen" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val path = createRandomDir
            val maps: Maps[Slice[Byte], Memory, LevelZeroMapCache] =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.mb,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            maps.write(_ => MapEntry.Put(1, Memory.put(1)))
            maps.write(_ => MapEntry.Put(2, Memory.put(2)))
            maps.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](1, Memory.remove(1)))

            maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.remove(1)
            maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2)
            //since the size of the Map is 1.mb and entries are too small. No flushing will value executed and there should only be one folder.
            path.folders.map(_.folderId) should contain only 0

            if (maps.mmap.hasMMAP && OperatingSystem.isWindows) {
              maps.close().value
              sweeper.receiveAll()
            }

            //reopen and it should contain the same entries as above and old map should value delete
            val reopen =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.mb,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            //adding more entries to reopened Map should contain all entries
            reopen.write(_ => MapEntry.Put(3, Memory.put(3)))
            reopen.write(_ => MapEntry.Put(4, Memory.put(4)))
            reopen.find[MemoryOption](Memory.Null, _.cache.skipList.get(3)) shouldBe Memory.put(3)
            reopen.find[MemoryOption](Memory.Null, _.cache.skipList.get(4)) shouldBe Memory.put(4)
            //old entries still exist in the reopened map
            reopen.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.remove(1)
            reopen.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2)

            //reopening will start the Map in recovery mode which will add all the existing maps in memory and initialise a new map for writing
            //so a new folder 1 is initialised.
            path.folders.map(_.folderId) shouldBe List(0, 1)
        }
      }
    }

    "delete empty maps on recovery" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val path = createRandomDir
            val maps =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.mb,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            if (maps.mmap.hasMMAP && OperatingSystem.isWindows) {
              maps.close().value
              sweeper.receiveAll()
            }

            maps.mapsCount shouldBe 1
            val currentMapsPath = maps.map.asInstanceOf[PersistentMap[Slice[Byte], Memory, LevelZeroMapCache]].path
            //above creates a map without any entries

            //reopen should create a new map, delete the previous maps current map as it's empty
            val reopen =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.mb,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            reopen.mapsCount shouldBe 1
            //since the old map is empty, it should value deleted

            if (reopen.mmap.hasMMAP && OperatingSystem.isWindows)
              sweeper.receiveAll()

            currentMapsPath.exists shouldBe false
        }
      }
    }
  }

  "Maps.memory" should {
    "initialise" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val map: Maps[Slice[Byte], Memory, LevelZeroMapCache] =
            Maps.memory[Slice[Byte], Memory, LevelZeroMapCache](
              fileSize = 1.mb,
              acceleration = Accelerator.brake()
            )

          map.write(_ => MapEntry.Put(1, Memory.put(1)))
          map.write(_ => MapEntry.Put(2, Memory.put(2)))
          map.write(_ => MapEntry.Put[Slice[Byte], Memory](1, Memory.remove(1)))

          map.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.remove(1)
          map.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2)
      }
    }
  }

  "Maps" should {
    "initialise a new map if the current map is full" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          def test(maps: Maps[Slice[Byte], Memory, LevelZeroMapCache]) = {
            maps.write(_ => MapEntry.Put(1, Memory.put(1))) //entry size is 21.bytes
            maps.write(_ => MapEntry.Put(2: Slice[Byte], Memory.Range(2, 2, Value.FromValue.Null, Value.update(2)))) //another 31.bytes
            maps.mapsCount shouldBe 1
            //another 32.bytes but map has total size of 82.bytes.
            //now since the Map is overflow a new should value created.
            maps.write(_ => MapEntry.Put[Slice[Byte], Memory](3, Memory.remove(3)))
            maps.mapsCount shouldBe 2
          }

          //persistent
          val path = createRandomDir
          val maps =
            Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
              path = path,
              mmap = MMAP.randomForMap(),
              fileSize = 21.bytes + 31.bytes,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.ReportFailure
            ).value.sweep()

          test(maps)
          //new map 1 gets created since the 3rd entry is overflow entry.
          path.folders.map(_.folderId) should contain only(0, 1)

          //in memory
          test(
            Maps.memory(
              fileSize = 21.bytes + 31.bytes,
              acceleration = Accelerator.brake()
            )
          )
      }
    }

    "write a key value larger then the actual fileSize" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val largeValue = randomBytesSlice(1.mb)

            def test(maps: Maps[Slice[Byte], Memory, LevelZeroMapCache]): Maps[Slice[Byte], Memory, LevelZeroMapCache] = {
              //adding 1.mb key-value to map, the file size is 500.bytes, since this is the first entry in the map and the map is empty,
              // the entry will value added.
              maps.write(_ => MapEntry.Put(1, Memory.put(1, largeValue))) //large entry
              maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.put(1, largeValue)
              maps.mapsCount shouldBe 1

              //now the map is overflown. Adding any other entry will create a new map
              maps.write(_ => MapEntry.Put(2, Memory.put(2, 2)))
              maps.mapsCount shouldBe 2

              //a small entry of 24.bytes gets written to the same Map since the total size is 500.bytes
              maps.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](3, Memory.remove(3)))
              maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(3)) shouldBe Memory.remove(3)
              maps.mapsCount shouldBe 2

              //write large entry again and a new Map is created again.
              maps.write(_ => MapEntry.Put(1, Memory.put(1, largeValue))) //large entry
              maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.put(1, largeValue)
              maps.mapsCount shouldBe 3

              //now again the map is overflown. Adding any other entry will create a new map
              maps.write(_ => MapEntry.Put[Slice[Byte], Memory](4, Memory.remove(4)))
              maps.mapsCount shouldBe 4
              maps
            }

            val path = createRandomDir
            //persistent

            val originalMaps =
              test(
                Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                  path = path,
                  mmap = MMAP.randomForMap(),
                  fileSize = 500.bytes,
                  acceleration = Accelerator.brake(),
                  recovery = RecoveryMode.ReportFailure
                ).value
              )

            if (originalMaps.mmap.hasMMAP && OperatingSystem.isWindows)
              originalMaps.ensureClose()

            //in memory
            test(
              Maps.memory(
                fileSize = 500.bytes,
                acceleration = Accelerator.brake()
              )
            )

            //reopen start in recovery mode and existing maps are cached
            val maps =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 500.bytes,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            maps.mapsCount shouldBe 5
            maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.put(1, largeValue)
            maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2, 2)
            maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(3)) shouldBe Memory.remove(3)
            maps.find[MemoryOption](Memory.Null, _.cache.skipList.get(4)) shouldBe Memory.remove(4)
        }
      }
    }

    "recover maps in newest to oldest order" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val path = createRandomDir
            val maps =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            maps.write(_ => MapEntry.Put(1, Memory.put(1)))
            maps.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](2, Memory.remove(2)))
            maps.write(_ => MapEntry.Put(3, Memory.put(3)))
            maps.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](4, Memory.remove(2)))
            maps.write(_ => MapEntry.Put(5, Memory.put(5)))

            maps.mapsCount shouldBe 5
            //maps value added
            getMaps(maps).iterator.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(4, 3, 2, 1, 0)
            maps.last().value.pathOption.value.folderId shouldBe 0

            if (maps.mmap.hasMMAP && OperatingSystem.isWindows) {
              maps.close().value
              sweeper.receiveAll()
            }

            val recovered1 =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            getMaps(recovered1).iterator.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(5, 4, 3, 2, 1, 0)
            recovered1.map.pathOption.value.folderId shouldBe 5
            recovered1.write(_ => MapEntry.Put[Slice[Byte], Memory.Remove](6, Memory.remove(6)))
            recovered1.last().value.pathOption.value.folderId shouldBe 0

            if (recovered1.mmap.hasMMAP && OperatingSystem.isWindows) {
              recovered1.close().value
              sweeper.receiveAll()
            }

            val recovered2 =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            getMaps(recovered2).iterator.toList.map(_.pathOption.value.folderId) should contain inOrderOnly(6, 5, 4, 3, 2, 1, 0)
            recovered2.map.pathOption.value.folderId shouldBe 6
            recovered2.last().value.pathOption.value.folderId shouldBe 0
        }
      }
    }

    "recover from existing maps" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val path = createRandomDir
            val maps =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            maps.write(_ => MapEntry.Put(1, Memory.put(1)))
            maps.write(_ => MapEntry.Put(2, Memory.put(2)))
            maps.write(_ => MapEntry.Put[Slice[Byte], Memory](1, Memory.remove(1)))

            if (maps.mmap.hasMMAP && OperatingSystem.isWindows) {
              maps.close().value
              sweeper.receiveAll()
            }

            val recoveredMaps =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value.sweep()

            val recoveredMapsMaps = getMaps(recoveredMaps).iterator.toList
            recoveredMapsMaps should have size 4
            recoveredMapsMaps.map(_.pathOption.value.folderId) shouldBe List(3, 2, 1, 0)

            recoveredMapsMaps.head.cache.skipList.size shouldBe 0
            recoveredMapsMaps.tail.head.cache.skipList.get(1) shouldBe Memory.remove(1)
            recoveredMapsMaps.tail.drop(1).head.cache.skipList.get(2) shouldBe Memory.put(2)
            recoveredMapsMaps.last.cache.skipList.get(1) shouldBe Memory.put(1)
        }
      }
    }

    "fail recovery if one of the map is corrupted and recovery mode is ReportFailure" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val path = createRandomDir
            val maps =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 1.byte,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            maps.write(_ => MapEntry.Put(1, Memory.put(1)))
            maps.write(_ => MapEntry.Put(2, Memory.put(2)))
            maps.write(_ => MapEntry.Put[Slice[Byte], Memory](3, Memory.remove(3)))

            if (maps.mmap.hasMMAP && OperatingSystem.isWindows) {
              maps.close().value
              sweeper.receiveAll()
            }

            val secondMapsPath = getMaps(maps).iterator.toList.tail.head.pathOption.value.files(Extension.Log).head
            val secondMapsBytes = Effect.readAllBytes(secondMapsPath)
            Effect.overwrite(secondMapsPath, secondMapsBytes.dropRight(1))

            Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
              path = path,
              mmap = MMAP.randomForMap(),
              fileSize = 1.byte,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.ReportFailure
            ).left.value.exception shouldBe a[IllegalStateException]
        }
      }
    }

    "continue recovery if one of the map is corrupted and recovery mode is DropCorruptedTailEntries" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val path = createRandomDir
            val maps =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.Off(TestForceSave.channel()),
                fileSize = 50.bytes,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.ReportFailure
              ).value

            maps.write(_ => MapEntry.Put(1, Memory.put(1)))
            maps.write(_ => MapEntry.Put(2, Memory.put(2)))
            maps.write(_ => MapEntry.Put(3, Memory.put(3, 3)))
            maps.write(_ => MapEntry.Put(4, Memory.put(4)))
            maps.write(_ => MapEntry.Put(5, Memory.put(5)))
            maps.write(_ => MapEntry.Put(6, Memory.put(6, 6)))

            val secondMapsPath = getMaps(maps).iterator.toList.tail.head.pathOption.value.files(Extension.Log).head
            val secondMapsBytes = Effect.readAllBytes(secondMapsPath)
            Effect.overwrite(secondMapsPath, secondMapsBytes.dropRight(1))

            val recovered =
              Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
                path = path,
                mmap = MMAP.randomForMap(),
                fileSize = 50.bytes,
                acceleration = Accelerator.brake(),
                recovery = RecoveryMode.DropCorruptedTailEntries
              ).value.sweep()

            val recoveredMaps = getMaps(recovered).iterator.toList

            //recovered maps will still be 4 (including a new map) but since second maps second entry is corrupted, the first entry will still exists.
            recoveredMaps should have size 4

            recoveredMaps.head.cache.skipList.size shouldBe 0

            val recoveredMapsWithoutNew = recoveredMaps.tail

            //newest map contains all key-values
            recoveredMapsWithoutNew.head.cache.skipList.get(5) shouldBe Memory.put(5)
            recoveredMapsWithoutNew.head.cache.skipList.get(6) shouldBe Memory.put(6, 6)

            //second map is the corrupted map and will have the 2nd entry missing
            recoveredMapsWithoutNew.tail.head.cache.skipList.get(3) shouldBe Memory.put(3, 3)
            recoveredMapsWithoutNew.tail.head.cache.skipList.get(4).toOptionS shouldBe empty //4th entry is corrupted, it will not exist the Map

            //oldest map contains all key-values
            recoveredMapsWithoutNew.last.cache.skipList.get(1) shouldBe Memory.put(1)
            recoveredMapsWithoutNew.last.cache.skipList.get(2) shouldBe Memory.put(2)
        }
      }
    }

    "continue recovery if one of the map is corrupted and recovery mode is DropCorruptedTailEntriesAndMaps" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val path = createRandomDir
          val maps =
            Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
              path = path,
              mmap = MMAP.Off(TestForceSave.channel()),
              fileSize = 50.bytes,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.ReportFailure
            ).value

          maps.write(_ => MapEntry.Put(1, Memory.put(1)))
          maps.write(_ => MapEntry.Put(2, Memory.put(2, 2)))
          maps.write(_ => MapEntry.Put(3, Memory.put(3)))
          maps.write(_ => MapEntry.Put(4, Memory.put(4)))
          maps.write(_ => MapEntry.Put(5, Memory.put(5)))
          maps.write(_ => MapEntry.Put(6, Memory.put(6)))

          val secondMapsPath = getMaps(maps).iterator.toList.tail.head.pathOption.value.files(Extension.Log).head
          val secondMapsBytes = Effect.readAllBytes(secondMapsPath)
          Effect.overwrite(secondMapsPath, secondMapsBytes.dropRight(1))

          val recoveredMaps =
            Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
              path = path,
              mmap = MMAP.randomForMap(),
              fileSize = 50.bytes,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.DropCorruptedTailEntriesAndMaps
            ).value.sweep()

          getMaps(recoveredMaps) should have size 3
          //the last map is delete since the second last Map is found corrupted.
          getMaps(maps).iterator.toList.last.exists shouldBe false

          //oldest map contains all key-values
          recoveredMaps.find[MemoryOption](Memory.Null, _.cache.skipList.get(1)) shouldBe Memory.put(1)
          recoveredMaps.find[MemoryOption](Memory.Null, _.cache.skipList.get(2)) shouldBe Memory.put(2, 2)
          //second map is partially read but it's missing 4th entry
          recoveredMaps.find[MemoryOption](Memory.Null, _.cache.skipList.get(3)) shouldBe Memory.put(3)
          //third map is completely ignored.
          recoveredMaps.find[MemoryOption](Memory.Null, _.cache.skipList.get(4)).toOptional shouldBe empty
          recoveredMaps.find[MemoryOption](Memory.Null, _.cache.skipList.get(5)).toOptional shouldBe empty
          recoveredMaps.find[MemoryOption](Memory.Null, _.cache.skipList.get(6)).toOptional shouldBe empty
      }
    }

    "start a new Map if writing an entry fails" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val path = createRandomDir
          val maps =
            Maps.persistent[Slice[Byte], Memory, LevelZeroMapCache](
              path = path,
              mmap = MMAP.Off(TestForceSave.channel()),
              fileSize = 100.bytes,
              acceleration = Accelerator.brake(),
              recovery = RecoveryMode.ReportFailure
            ).value

          maps.write(_ => MapEntry.Put(1, Memory.put(1)))
          maps.mapsCount shouldBe 1
          //delete the map
          maps.map.delete

          //failure because the file is deleted. The Map will NOT try to re-write this entry again because
          //it should be an indication that something is wrong with the file system permissions.
          assertThrows[NoSuchFileException] {
            maps.write(_ => MapEntry.Put(2, Memory.put(2)))
          }

          //new Map file is created. Now this write will succeed.
          maps.write(_ => MapEntry.Put(2, Memory.put(2)))
      }
    }
  }
}
