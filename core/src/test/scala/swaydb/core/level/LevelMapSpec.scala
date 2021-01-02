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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.map.{Map, MapEntry}
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.serializers.Default._
import swaydb.serializers._

class LevelMapSpec0 extends LevelMapSpec

class LevelMapSpec1 extends LevelMapSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelMapSpec2 extends LevelMapSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelMapSpec3 extends LevelMapSpec {
  override def inMemoryStorage = true
}

sealed trait LevelMapSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val ec = TestExecutionContext.executionContext
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  "putMap on a single Level" should {
    import swaydb.core.map.serializer.LevelZeroMapEntryReader._
    import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

    def createTestMap()(implicit sweeper: TestCaseSweeper) = {
      import sweeper._

      implicit val optimiseWrites = OptimiseWrites.random
      implicit val atomic: Atomic = Atomic.random

      val map =
        if (persistent)
          Map.persistent[Slice[Byte], Memory, LevelZeroMapCache](
            folder = randomIntDirectory,
            mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
            flushOnOverflow = true,
            fileSize = 1.mb,
            dropCorruptedTailEntries = false
          ).runRandomIO.right.value.item.sweep()
        else
          Map.memory[Slice[Byte], Memory, LevelZeroMapCache]()

      val keyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addPutDeadlines = false)
      keyValues foreach {
        keyValue =>
          map.writeSync(MapEntry.Put(keyValue.key, keyValue))
      }

      (map, keyValues)
    }

    "succeed" when {
      "writing to an empty Level" in {
        TestCaseSweeper {
          implicit sweeper =>

            val (map, keyValues) = createTestMap()

            val level = TestLevel()
            level.putMap(map).get
            //since this is a new Segment and Level has no sub-level, all the deleted key-values will value removed.
            val (deletedKeyValues, otherKeyValues) = keyValues.partition(_.isInstanceOf[Memory.Remove])

            assertReads(otherKeyValues, level)

            //deleted key-values do not exist.
            deletedKeyValues foreach {
              deleted =>
                level.get(deleted.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
            }
        }
      }

      "writing to a non empty Level" in {
        TestCaseSweeper {
          implicit sweeper =>
            val (map, keyValues) = createTestMap()

            val level = TestLevel()

            //creating a Segment with existing string key-values
            val existingKeyValues = Array(Memory.put("one", "one"), Memory.put("two", "two"), Memory.put("three", "three"))

            val sortedExistingKeyValues =
              Slice(
                Array(
                  //also randomly set expired deadline for Remove.
                  Memory.put("one", "one"), Memory.put("two", "two"), Memory.put("three", "three"), Memory.remove("four", randomly(expiredDeadline()))
                ).sorted(keyOrder.on[KeyValue](_.key)))

            level.put(sortedExistingKeyValues).runRandomIO.right.value

            //put a new map
            level.putMap(map).get
            assertGet(keyValues.filterNot(_.isInstanceOf[Memory.Remove]), level)

            level.get("one", ThreadReadState.random).runRandomIO.right.value.getPut shouldBe existingKeyValues(0)
            level.get("two", ThreadReadState.random).runRandomIO.right.value.getPut shouldBe existingKeyValues(1)
            level.get("three", ThreadReadState.random).runRandomIO.right.value.getPut shouldBe existingKeyValues(2)
            level.get("four", ThreadReadState.random).runRandomIO.right.value.toOptionPut shouldBe empty
        }
      }
    }
  }
}
