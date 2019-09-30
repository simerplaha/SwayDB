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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.map

import org.scalatest.OptionValues._
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.actor.FileSweeper
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.map.serializer.LevelZeroMapEntryWriter.Level0PutWriter
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class MapStressSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  implicit val skipListMerger = LevelZeroSkipListMerger
  implicit val fileSweeper: FileSweeper.Enabled = TestLimitQueues.fileSweeper
  implicit val memorySweeper = TestLimitQueues.memorySweeperMax
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  implicit def testTimer: TestTimer = TestTimer.Empty

  "Map" should {
    "write entries when flushOnOverflow is true and map size is 1.kb" in {
      val keyValues = randomKeyValues(100)

      def test(map: Map[Slice[Byte], Memory]) = {
        keyValues foreach {
          keyValue =>
            val entry = MapEntry.Put[Slice[Byte], Memory.Put](keyValue.key, Memory.put(keyValue.key, keyValue.getOrFetchValue))(Level0PutWriter)
            map.write(entry).runRandomIO.right.value shouldBe true
        }

        testRead(map)
      }

      def testRead(map: Map[Slice[Byte], Memory]) =
        keyValues foreach {
          keyValue =>
            map.skipList.get(keyValue.key).value shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
        }

      val dir1 = createRandomDir
      val dir2 = createRandomDir

      import swaydb.core.map.serializer.LevelZeroMapEntryReader.Level0Reader
      import swaydb.core.map.serializer.LevelZeroMapEntryWriter.Level0MapEntryPutWriter

      test(Map.persistent[Slice[Byte], Memory](dir1, mmap = true, flushOnOverflow = true, 1.kb, dropCorruptedTailEntries = false).runRandomIO.right.value.item)
      test(Map.persistent[Slice[Byte], Memory](dir2, mmap = false, flushOnOverflow = true, 1.kb, dropCorruptedTailEntries = false).runRandomIO.right.value.item)
      test(Map.memory[Slice[Byte], Memory](flushOnOverflow = true, fileSize = 1.kb))

      //reopen - all the entries should value recovered for persistent maps. Also switch mmap types.
      testRead(Map.persistent[Slice[Byte], Memory](dir1, mmap = false, flushOnOverflow = true, 1.kb, dropCorruptedTailEntries = false).runRandomIO.right.value.item)
      testRead(Map.persistent[Slice[Byte], Memory](dir2, mmap = true, flushOnOverflow = true, 1.kb, dropCorruptedTailEntries = false).runRandomIO.right.value.item)

      //write the same data again
      test(Map.persistent[Slice[Byte], Memory](dir1, mmap = true, flushOnOverflow = true, 1.kb, dropCorruptedTailEntries = false).runRandomIO.right.value.item)
      test(Map.persistent[Slice[Byte], Memory](dir2, mmap = false, flushOnOverflow = true, 1.kb, dropCorruptedTailEntries = false).runRandomIO.right.value.item)

      //read again
      testRead(Map.persistent[Slice[Byte], Memory](dir1, mmap = false, flushOnOverflow = true, 1.kb, dropCorruptedTailEntries = false).runRandomIO.right.value.item)
      testRead(Map.persistent[Slice[Byte], Memory](dir2, mmap = true, flushOnOverflow = true, 1.kb, dropCorruptedTailEntries = false).runRandomIO.right.value.item)
    }
  }
}
