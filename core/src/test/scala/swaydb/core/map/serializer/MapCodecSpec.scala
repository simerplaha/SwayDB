/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.map.serializer


import org.scalatest.OptionValues._
import swaydb.Error.Map.ExceptionHandler
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data._
import swaydb.core.io.file.BlockCache
import swaydb.core.segment.format.a.block.SegmentIO
import swaydb.core.util.SkipList
import swaydb.core.{TestBase, TestSweeper, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.serializers.Default._
import swaydb.serializers._

class MapCodecSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeper.Enabled = TestSweeper.fileSweeper
  implicit val memorySweeperImplicitSweeper: Option[MemorySweeper.All] = TestSweeper.memorySweeperMax
  implicit def blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO: SegmentIO = SegmentIO.random

  val appendixReader = AppendixMapEntryReader(true, true)

  "MemoryMapCodec" should {
    "write and read empty bytes" in {
      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      val map = SkipList.concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      val bytes = MapCodec.write(map)
      bytes.isFull shouldBe true

      import LevelZeroMapEntryReader.Level0Reader
      MapCodec.read[Slice[Byte], Memory](bytes, dropCorruptedTailEntries = false).runRandomIO.right.value.item shouldBe empty
    }

    "write and read key values" in {
      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      val keyValues = randomKeyValues(1000, addRemoves = true)
      val map = SkipList.concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      keyValues foreach {
        keyValue =>
          map.put(keyValue.key, Memory.put(keyValue.key, keyValue.getOrFetchValue))
      }

      val bytes = MapCodec.write(map)
      bytes.isFull shouldBe true

      //re-read the bytes written to map and it should contain all the original entries
      import LevelZeroMapEntryReader.Level0Reader
      val readMap = SkipList.concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      MapCodec.read[Slice[Byte], Memory](bytes, dropCorruptedTailEntries = false).runRandomIO.right.value.item.value applyTo readMap
      keyValues foreach {
        keyValue =>
          val value = readMap.get(keyValue.key)
          value.getUnsafe shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
      }
    }

    "read bytes to map and ignore empty written byte(s)" in {
      val keyValues = randomKeyValues(1000, addRemoves = true)
      val map = SkipList.concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      keyValues foreach {
        keyValue =>
          map.put(keyValue.key, Memory.put(keyValue.key, keyValue.getOrFetchValue))
      }

      def assertBytes(bytesWithEmpty: Slice[Byte]) = {
        //re-read the bytes written to map and it should contain all the original entries
        import LevelZeroMapEntryReader.Level0Reader
        val readMap = SkipList.concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        MapCodec.read(bytesWithEmpty, dropCorruptedTailEntries = false).runRandomIO.right.value.item.value applyTo readMap
        keyValues foreach {
          keyValue =>
            val value = readMap.get(keyValue.key)
            value.getUnsafe shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
        }
      }

      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      //first write creates bytes that have no empty bytes
      val bytes = MapCodec.write(map)
      bytes.isFull shouldBe true

      //1 empty byte.
      val bytesWith1EmptyByte = Slice.create[Byte](bytes.size + 1)
      bytes foreach bytesWith1EmptyByte.add
      bytesWith1EmptyByte.isFull shouldBe false
      assertBytes(bytesWith1EmptyByte)

      //10 empty bytes. This is to check that key-values are still read if there are enough bytes to create a CRC
      val bytesWith20EmptyByte = Slice.create[Byte](bytes.size + 10)
      bytes foreach bytesWith20EmptyByte.add
      bytesWith20EmptyByte.isFull shouldBe false
      assertBytes(bytesWith20EmptyByte)
    }

    "only skip entries that are do not pass the CRC check if skipOnCorruption is true" in {
      def createKeyValueSkipList(keyValues: Slice[Memory]) = {
        val map = SkipList.concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        keyValues foreach {
          keyValue =>
            map.put(keyValue.key, Memory.put(keyValue.key, keyValue.getOrFetchValue))
        }
        map
      }

      val keyValues1 = Slice(Memory.put(1, 1), Memory.put(2, 2), Memory.put(3, 3), Memory.put(4, 4), Memory.put(5, 5))
      val keyValues2 = Slice(Memory.put(6, 6), Memory.put(7, 7), Memory.put(8, 8), Memory.put(9, 9), Memory.put(10, 10))
      val allKeyValues = keyValues1 ++ keyValues2

      val skipList1 = createKeyValueSkipList(keyValues1)
      val skipList2 = createKeyValueSkipList(keyValues2)

      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      val bytes1 = MapCodec.write(skipList1)
      val bytes2 = MapCodec.write(skipList2)
      //combined the bytes of both the entries so that are in one single file.
      val allBytes = Slice((bytes1 ++ bytes2).toArray)

      val map = SkipList.concurrent[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)

      //re-read allBytes and write it to skipList and it should contain all the original entries
      import LevelZeroMapEntryReader.Level0Reader
      val mapEntry = MapCodec.read(allBytes, dropCorruptedTailEntries = false).runRandomIO.right.value.item.value
      mapEntry applyTo map
      map should have size allKeyValues.size
      allKeyValues foreach {
        keyValue =>
          val value = map.get(keyValue.key)
          value.getUnsafe shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
      }

      //corrupt bytes in bytes2 and read the bytes again. keyValues2 should not exist as it's key-values are corrupted.
      val corruptedBytes2: Slice[Byte] = allBytes.dropRight(1)
      MapCodec.read(corruptedBytes2, dropCorruptedTailEntries = false).left.runRandomIO.right.value.exception shouldBe a[IllegalStateException]
      //enable skip corrupt entries.
      val mapEntryWithTailCorruptionSkipOnCorruption = MapCodec.read(corruptedBytes2, dropCorruptedTailEntries = true).runRandomIO.right.value.item.value
      map.clear()
      mapEntryWithTailCorruptionSkipOnCorruption applyTo map
      map should have size 5 //only one entry is corrupted
      keyValues1 foreach {
        keyValue =>
          val value = map.get(keyValue.key).getUnsafe
          value shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
      }

      //corrupt bytes of bytes1
      val corruptedBytes1: Slice[Byte] = allBytes.drop(1)
      //all bytes are corrupted, failure occurs.
      MapCodec.read(corruptedBytes1, dropCorruptedTailEntries = false).left.runRandomIO.right.value.exception shouldBe a[IllegalStateException]
      MapCodec.read(corruptedBytes1, dropCorruptedTailEntries = true).runRandomIO.right.value.item shouldBe empty
    }
  }
}
