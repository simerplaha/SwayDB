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

package swaydb.core.map.serializer

import java.util.concurrent.ConcurrentSkipListMap

import org.scalatest.OptionValues._
import swaydb.core.CommonAssertions._
import swaydb.core.IOValues._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.format.a.block.SegmentIO
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.io.Core.IO.Error.ErrorHandler

class MapCodecSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def compression = randomGroupingStrategyOption(randomNextInt(1000))
  implicit def segmentIO: SegmentIO = SegmentIO.random

  val appendixReader = AppendixMapEntryReader(true, true)

  "MemoryMapCodec" should {
    "write and read empty bytes" in {
      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      val map = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)
      val bytes = MapCodec.write(map)
      bytes.isFull shouldBe true

      import LevelZeroMapEntryReader.Level0Reader
      MapCodec.read[Slice[Byte], Memory.SegmentResponse](bytes, dropCorruptedTailEntries = false).runIO.item shouldBe empty
    }

    "write and read key values" in {
      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      val keyValues = randomKeyValues(1000, addRemoves = true)
      val map = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)
      keyValues foreach {
        keyValue =>
          map.put(keyValue.key, Memory.put(keyValue.key, keyValue.getOrFetchValue))
      }

      val bytes = MapCodec.write(map)
      bytes.isFull shouldBe true

      //re-read the bytes written to map and it should contain all the original entries
      import LevelZeroMapEntryReader.Level0Reader
      val readMap = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)
      MapCodec.read[Slice[Byte], Memory.SegmentResponse](bytes, dropCorruptedTailEntries = false).runIO.item.value applyTo readMap
      keyValues foreach {
        keyValue =>
          val value = readMap.get(keyValue.key)
          value shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
      }
    }

    "read bytes to map and ignore empty written byte(s)" in {
      val keyValues = randomKeyValues(1000, addRemoves = true)
      val map = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)
      keyValues foreach {
        keyValue =>
          map.put(keyValue.key, Memory.put(keyValue.key, keyValue.getOrFetchValue))
      }

      def assertBytes(bytesWithEmpty: Slice[Byte]) = {
        //re-read the bytes written to map and it should contain all the original entries
        import LevelZeroMapEntryReader.Level0Reader
        val readMap = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)
        MapCodec.read(bytesWithEmpty, dropCorruptedTailEntries = false).runIO.item.value applyTo readMap
        keyValues foreach {
          keyValue =>
            val value = readMap.get(keyValue.key)
            value shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
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
      def createKeyValueSkipList(keyValues: Slice[Transient]) = {
        val map = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)
        keyValues foreach {
          keyValue =>
            map.put(keyValue.key, Memory.put(keyValue.key, keyValue.getOrFetchValue))
        }
        map
      }

      val keyValues1 = Slice(Transient.put(1, 1), Transient.put(2, 2), Transient.put(3, 3), Transient.put(4, 4), Transient.put(5, 5)).updateStats
      val keyValues2 = Slice(Transient.put(6, 6), Transient.put(7, 7), Transient.put(8, 8), Transient.put(9, 9), Transient.put(10, 10)).updateStats
      val allKeyValues = keyValues1 ++ keyValues2

      val skipList1 = createKeyValueSkipList(keyValues1)
      val skipList2 = createKeyValueSkipList(keyValues2)

      import LevelZeroMapEntryWriter.Level0MapEntryPutWriter
      val bytes1 = MapCodec.write(skipList1)
      val bytes2 = MapCodec.write(skipList2)
      //combined the bytes of both the entries so that are in one single file.
      val allBytes = Slice((bytes1 ++ bytes2).toArray)

      val map = new ConcurrentSkipListMap[Slice[Byte], Memory.SegmentResponse](keyOrder)

      //re-read allBytes and write it to skipList and it should contain all the original entries
      import LevelZeroMapEntryReader.Level0Reader
      val mapEntry = MapCodec.read(allBytes, dropCorruptedTailEntries = false).runIO.item.value
      mapEntry applyTo map
      map should have size allKeyValues.size
      allKeyValues foreach {
        keyValue =>
          val value = map.get(keyValue.key)
          value shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
      }

      //corrupt bytes in bytes2 and read the bytes again. keyValues2 should not exist as it's key-values are corrupted.
      val corruptedBytes2: Slice[Byte] = allBytes.dropRight(1)
      MapCodec.read(corruptedBytes2, dropCorruptedTailEntries = false).failed.runIO.exception shouldBe a[IllegalStateException]
      //enable skip corrupt entries.
      val mapEntryWithTailCorruptionSkipOnCorruption = MapCodec.read(corruptedBytes2, dropCorruptedTailEntries = true).runIO.item.value
      map.clear()
      mapEntryWithTailCorruptionSkipOnCorruption applyTo map
      map should have size 5 //only one entry is corrupted
      keyValues1 foreach {
        keyValue =>
          val value = map.get(keyValue.key)
          value shouldBe Memory.put(keyValue.key, keyValue.getOrFetchValue)
      }

      //corrupt bytes of bytes1
      val corruptedBytes1: Slice[Byte] = allBytes.drop(1)
      //all bytes are corrupted, failure occurs.
      MapCodec.read(corruptedBytes1, dropCorruptedTailEntries = false).failed.runIO.exception shouldBe a[IllegalStateException]
      MapCodec.read(corruptedBytes1, dropCorruptedTailEntries = true).runIO.item shouldBe empty
    }
  }
}
