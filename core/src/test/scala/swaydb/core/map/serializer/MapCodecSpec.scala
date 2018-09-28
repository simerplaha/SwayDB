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

package swaydb.core.map.serializer

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.core.data._
import swaydb.core.io.file.DBFile
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.Segment
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

class MapCodecSpec extends TestBase {

  override implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  implicit val compression = groupingStrategy
  val appendixReader = AppendixMapEntryReader(false, true, true)

  "MemoryMapCodec" should {
    "write and read empty bytes" in {
      import LevelZeroMapEntryWriter.Level0PutValueWriter
      val map = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
      val bytes = MapCodec.write(map)
      bytes.isFull shouldBe true

      import LevelZeroMapEntryReader.Level0Reader
      MapCodec.read[Slice[Byte], Memory.Response](bytes, dropCorruptedTailEntries = false).assertGet.item shouldBe empty
    }

    "write and read key values" in {
      import LevelZeroMapEntryWriter.Level0PutValueWriter
      val keyValues = randomIntKeyValues(1000, addRandomRemoves = true)
      val map = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
      keyValues foreach {
        keyValue =>
          map.put(keyValue.key, Memory.Put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt))
      }

      val bytes = MapCodec.write(map)
      bytes.isFull shouldBe true

      //re-read the bytes written to map and it should contain all the original entries
      import LevelZeroMapEntryReader.Level0Reader
      val readMap = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
      MapCodec.read[Slice[Byte], Memory.Response](bytes, dropCorruptedTailEntries = false).assertGet.item.assertGet applyTo readMap
      keyValues foreach {
        keyValue =>
          val value = readMap.get(keyValue.key)
          value shouldBe Memory.Put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt)
      }
    }

    "read bytes to map and ignore empty written byte(s)" in {
      val keyValues = randomIntKeyValues(1000, addRandomRemoves = true)
      val map = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
      keyValues foreach {
        keyValue =>
          map.put(keyValue.key, Memory.Put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt))
      }

      def assertBytes(bytesWithEmpty: Slice[Byte]) = {
        //re-read the bytes written to map and it should contain all the original entries
        import LevelZeroMapEntryReader.Level0Reader
        val readMap = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
        MapCodec.read(bytesWithEmpty, dropCorruptedTailEntries = false).assertGet.item.assertGet applyTo readMap
        keyValues foreach {
          keyValue =>
            val value = readMap.get(keyValue.key)
            value shouldBe Memory.Put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt)
        }
      }

      import LevelZeroMapEntryWriter.Level0PutValueWriter
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
      def createKeyValueSkipList(keyValues: Slice[KeyValue.WriteOnly]) = {
        val map = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)
        keyValues foreach {
          keyValue =>
            map.put(keyValue.key, Memory.Put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt))
        }
        map
      }

      val keyValues1 = Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3), Transient.Put(4, 4), Transient.Put(5, 5)).updateStats
      val keyValues2 = Slice(Transient.Put(6, 6), Transient.Put(7, 7), Transient.Put(8, 8), Transient.Put(9, 9), Transient.Put(10, 10)).updateStats
      val allKeyValues = keyValues1 ++ keyValues2

      val skipList1 = createKeyValueSkipList(keyValues1)
      val skipList2 = createKeyValueSkipList(keyValues2)

      import LevelZeroMapEntryWriter.Level0PutValueWriter
      val bytes1 = MapCodec.write(skipList1)
      val bytes2 = MapCodec.write(skipList2)
      //combined the bytes of both the entries so that are in one single file.
      val allBytes = Slice((bytes1 ++ bytes2).toArray)

      val map = new ConcurrentSkipListMap[Slice[Byte], Memory.Response](ordering)

      //re-read allBytes and write it to skipList and it should contain all the original entries
      import LevelZeroMapEntryReader.Level0Reader
      val mapEntry = MapCodec.read(allBytes, dropCorruptedTailEntries = false).assertGet.item.assertGet
      mapEntry applyTo map
      map should have size allKeyValues.size
      allKeyValues foreach {
        keyValue =>
          val value = map.get(keyValue.key)
          value shouldBe Memory.Put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt)
      }

      //corrupt bytes in bytes2 and read the bytes again. keyValues2 should not exist as it's key-values are corrupted.
      val corruptedBytes2: Slice[Byte] = allBytes.dropRight(1)
      MapCodec.read(corruptedBytes2, dropCorruptedTailEntries = false).failed.assertGet shouldBe a[IllegalStateException]
      //enable skip corrupt entries.
      val mapEntryWithTailCorruptionSkipOnCorruption = MapCodec.read(corruptedBytes2, dropCorruptedTailEntries = true).assertGet.item.assertGet
      map.clear()
      mapEntryWithTailCorruptionSkipOnCorruption applyTo map
      map should have size 5 //only one entry is corrupted
      keyValues1 foreach {
        keyValue =>
          val value = map.get(keyValue.key)
          value shouldBe Memory.Put(keyValue.key, keyValue.getOrFetchValue.assertGetOpt)
      }

      //corrupt bytes of bytes1
      val corruptedBytes1: Slice[Byte] = allBytes.drop(1)
      //all bytes are corrupted, failure occurs.
      MapCodec.read(corruptedBytes1, dropCorruptedTailEntries = false).failed.assertGet shouldBe a[IllegalStateException]
      MapCodec.read(corruptedBytes1, dropCorruptedTailEntries = true).assertGet.item shouldBe empty
    }

  }

}
