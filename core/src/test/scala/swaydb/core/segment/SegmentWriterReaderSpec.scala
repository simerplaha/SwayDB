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

package swaydb.core.segment

import swaydb.core.TestBase
import swaydb.core.data.{KeyValue, Persistent, Transient, Value}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.one.{KeyMatcher, SegmentFooter, SegmentReader, SegmentWriter}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteUtil
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.util.Random

class SegmentWriterReaderSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  import swaydb.core.map.serializer.RangeValueSerializers._

  "SegmentFormat" should {
    "convert empty KeyValues and not throw exception but return empty bytes" in {
      val bytes = SegmentWriter.toSlice(Seq(), 0.1).assertGet
      bytes.isEmpty shouldBe true
    }

    "converting KeyValues to bytes and execute readAll and find on the bytes" in {
      implicit val ordering = KeyOrder.default

      def test(keyValues: Slice[KeyValue.WriteOnly]) = {
        val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet
        bytes.isFull shouldBe true
        //in memory
        assertReads(keyValues, Reader(bytes))
        //on disk
        assertReads(keyValues, createFileChannelReader(bytes))
      }

      //testing key-values of various lengths in random positions

      //single key-values
      test(Slice(Transient.Put(1, value = 1)))
      test(Slice(Transient.Put(1)))
      test(Slice(Transient.Remove(1)))
      test(Slice(Transient.Range[Value, Value](1, 10, None, Value.Remove)))
      test(Slice(Transient.Range[Value, Value](1, 10, Some(Value.Remove), Value.Remove)))
      test(Slice(Transient.Range[Value, Value](1, 10, Some(Value.Put(1)), Value.Remove)))
      test(Slice(Transient.Range[Value, Value](1, 10, None, Value.Put(1))))
      test(Slice(Transient.Range[Value, Value](1, 10, Some(Value.Put(1)), Value.Put(10))))
      test(Slice(Transient.Range[Value, Value](1, 10, Some(Value.Remove), Value.Put(10))))

      //single key-values with String keys and values
      test(Slice(Transient.Put("one", value = "one")))
      test(Slice(Transient.Put("one")))
      test(Slice(Transient.Remove("one")))
      test(Slice(Transient.Range[Value, Value]("one", "ten", None, Value.Remove)))
      test(Slice(Transient.Range[Value, Value]("one", "ten", Some(Value.Remove), Value.Remove)))
      test(Slice(Transient.Range[Value, Value]("one", "ten", Some(Value.Put("one")), Value.Remove)))
      test(Slice(Transient.Range[Value, Value]("one", "ten", None, Value.Put("one"))))
      test(Slice(Transient.Range[Value, Value]("one", "ten", Some(Value.Put("one")), Value.Put("ten"))))
      test(Slice(Transient.Range[Value, Value]("one", "ten", Some(Value.Remove), Value.Put("ten"))))

      //single large
      test(Slice(Transient.Put(1, "one" * 10)))
      test(Slice(Transient.Put("two" * 10)))
      test(Slice(Transient.Remove("three" * 10)))
      test(Slice(Transient.Range[Value, Value]("one" * 10, "ten" * 10, Some(Value.Put("one" * 10)), Value.Put("ten" * 10))))

      //put key values variations
      test(Slice(Transient.Put(1, 1), Transient.Remove(2)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Put(2, 2)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value, Value](2, 10, None, Value.Put(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value, Value](2, 10, Some(Value.Put(2)), Value.Put(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value, Value](2, 10, Some(Value.Remove), Value.Put(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value, Value](2, 10, None, Value.Remove)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value, Value](2, 10, Some(Value.Put(2)), Value.Remove)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value, Value](2, 10, Some(Value.Remove), Value.Remove)).updateStats)

      //remove key values variations
      test(Slice(Transient.Remove(1), Transient.Remove(2)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Put(2, 2)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value, Value](2, 10, None, Value.Put(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value, Value](2, 10, Some(Value.Put(2)), Value.Put(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value, Value](2, 10, Some(Value.Remove), Value.Put(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value, Value](2, 10, None, Value.Remove)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value, Value](2, 10, Some(Value.Put(2)), Value.Remove)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value, Value](2, 10, Some(Value.Remove), Value.Remove)).updateStats)

      test(Slice(Transient.Remove(1), Transient.Put(2), Transient.Remove(3), Transient.Remove(4), Transient.Range[Value, Value](5, 10, Some(Value.Put(10)), Value.Remove)).updateStats)

      test(Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Put(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Remove(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Remove(2), Transient.Put(3, 3)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Remove(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Put(2, 2), Transient.Put(3)).updateStats)
      test(
        Slice(
          Transient.Range[Value, Value](1, 10, None, Value.Remove),
          Transient.Range[Value, Value](10, 20, Some(Value.Remove), Value.Remove),
          Transient.Range[Value, Value](20, 30, Some(Value.Put(1)), Value.Remove),
          Transient.Range[Value, Value](30, 40, None, Value.Put(1)),
          Transient.Range[Value, Value](40, 50, Some(Value.Put(1)), Value.Put(10)),
          Transient.Range[Value, Value](50, 60, Some(Value.Remove), Value.Put(10))
        ).updateStats
      )
    }

    "compress write and read key-values with common bytes" in {
      val keyValues = Slice(
        Transient.Put(12),
        Transient.Put(120),
        Transient.Remove(121),
        Transient.Put(122),
        Transient.Remove(123)
      ).updateStats

      keyValues.head.stats.commonBytes shouldBe 0
      keyValues.drop(1) foreach (_.stats.commonBytes shouldBe 3)

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet
      bytes.isFull shouldBe true
      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "compress write and read range key-values with common bytes" in {
      val key1: Slice[Byte] = 12
      val key2: Slice[Byte] = 120
      val key3: Slice[Byte] = 121
      val key4: Slice[Byte] = 122
      val key5: Slice[Byte] = 123
      val key6: Slice[Byte] = 124
      val key7: Slice[Byte] = 125

      val keyValues = Slice(
        Transient.Range[Value, Value](key1, key2, None, Value.Put(10)),
        Transient.Range[Value, Value](key2, key3, Some(Value.Put(10)), Value.Put(10)),
        Transient.Range[Value, Value](key3, key4, Some(Value.Remove), Value.Put(10)),
        Transient.Range[Value, Value](key4, key5, None, Value.Remove),
        Transient.Range[Value, Value](key5, key6, Some(Value.Put(10)), Value.Remove),
        Transient.Range[Value, Value](key6, key7, Some(Value.Remove), Value.Remove)
      ).updateStats

      keyValues.head.stats.commonBytes shouldBe 0
      keyValues.drop(1) foreach (_.stats.commonBytes shouldBe 3)

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet
      bytes.isFull shouldBe true
      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "converting large KeyValues to bytes" in {
      def randomChars = Random.alphanumeric.take(10000).mkString

      val keyValues =
        Slice(
          Transient.Put(Slice(s"a$randomChars".getBytes()), value = randomChars),
          Transient.Remove(Slice(s"b$randomChars".getBytes())),
          Transient.Put(Slice(s"c$randomChars".getBytes()), value = randomChars),
          Transient.Range[Value, Value](fromKey = Slice(s"d$randomChars".getBytes()), toKey = Slice(s"e$randomChars".getBytes()), None, Value.Put(randomChars)),
          Transient.Range[Value, Value](fromKey = Slice(s"f$randomChars".getBytes()), toKey = Slice(s"g$randomChars".getBytes()), Some(Value.Put(randomChars)), Value.Put(randomChars)),
          Transient.Range[Value, Value](fromKey = Slice(s"h$randomChars".getBytes()), toKey = Slice(s"i$randomChars".getBytes()), Some(Value.Remove), Value.Put(randomChars))
        ).updateStats

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet
      bytes.isFull shouldBe true

      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "write and read Int min max key values" in {
      val keyValues = Slice(Transient.Put(Int.MaxValue, Int.MinValue), Transient.Put(Int.MinValue, Int.MaxValue)).updateStats

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "write and read 100 KeyValues to a Slice[Byte]" in {
      val keyValues = randomIntKeyValues(100)
      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "write and read 100 Keys with None value to a Slice[Byte]" in {
      val keyValues = randomIntKeys(100)

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "read footer" in {
      val keyValues = Slice(Transient.Put(1, 1), Transient.Remove(2), Transient.Range[Value, Value](3, 4, Some(Value.Put(3)), Value.Remove)).updateStats

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
      footer.keyValueCount shouldBe keyValues.size
      footer.keyValueCount shouldBe keyValues.size
      footer.startIndexOffset shouldBe keyValues.last.stats.toValueOffset + 1
    }

    "report Segment corruption is CRC check does not match when reading the footer" in {
      val keyValues = Slice(Transient.Put(1)).updateStats

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      SegmentReader.readFooter(Reader(bytes.drop(1))).failed.assertGet shouldBe a[SegmentCorruptionException]
      SegmentReader.readFooter(Reader(bytes.dropRight(1))).failed.assertGet shouldBe a[SegmentCorruptionException]
      SegmentReader.readFooter(Reader(bytes.slice(10, 20))).failed.assertGet shouldBe a[SegmentCorruptionException]
    }
  }

  "SegmentReader.find" should {
    "get key-values using KeyMatcher.Get" in {
      val keyValues =
        Slice(
          Transient.Put(1, "one"),
          Transient.Put(2, "two"),
          Transient.Remove(Int.MaxValue - 10),
          Transient.Range[Value, Value](Int.MaxValue - 9, Int.MaxValue, None, Value.Put(10))
        ).updateStats

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet
      val footer = SegmentReader.readFooter(Reader(bytes)).assertGet

      //FIRST KEY
      val foundKeyValue1 = SegmentReader.find(KeyMatcher.Get(keyValues.head.key), None, Reader(bytes)).assertGet
      foundKeyValue1.getOrFetchValue.assertGetOpt shouldBe keyValues.head.getOrFetchValue.assertGetOpt
      //key is a slice of bytes array
      foundKeyValue1.key.underlyingArraySize shouldBe bytes.size
      //value is a slice of bytes array
      foundKeyValue1.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size
      foundKeyValue1.indexOffset shouldBe footer.startIndexOffset

      //SECOND KEY
      val foundKeyValue2 = SegmentReader.find(KeyMatcher.Get(keyValues(1).key), None, Reader(bytes)).assertGet
      foundKeyValue2.getOrFetchValue.assertGetOpt shouldBe keyValues(1).getOrFetchValue.assertGetOpt
      //common bytes with previous key-values so here the key will not be a slice of bytes array.
      foundKeyValue2.key.underlyingArraySize shouldBe 4
      foundKeyValue2.key.toArray shouldBe keyValues(1).key.toArray
      //value is a slice of bytes array and not
      foundKeyValue2.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size
      foundKeyValue2.indexOffset shouldBe foundKeyValue1.nextIndexOffset

      //THIRD KEY
      val foundKeyValue3 = SegmentReader.find(KeyMatcher.Get(keyValues(2).key), None, Reader(bytes)).assertGet
      foundKeyValue3.getOrFetchValue.assertGetOpt shouldBe keyValues(2).getOrFetchValue.assertGetOpt
      //3 does not have any common shared bytes with the previous key-value and will not be unsliced.
      foundKeyValue3.key.underlyingArraySize shouldBe bytes.size
      foundKeyValue3.key.toArray shouldBe keyValues(2).key.toArray
      //value is a slice of bytes array and not
      foundKeyValue3.indexOffset shouldBe foundKeyValue2.nextIndexOffset

      //FOURTH KEY
      val foundKeyValue4 = SegmentReader.find(KeyMatcher.Get(keyValues(3).key), None, Reader(bytes)).assertGet.asInstanceOf[Persistent.Range]
      foundKeyValue4.getOrFetchValue.assertGetOpt shouldBe keyValues(3).getOrFetchValue.assertGetOpt
      foundKeyValue4.fromKey shouldBe (Int.MaxValue - 9: Slice[Byte])
      foundKeyValue4.toKey shouldBe (Int.MaxValue: Slice[Byte])
      //4 has common bytes with 3rd key-value. It will be sliced.
      foundKeyValue4.key.underlyingArraySize shouldBe 8
      foundKeyValue4.fromKey.underlyingArraySize shouldBe 8 //fromKey is unsliced
      foundKeyValue4.toKey.underlyingArraySize shouldBe 4 //toKey shares common bytes with fromKey so it will be unsliced.

      foundKeyValue4.key.toArray shouldBe keyValues(3).key.toArray
      //value is a slice of bytes array and not
      foundKeyValue4.indexOffset shouldBe foundKeyValue3.nextIndexOffset
    }

    "get key-values using KeyMatcher.Lower" in {
      val keyValues =
        Slice(
          Transient.Put(1, "one"),
          Transient.Put(2, "two"),
          Transient.Remove(Int.MaxValue - 10),
          Transient.Range[Value, Value](Int.MaxValue - 9, Int.MaxValue, None, Value.Put(10))
        ).updateStats

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      //FIRST
      SegmentReader.find(KeyMatcher.Lower(keyValues.head.key), None, Reader(bytes)).assertGetOpt shouldBe empty

      //SECOND
      val foundKeyValue2 = SegmentReader.find(KeyMatcher.Lower(keyValues(1).key), None, Reader(bytes)).assertGet
      foundKeyValue2.getOrFetchValue.assertGetOpt shouldBe keyValues.head.getOrFetchValue.assertGetOpt
      foundKeyValue2.key shouldBe keyValues.head.key
      //ensure value is unsliced
      foundKeyValue2.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size

      //THIRD
      val foundKeyValue3 = SegmentReader.find(KeyMatcher.Lower(keyValues(2).key), None, Reader(bytes)).assertGet
      foundKeyValue3.getOrFetchValue.assertGetOpt shouldBe keyValues(1).getOrFetchValue.assertGetOpt
      foundKeyValue3.key shouldBe keyValues(1).key
      //ensure value is unsliced
      foundKeyValue3.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size

      //FOURTH
      val fourth = keyValues(3).asInstanceOf[Transient.Range]
      val foundKeyValue4FromKey = SegmentReader.find(KeyMatcher.Lower(fourth.fromKey), None, Reader(bytes)).assertGet
      foundKeyValue4FromKey.getOrFetchValue.assertGetOpt shouldBe empty //lower is Remove
      foundKeyValue4FromKey.key shouldBe keyValues(2).key

      val foundKeyValue4ToKey = SegmentReader.find(KeyMatcher.Lower(fourth.toKey), None, Reader(bytes)).assertGet.asInstanceOf[Persistent.Range]
      foundKeyValue4ToKey.getOrFetchValue.assertGetOpt shouldBe fourth.getOrFetchValue.assertGetOpt //lower is Self
      foundKeyValue4ToKey.fromKey shouldBe fourth.fromKey
      foundKeyValue4ToKey.toKey shouldBe fourth.toKey
    }

    "get key-values using KeyMatcher.Higher" in {
      val keyValues =
        Slice(
          Transient.Put(1, "one"),
          Transient.Put(2, "two"),
          Transient.Remove(Int.MaxValue - 10),
          Transient.Range[Value, Value](Int.MaxValue - 9, Int.MaxValue, None, Value.Put(10))
        ).updateStats

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      val foundKeyValue1 = SegmentReader.find(KeyMatcher.Higher(keyValues.head.key), None, Reader(bytes)).assertGet
      foundKeyValue1.getOrFetchValue.assertGetOpt shouldBe keyValues(1).getOrFetchValue.assertGetOpt
      foundKeyValue1.key.toArray shouldBe keyValues(1).key.toArray
      //ensure value is unsliced
      foundKeyValue1.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size

      //SECOND
      val foundKeyValue2 = SegmentReader.find(KeyMatcher.Higher(keyValues(1).key), None, Reader(bytes)).assertGet
      foundKeyValue2.getOrFetchValue.assertGetOpt shouldBe empty
      foundKeyValue2.key shouldBe keyValues(2).key
      //ensure value is unsliced

      //THIRD
      val foundKeyValue3 = SegmentReader.find(KeyMatcher.Higher(keyValues(2).key), None, Reader(bytes)).assertGet
      foundKeyValue3.getOrFetchValue.assertGetOpt shouldBe keyValues(3).getOrFetchValue.assertGetOpt
      foundKeyValue3.key shouldBe keyValues(3).key
      //ensure value is unsliced
      foundKeyValue3.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size

      //FOURTH
      val fourth = keyValues(3).asInstanceOf[Transient.Range]
      val foundKeyValue4FromKey = SegmentReader.find(KeyMatcher.Higher(fourth.fromKey), None, Reader(bytes)).assertGet.asInstanceOf[Persistent.Range]
      foundKeyValue4FromKey.getOrFetchValue.assertGetOpt shouldBe fourth.getOrFetchValue.assertGetOpt //lower is Remove
      foundKeyValue4FromKey.fromKey shouldBe fourth.fromKey
      foundKeyValue4FromKey.toKey shouldBe fourth.toKey

      SegmentReader.find(KeyMatcher.Higher(fourth.toKey), None, Reader(bytes)).assertGetOpt shouldBe empty
    }
  }

}
