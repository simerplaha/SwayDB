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
import swaydb.core.data.{KeyValue, Transient, Value}
import swaydb.core.data.Transient.Remove
import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.one.{KeyMatcher, SegmentFooter, SegmentReader, SegmentWriter}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers._
import swaydb.serializers.Default._

import scala.util.Random

class SegmentWriterReaderSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  "SegmentFormat" should {
    "convert empty KeyValues and not throw exception but return empty bytes" in {
      val bytes = SegmentWriter.toSlice(Seq(), 0.1).assertGet
      bytes.isEmpty shouldBe true
    }

    "converting KeyValues to bytes and execute readAll and find on the bytes" in {
      implicit val ordering = KeyOrder.default
      import swaydb.core.map.serializer.RangeValueSerializers._

      def test(keyValues: Slice[KeyValue.WriteOnly]) = {
        val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet
        bytes.isFull shouldBe true
        //in memory
        assertGet(keyValues, Reader(bytes))
        //on disk
        assertGet(keyValues, createFileChannelReader(bytes))
      }

      //testing key-values of various lengths in random positions

      //single key-values
      test(Slice(Transient.Put(1, value = 1)))
      test(Slice(Transient.Put(1)))
      test(Slice(Transient.Remove(1)))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove)))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Remove)))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Remove)))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Put(1))))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Put(1)), Value.Put(10))))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed](1, 10, Some(Value.Remove), Value.Put(10))))

      //single key-values with String keys and values
      test(Slice(Transient.Put("one", value = "one")))
      test(Slice(Transient.Put("one")))
      test(Slice(Transient.Remove("one")))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed]("one", "ten", None, Value.Remove)))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed]("one", "ten", Some(Value.Remove), Value.Remove)))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed]("one", "ten", Some(Value.Put("one")), Value.Remove)))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed]("one", "ten", None, Value.Put("one"))))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed]("one", "ten", Some(Value.Put("one")), Value.Put("ten"))))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed]("one", "ten", Some(Value.Remove), Value.Put("ten"))))

      //single large
      test(Slice(Transient.Put(1, "one one one one one one one one one one one one one one one one one one one one one one")))
      test(Slice(Transient.Put("two two two two two two two two two two two two two two two two two two two two two two two")))
      test(Slice(Transient.Remove("three three three three three three three three three three three three three three three three")))
      test(Slice(Transient.Range[Value.Fixed, Value.Fixed]("one one one one one one one one", "ten ten ten ten ten ten ten ten ten", Some(Value.Put("one one one one")), Value.Put("ten ten ten ten ten ten ten"))))

      //put key values variations
      test(Slice(Transient.Put(1, 1), Transient.Remove(2)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Put(2, 2)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Put(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Put(2)), Value.Put(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Remove), Value.Put(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Put(2)), Value.Remove)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Remove), Value.Remove)).updateStats)

      //remove key values variations
      test(Slice(Transient.Remove(1), Transient.Remove(2)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Put(2, 2)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Put(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Put(2)), Value.Put(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Remove), Value.Put(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Put(2)), Value.Remove)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, Some(Value.Remove), Value.Remove)).updateStats)

      test(Slice(Transient.Remove(1), Transient.Put(2), Transient.Remove(3), Transient.Remove(4), Transient.Range[Value.Fixed, Value.Fixed](5, 10, Some(Value.Put(10)), Value.Remove)).updateStats)

      test(Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Put(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Remove(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Remove(2), Transient.Put(3, 3)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Remove(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Put(2, 2), Transient.Put(3)).updateStats)
      test(
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](1, 10, None, Value.Remove),
          Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Remove), Value.Remove),
          Transient.Range[Value.Fixed, Value.Fixed](20, 30, Some(Value.Put(1)), Value.Remove),
          Transient.Range[Value.Fixed, Value.Fixed](30, 40, None, Value.Put(1)),
          Transient.Range[Value.Fixed, Value.Fixed](40, 50, Some(Value.Put(1)), Value.Put(10)),
          Transient.Range[Value.Fixed, Value.Fixed](50, 60, Some(Value.Remove), Value.Put(10))
        ).updateStats
      )
    }

    "converting two KeyValues with common key bytes to Segment slice and read them" in {

      val keyValues = Slice(Transient.Put("123", value = 1), Transient.Put("1234", value = 2)).updateStats
      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet
      bytes.isFull shouldBe true
      //in memory
      assertReads(keyValues, Reader(bytes))
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "converting large KeyValues to bytes" in {
      def randomChars = Random.alphanumeric.take(10000).mkString

      val keyValues =
        Slice(
          Transient.Put(Slice(s"a$randomChars".getBytes()), value = randomChars),
          Transient.Put(Slice(s"b$randomChars".getBytes()), value = randomChars),
          Transient.Put(Slice(s"c$randomChars".getBytes()), value = randomChars),
          Transient.Put(Slice(s"d$randomChars".getBytes()), value = randomChars)
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
      val keyValues = Slice(Transient.Put(1, 1), Transient.Put(2, 2)).updateStats

      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
      footer.keyValueCount shouldBe keyValues.size
      footer.keyValueCount shouldBe keyValues.size
      footer.startIndexOffset shouldBe keyValues.last.stats.toValueOffset + 1
      //      footer.endIndexOffset shouldBe (bytes.size - Segment.footerSize - 1)
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
    "get key-values using KeyMatcher.Exact" in {
      val keyValues = Slice(Transient.Put(1, "one"), Transient.Put(2, "two")).updateStats
      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      val footer = SegmentReader.readFooter(Reader(bytes)).assertGet

      val foundKeyValue1 = SegmentReader.find(KeyMatcher.Get(keyValues.head.key), None, Reader(bytes)).assertGet
      foundKeyValue1.getOrFetchValue.assertGetOpt shouldBe keyValues.head.getOrFetchValue.assertGetOpt
      //key is a slice of bytes array
      foundKeyValue1.key.underlyingArraySize shouldBe bytes.size
      //value is a slice of bytes array
      foundKeyValue1.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size
      foundKeyValue1.indexOffset shouldBe footer.startIndexOffset

      val foundKeyValue2 = SegmentReader.find(KeyMatcher.Get(keyValues.last.key), None, Reader(bytes)).assertGet
      foundKeyValue2.getOrFetchValue.assertGetOpt shouldBe keyValues.last.getOrFetchValue.assertGetOpt
      //common bytes with previous key-values so here the key will not be a slice of bytes array.
      foundKeyValue2.key.toArray shouldBe keyValues.last.key.toArray
      //value is a slice of bytes array and not
      foundKeyValue2.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size
      foundKeyValue2.indexOffset shouldBe foundKeyValue1.nextIndexOffset
    }

    "get key-values using KeyMatcher.Lower" in {
      val keyValues = Slice(Transient.Put(1, "one"), Transient.Put(2, "two")).updateStats
      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      SegmentReader.find(KeyMatcher.Lower(keyValues.head.key), None, Reader(bytes)).assertGetOpt shouldBe empty

      val foundKeyValue = SegmentReader.find(KeyMatcher.Lower(keyValues.last.key), None, Reader(bytes)).assertGet
      foundKeyValue.getOrFetchValue.assertGetOpt shouldBe keyValues.head.getOrFetchValue.assertGetOpt

      foundKeyValue.key shouldBe keyValues.head.key
      //ensure value is unsliced
      foundKeyValue.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size
    }

    "get key-values using KeyMatcher.Higher" in {
      val keyValues = Slice(Transient.Put(1, "one"), Transient.Put(2, "two")).updateStats
      val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

      val foundKeyValue = SegmentReader.find(KeyMatcher.Higher(keyValues.head.key), None, Reader(bytes)).assertGet
      foundKeyValue.getOrFetchValue.assertGetOpt shouldBe keyValues.last.getOrFetchValue.assertGetOpt
      foundKeyValue.key.toArray shouldBe keyValues.last.key.toArray

      //ensure value is unsliced
      foundKeyValue.getOrFetchValue.assertGet.underlyingArraySize shouldBe bytes.size

      SegmentReader.find(KeyMatcher.Higher(keyValues.last.key), None, Reader(bytes)).assertGetOpt shouldBe empty
      //last key has no higher
      SegmentReader.find(KeyMatcher.Higher(keyValues.last.key), Some(foundKeyValue), Reader(bytes)).assertGetOpt shouldBe empty
    }
  }

}
