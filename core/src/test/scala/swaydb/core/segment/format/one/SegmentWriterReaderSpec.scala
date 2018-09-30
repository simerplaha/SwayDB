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

package swaydb.core.segment.format.one

import swaydb.core.TestBase
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data._
import swaydb.core.group.compression.GroupCompressor
import swaydb.core.io.reader.Reader
import swaydb.core.segment.Segment
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.util.Random

class SegmentWriterReaderSpec extends TestBase {

  override implicit val ordering = KeyOrder.default

  val keyValueCount = 100

  import swaydb.core.map.serializer.RangeValueSerializers._

  "SegmentFormat" should {
    "convert empty KeyValues and not throw exception but return empty bytes" in {
      val (bytes, nearestDeadline) = SegmentWriter.write(Seq(), 0.1).assertGet
      bytes.isEmpty shouldBe true
      nearestDeadline shouldBe empty
    }

    "converting KeyValues to bytes and execute readAll and find on the bytes" in {
      implicit val ordering = KeyOrder.default

      def test(keyValues: Slice[KeyValue.WriteOnly]) = {
        val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet
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
      test(Slice(Transient.Put(1, 10.seconds)))
      test(Slice(Transient.Remove(1)))
      test(Slice(Transient.Remove(1, 50.seconds, 0.1)))
      test(Slice(Transient.Update(1, 10)))

      test(Slice(Transient.Range[FromValue, RangeValue](1, 10, None, Value.Remove(None))))
      test(Slice(Transient.Range[FromValue, RangeValue](1, 10, Some(Value.Remove(None)), Value.Remove(None))))
      test(Slice(Transient.Range[FromValue, RangeValue](1, 10, Some(Value.Put(1)), Value.Remove(None))))
      test(Slice(Transient.Range[FromValue, RangeValue](1, 10, None, Value.Update(1))))
      test(Slice(Transient.Range[FromValue, RangeValue](1, 10, Some(Value.Put(1)), Value.Update(10))))
      test(Slice(Transient.Range[FromValue, RangeValue](1, 10, Some(Value.Remove(None)), Value.Update(10))))
      test(Slice(Transient.Group(Slice(Transient.Put(1, value = 1)), randomCompression(), randomCompression(), 0.1, None).assertGet))
      test(Slice(Transient.Group(Slice(Transient.Remove(1, 10.seconds, 0.1)), randomCompression(), randomCompression(), 0.1, None).assertGet))
      test(Slice(Transient.Group(Slice(Transient.Update(1, 1.seconds)), randomCompression(), randomCompression(), 0.1, None).assertGet))

      //      //single key-values with String keys and values
      test(Slice(Transient.Put("one", value = "one", 10.seconds)))
      test(Slice(Transient.Put("one")))
      test(Slice(Transient.Remove("one")))
      test(Slice(Transient.Range[FromValue, RangeValue]("one", "ten", None, Value.Remove(None))))
      test(Slice(Transient.Range[FromValue, RangeValue]("one", "ten", Some(Value.Remove(None)), Value.Remove(None))))
      test(Slice(Transient.Range[FromValue, RangeValue]("one", "ten", Some(Value.Put("one")), Value.Remove(None))))
      test(Slice(Transient.Range[FromValue, RangeValue]("one", "ten", None, Value.Update("one"))))
      test(Slice(Transient.Range[FromValue, RangeValue]("one", "ten", Some(Value.Put("one")), Value.Update("ten"))))
      test(Slice(Transient.Range[FromValue, RangeValue]("one", "ten", Some(Value.Remove(None)), Value.Update("ten"))))
      test(Slice(Transient.Group(Slice(Transient.Put("one"), Transient.Put("ten")).updateStats, randomCompression(), randomCompression(), 0.1, None).assertGet))
      test(Slice(Transient.Group(Slice(Transient.Remove("one", 10.seconds, 0.1), Transient.Remove("ten", 2.seconds, 0.1)).updateStats, randomCompression(), randomCompression(), 0.1, None).assertGet))
      test(Slice(Transient.Group(Slice(Transient.Update("one", 1.seconds), Transient.Remove("ten")).updateStats, randomCompression(), randomCompression(), 0.1, None).assertGet))

      //single large
      test(Slice(Transient.Put(1, "one" * 10)))
      test(Slice(Transient.Put("two" * 10)))
      test(Slice(Transient.Remove("three" * 10)))
      test(Slice(Transient.Range[FromValue, RangeValue]("one" * 10, "ten" * 10, Some(Value.Put("one" * 10)), Value.Update("ten" * 10))))
      test(Slice(Transient.Group(Slice(Transient.Put("one" * 10), Transient.Put("ten" * 10)).updateStats, randomCompression(), randomCompression(), 0.1, None).assertGet))

      //put key values variations
      test(Slice(Transient.Put(1, 1), Transient.Remove(2)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Put(2, 2)).updateStats)
      test(Slice(Transient.Put(1, 1, 1.day), Transient.Remove(2, 2.days, 0.1)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[FromValue, RangeValue](2, 10, None, Value.Update(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[FromValue, RangeValue](2, 10, Some(Value.Put(2)), Value.Update(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[FromValue, RangeValue](2, 10, Some(Value.Remove(None)), Value.Update(10))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[FromValue, RangeValue](2, 10, None, Value.Remove(None))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[FromValue, RangeValue](2, 10, Some(Value.Put(2)), Value.Remove(None))).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Range[FromValue, RangeValue](2, 10, Some(Value.Remove(None)), Value.Remove(None))).updateStats)

      //remove key values variations
      test(Slice(Transient.Remove(1), Transient.Remove(2)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Put(2, 2)).updateStats)
      test(Slice(Transient.Remove(1, 1.day, 0.1), Transient.Put(2, 2, 1.day)).updateStats)
      test(Slice(Transient.Remove(1, 10000.days, 0.1), Transient.Remove(2, 10000.days, 0.1)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[FromValue, RangeValue](2, 10, None, Value.Update(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[FromValue, RangeValue](2, 10, Some(Value.Update(2)), Value.Update(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[FromValue, RangeValue](2, 10, Some(Value.Remove(None)), Value.Update(10))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[FromValue, RangeValue](2, 10, None, Value.Remove(None))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[FromValue, RangeValue](2, 10, Some(Value.Put(2)), Value.Remove(None))).updateStats)
      test(Slice(Transient.Remove(1), Transient.Range[FromValue, RangeValue](2, 10, Some(Value.Remove(None)), Value.Remove(None))).updateStats)

      runThis(10.times) {
        test(Slice(Transient.Group(randomizedIntKeyValues(keyValueCount, startId = Some(1)), randomCompression(), randomCompression(), 0.1, None).assertGet))
      }

      test(Slice(Transient.Remove(1), Transient.Put(2), Transient.Remove(3), Transient.Remove(4), Transient.Range[FromValue, RangeValue](5, 10, Some(Value.Put(10)), Value.Remove(None))).updateStats)

      test(Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Put(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Remove(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Put(1), Transient.Remove(2), Transient.Put(3, 3)).updateStats)
      test(Slice(Transient.Put(1, 1), Transient.Remove(2), Transient.Put(3)).updateStats)
      test(Slice(Transient.Remove(1), Transient.Put(2, 2), Transient.Put(3)).updateStats)
      test(
        Slice(
          Transient.Range[FromValue, RangeValue](1, 10, None, Value.Remove(None)),
          Transient.Range[FromValue, RangeValue](10, 20, Some(Value.Remove(None)), Value.Remove(None)),
          Transient.Range[FromValue, RangeValue](20, 30, Some(Value.Put(1)), Value.Remove(None)),
          Transient.Range[FromValue, RangeValue](30, 40, None, Value.Update(1)),
          Transient.Range[FromValue, RangeValue](40, 50, Some(Value.Put(1)), Value.Update(10)),
          Transient.Range[FromValue, RangeValue](50, 60, Some(Value.Remove(None)), Value.Update(10))
        ).updateStats
      )
    }

    "write and read a group" in {
      val groupKeyValues = randomIntKeyValues(keyValueCount)
      val group = Transient.Group(groupKeyValues, randomCompression(), randomCompression(), 0.1, None).assertGet

      val (bytes, deadline) = SegmentWriter.write(Seq(group), 0.1).assertGet
      bytes.isFull shouldBe true

      val allKeyValuesForGroups = readAll(bytes).assertGet.asInstanceOf[Slice[KeyValue.ReadOnly.Group]].flatMap(_.segmentCache.getAll().assertGet)
      allKeyValuesForGroups shouldBe groupKeyValues.toMemory
    }

    "write two sibling groups" in {
      val group1KeyValues = randomizedIntKeyValues(keyValueCount)
      val group1 = Transient.Group(group1KeyValues, randomCompression(), randomCompression(), 0.1, None).assertGet

      val group2KeyValues = randomizedIntKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1))
      val group2 = Transient.Group(group2KeyValues, randomCompression(), randomCompression(), 0.1, Some(group1)).assertGet

      val (bytes, deadline) = SegmentWriter.write(Seq(group1, group2), 0.1).assertGet
      bytes.isFull shouldBe true

      val allBytes = readAll(bytes).assertGet
      allBytes.isInstanceOf[Slice[KeyValue.ReadOnly.Group]] shouldBe true

      val allKeyValuesForGroups = allBytes.asInstanceOf[Slice[KeyValue.ReadOnly.Group]].flatMap(_.segmentCache.getAll().assertGet)
      allKeyValuesForGroups shouldBe (group1KeyValues ++ group2KeyValues).toMemory
    }

    "write child groups to a root group" in {
      val group1KeyValues = randomizedIntKeyValues(keyValueCount)
      val group1 = Transient.Group(group1KeyValues, randomCompression(), randomCompressionLZ4OrSnappy(Double.MinValue), 0.1, None).assertGet

      val group2KeyValues = randomizedIntKeyValues(keyValueCount, startId = Some(group1.maxKey.maxKey.readInt() + 1))
      val group2 = Transient.Group(group2KeyValues, randomCompressionLZ4OrSnappy(Double.MinValue), randomCompression(), 0.1, Some(group1)).assertGet

      val group3KeyValues = randomizedIntKeyValues(keyValueCount, startId = Some(group2.maxKey.maxKey.readInt() + 1))
      val group3 = Transient.Group(group3KeyValues, randomCompression(), randomCompressionLZ4OrSnappy(Double.MinValue), 0.1, Some(group2)).assertGet

      //root group
      val group4KeyValues = Seq(group1, group2, group3).updateStats
      val group4 = Transient.Group(group4KeyValues, randomCompression(), randomCompression(), 0.1, None).assertGet

      val bytes = SegmentWriter.write(Seq(group4), 0.1).assertGet._1
      bytes.isFull shouldBe true

      val rootGroup = readAll(bytes).assertGet
      rootGroup should have size 1
      rootGroup.isInstanceOf[Slice[KeyValue.ReadOnly.Group]] shouldBe true

      val childGroups = rootGroup.head.asInstanceOf[KeyValue.ReadOnly.Group].segmentCache.getAll().assertGet
      childGroups.isInstanceOf[Slice[KeyValue.ReadOnly.Group]] shouldBe true

      val allKeyValuesForGroups = childGroups.asInstanceOf[Slice[KeyValue.ReadOnly.Group]].flatMap(_.segmentCache.getAll().assertGet)
      allKeyValuesForGroups shouldBe (group1KeyValues ++ group2KeyValues ++ group3KeyValues).toMemory
    }

    "converting large KeyValues to bytes" in {
      def randomChars = Random.alphanumeric.take(10000).mkString

      val keyValues =
        Slice(
          Transient.Put(Slice(s"a$randomChars".getBytes()), value = randomChars),
          Transient.Remove(Slice(s"b$randomChars".getBytes())),
          Transient.Put(Slice(s"c$randomChars".getBytes()), value = randomChars, 1000.days),
          Transient.Range[FromValue, RangeValue](fromKey = Slice(s"d$randomChars".getBytes()), toKey = Slice(s"e$randomChars".getBytes()), randomFromValueOption(), Value.Update(randomChars)),
          Transient.Range[FromValue, RangeValue](fromKey = Slice(s"f$randomChars".getBytes()), toKey = Slice(s"g$randomChars".getBytes()), Some(Value.Put(randomChars)), Value.Update(randomChars)),
          Transient.Range[FromValue, RangeValue](fromKey = Slice(s"h$randomChars".getBytes()), toKey = Slice(s"i$randomChars".getBytes()), randomFromValueOption(), Value.Update(randomChars)),
          Transient.Group(Slice(Memory.Put(s"j$randomChars"), Memory.Put(s"k$randomChars")).toTransient, randomCompression(), randomCompression(), 0.1, None).assertGet
        ).updateStats

      val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet
      bytes.isFull shouldBe true

      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "write and read Int min max key values" in {
      val keyValues = Slice(Transient.Put(Int.MaxValue, Int.MinValue), Transient.Put(Int.MinValue, Int.MaxValue)).updateStats

      val (bytes, deadline) = SegmentWriter.write(keyValues, 0.1).assertGet
      deadline shouldBe empty

      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "write and read random key-values" in {
      runThis(10.times) {
        val keyValues = randomizedIntKeyValues(keyValueCount, addRandomGroups = false)
        val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

        //in memory
        assertReads(keyValues, Reader(bytes))
        //on disk
        assertReads(keyValues, createFileChannelReader(bytes))
      }
    }

    "write and read 100 Keys with None value to a Slice[Byte]" in {
      val keyValues = randomIntKeys(keyValueCount)

      val (bytes, deadline) = SegmentWriter.write(keyValues, 0.1).assertGet

      deadline shouldBe empty

      //in memory
      assertReads(keyValues, Reader(bytes))
      //on disk
      assertReads(keyValues, createFileChannelReader(bytes))
    }

    "report Segment corruption is CRC check does not match when reading the footer" in {
      val keyValues = Slice(Transient.Put(1)).updateStats

      val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

      SegmentReader.readFooter(Reader(bytes.drop(1))).failed.assertGet shouldBe a[SegmentCorruptionException]
      SegmentReader.readFooter(Reader(bytes.dropRight(1))).failed.assertGet shouldBe a[SegmentCorruptionException]
      SegmentReader.readFooter(Reader(bytes.slice(10, 20))).failed.assertGet shouldBe a[SegmentCorruptionException]
    }
  }

  "SegmentReader.readFooter" should {
    "set hasRange to false when Segment contains no Range key-value" in {
      val keyValues = Slice(Transient.Put(1, 1), Transient.Remove(2)).updateStats

      val (bytes, deadline) = SegmentWriter.write(keyValues, 0.1).assertGet
      deadline shouldBe empty

      val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
      footer.keyValueCount shouldBe keyValues.size
      //      footer.startIndexOffset shouldBe keyValues.head.stats.toValueOffset + 1
      footer.hasRange shouldBe false
      val bloomFilter = footer.bloomFilter.assertGet
      assertBloom(keyValues, bloomFilter)
      footer.crc should be > 0L
    }

    "set hasRange to true when Segment does not contain Remove range key-value" in {
      def doAssert(keyValues: Slice[KeyValue.WriteOnly]) = {
        keyValues.last.stats.hasRemoveRange shouldBe false

        val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

        val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
        footer.keyValueCount shouldBe keyValues.size
        footer.keyValueCount shouldBe keyValues.size
        //        footer.startIndexOffset shouldBe keyValues.last.stats.toValueOffset + 1
        footer.hasRange shouldBe true
        val bloomFilter = footer.bloomFilter.assertGet
        assertBloom(keyValues, bloomFilter)
        bloomFilter.mightContain("this does not exist") shouldBe false

        footer.crc should be > 0L
      }

      runThis(100.times) {
        doAssert(
          Slice(
            randomFixedKeyValue(1).toTransient,
            randomFixedKeyValue(2).toTransient,
            Transient.Group(Slice(randomFixedKeyValue(3)).toTransient, randomCompression(), randomCompression(), 0.1, None).assertGet,
            Transient.Range[FromValue, RangeValue](4, 8, randomFromValueOption(), Value.Update(10)),
            //also add a Range key-value to the Group that does NOT contain a remove range.
            Transient.Group(Slice(randomFixedKeyValue(10), randomRangeKeyValue(12, 15, rangeValue = Value.Update(10))).toTransient, randomCompression(), randomCompression(), 0.1, None).assertGet,
            Transient.Range[FromValue, RangeValue](20, 21, randomFromValueOption(), Value.Update(10))
          ).updateStats
        )
      }
    }

    "set hasRange to true and not create bloomFilter when Segment contains Remove range key-value" in {
      def doAssert(keyValues: Slice[KeyValue.WriteOnly]) = {
        keyValues.last.stats.hasRemoveRange shouldBe true

        val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

        val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
        footer.keyValueCount shouldBe keyValues.size
        footer.keyValueCount shouldBe keyValues.size
        footer.hasRange shouldBe true
        //bloom filters do
        footer.bloomFilter shouldBe empty
        footer.crc should be > 0L
      }

      runThis(100.times) {
        doAssert(
          Slice(
            randomFixedKeyValue(1).toTransient,
            randomFixedKeyValue(2).toTransient,
            Transient.Group(Slice(randomFixedKeyValue(3)).toTransient, randomCompression(), randomCompression(), 0.1, None).assertGet,
            Transient.Range[FromValue, RangeValue](4, 8, randomFromValueOption(), Value.Update(10)),
            //also add a Group or a Range key-value that contains a remove range.
            eitherOne(
              left = Transient.Group(Slice(randomFixedKeyValue(10), randomRangeKeyValue(12, 15, rangeValue = Value.Remove(randomDeadlineOption))).toTransient, randomCompression(), randomCompression(), 0.1, None).assertGet,
              right = Transient.Range[FromValue, RangeValue](20, 21, randomFromValueOption(), Value.Remove(randomDeadlineOption))
            )
          ).updateStats
        )
      }
    }

    "create bloomFilter when Segment not does contains Remove range key-value but contains a Group" in {
      def doAssert(keyValues: Slice[KeyValue.WriteOnly]) = {
        keyValues.last.stats.hasRemoveRange shouldBe false

        val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

        val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
        footer.keyValueCount shouldBe keyValues.size
        footer.keyValueCount shouldBe keyValues.size
        footer.hasRange shouldBe true
        //bloom filters do
        val bloomFilter = footer.bloomFilter.assertGet
        assertBloom(keyValues, bloomFilter)
        bloomFilter.mightContain("this does not exist") shouldBe false
        footer.crc should be > 0L
      }

      runThis(100.times) {
        doAssert(
          Slice(
            randomFixedKeyValue(1).toTransient,
            randomFixedKeyValue(2).toTransient,
            Transient.Group(Slice(randomFixedKeyValue(10), randomRangeKeyValue(12, 15, rangeValue = Value.Update(1))).toTransient, randomCompression(), randomCompression(), 0.1, None).assertGet
          ).updateStats
        )
      }
    }

    "set hasRange to false when there are no ranges" in {
      def doAssert(keyValues: Slice[KeyValue.WriteOnly]) = {
        keyValues.last.stats.hasRemoveRange shouldBe false

        val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

        val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
        footer.keyValueCount shouldBe keyValues.size
        footer.keyValueCount shouldBe keyValues.size
        footer.hasRange shouldBe false
        //bloom filters do
        val bloomFilter = footer.bloomFilter.assertGet
        assertBloom(keyValues, bloomFilter)
        bloomFilter.mightContain("this does not exist") shouldBe false
        footer.crc should be > 0L

      }

      runThis(10.times) {
        doAssert(
          Slice(
            randomFixedKeyValue(1).toTransient,
            randomFixedKeyValue(2).toTransient,
            Transient.Group(Slice(randomFixedKeyValue(10)).toTransient, randomCompression(), randomCompression(), 0.1, None).assertGet
          ).updateStats
        )
      }
    }

    "set hasRange to true when only the group contains range" in {
      val keyCompression = randomCompression()
      val valueCompression = randomCompression()

      def doAssert(keyValues: Slice[KeyValue.WriteOnly]) = {
        keyValues.last.stats.hasRemoveRange shouldBe false

        val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

        val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
        footer.keyValueCount shouldBe keyValues.size
        footer.keyValueCount shouldBe keyValues.size
        footer.hasRange shouldBe true
        //bloom filters do
        val bloomFilter = footer.bloomFilter.assertGet
        assertBloom(keyValues, bloomFilter)
        bloomFilter.mightContain("this does not exist") shouldBe false
        footer.crc should be > 0L

        keyValues foreach {
          case group: Transient.Group =>
            assertGroup(group, keyCompression, Some(valueCompression))
          case _ =>
        }

      }

      runThis(100.times) {
        doAssert(
          Slice(
            randomFixedKeyValue(1).toTransient,
            randomFixedKeyValue(2).toTransient,
            Transient.Group(Slice(randomPutKeyValue(10, Some("val")), randomRangeKeyValue(12, 15, rangeValue = Value.Update(1))).toTransient, keyCompression, valueCompression, 0.1, None).assertGet
          ).updateStats
        )
      }
    }

    "set hasRemoveRange to true, hasGroup to true & not create bloomFilter when only the group contains remove range" in {
      val keyCompression = randomCompression()
      val valueCompression = randomCompression()

      def doAssert(keyValues: Slice[KeyValue.WriteOnly]) = {
        keyValues.last.stats.hasRemoveRange shouldBe true

        val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

        val footer: SegmentFooter = SegmentReader.readFooter(Reader(bytes)).get
        footer.keyValueCount shouldBe keyValues.size
        footer.keyValueCount shouldBe keyValues.size
        footer.hasRange shouldBe true
        //bloom filters do
        footer.bloomFilter shouldBe empty
        footer.crc should be > 0L

        keyValues foreach {
          case group: Transient.Group =>
            assertGroup(group, keyCompression, Some(valueCompression))
          case _ =>
        }
      }

      runThis(100.times) {
        doAssert(
          Slice(
            randomFixedKeyValue(1).toTransient,
            randomFixedKeyValue(2).toTransient,
            Transient.Group(Slice(randomPutKeyValue(10, Some("val")), randomRangeKeyValue(12, 15, rangeValue = Value.Remove(None))).toTransient, keyCompression, valueCompression, 0.1, None).assertGet
          ).updateStats
        )
      }
    }
  }

  "SegmentReader.find" should {
    "get key-values using KeyMatcher.Get" in {
      val keyValues =
        Slice(
          Transient.Put(1, "one"),
          Transient.Put(2, "two"),
          Transient.Remove(Int.MaxValue - 1000),
          Transient.Range[FromValue, RangeValue](Int.MaxValue - 900, Int.MaxValue - 800, None, Value.Update(10)),
          Transient.Group(Slice(randomPutKeyValue(Int.MaxValue - 600, Some("val")), randomRangeKeyValue(Int.MaxValue - 500, Int.MaxValue - 400, rangeValue = Value.Remove(None))).toTransient, randomCompression(), randomCompression(), 0.1, None).assertGet
        ).updateStats

      val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet
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
      foundKeyValue4.fromKey shouldBe (Int.MaxValue - 900: Slice[Byte])
      foundKeyValue4.toKey shouldBe (Int.MaxValue - 800: Slice[Byte])
      //4 has common bytes with 3rd key-value. It will be sliced.
      foundKeyValue4.key.underlyingArraySize shouldBe 8
      foundKeyValue4.fromKey.underlyingArraySize shouldBe 8 //fromKey is unsliced
      foundKeyValue4.toKey.underlyingArraySize shouldBe 4 //toKey shares common bytes with fromKey so it will be unsliced.

      foundKeyValue4.key.toArray shouldBe keyValues(3).key.toArray
      //value is a slice of bytes array and not
      foundKeyValue4.indexOffset shouldBe foundKeyValue3.nextIndexOffset

      //FIFTH KEY
      val foundKeyValue5 = SegmentReader.find(KeyMatcher.Get(keyValues(4).key), None, Reader(bytes)).assertGet.asInstanceOf[Persistent.Group]
      foundKeyValue5.getOrFetchValue.assertGetOpt shouldBe keyValues(4).getOrFetchValue.assertGetOpt
      foundKeyValue5.minKey shouldBe (Int.MaxValue - 600: Slice[Byte])
      foundKeyValue5.maxKey shouldBe keyValues.maxKey()
      //5 has common bytes with 4rd key-value. It will be sliced.
      foundKeyValue5.key.underlyingArraySize shouldBe GroupCompressor.buildCompressedKey(Slice(foundKeyValue5).toTransient)._3.size
      foundKeyValue5.minKey.underlyingArraySize shouldBe GroupCompressor.buildCompressedKey(Slice(foundKeyValue5).toTransient)._3.size //fromKey is unsliced
      foundKeyValue5.maxKey.maxKey.underlyingArraySize shouldBe 4 //toKey shares common bytes with fromKey so it will be unsliced.

      foundKeyValue5.key.toArray shouldBe keyValues(4).key.toArray
      //value is a slice of bytes array and not
      foundKeyValue5.indexOffset shouldBe foundKeyValue4.nextIndexOffset
    }

    "get key-values using KeyMatcher.Lower" in {
      val keyValues =
        Slice(
          Transient.Put(1, "one"),
          Transient.Put(2, "two", 10.days),
          Transient.Remove(Int.MaxValue - 10),
          Transient.Range[FromValue, RangeValue](Int.MaxValue - 9, Int.MaxValue, None, Value.Update(10))
        ).updateStats

      val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

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
          Transient.Put(2, "two", 2.days),
          Transient.Remove(Int.MaxValue - 10),
          Transient.Range[FromValue, RangeValue](Int.MaxValue - 9, Int.MaxValue, None, Value.Update(10))
        ).updateStats

      val (bytes, _) = SegmentWriter.write(keyValues, 0.1).assertGet

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

    "return nearest deadline" when {
      "there are no groups" in {
        val deadline1 = 10.seconds.fromNow
        val deadline2 = 20.seconds.fromNow
        val deadline3 = 30.seconds.fromNow

        SegmentWriter.write(
          keyValues = Slice(Memory.Put(1, 1, deadline1), Memory.Put(2, 2)).toTransient,
          bloomFilterFalsePositiveRate = 0.1
        ).assertGet._2 should contain(deadline1)

        SegmentWriter.write(
          keyValues = Slice(Memory.Put(1, 1), Memory.Put(2, 2, deadline3), Memory.Put(3, 3, deadline1), Memory.Put(4, 4), Memory.Put(5, 5, deadline2)).toTransient,
          bloomFilterFalsePositiveRate = 0.1
        ).assertGet._2 should contain(deadline1)
      }

      "are groups" in {
        val group1 = randomGroup(randomizedIntKeyValues(1000, addRandomPutDeadlines = true, addRandomRemoveDeadlines = true, addRandomGroups = true))
        val group2 = randomGroup(randomizedIntKeyValues(2000, startId = Some(group1.maxKey.maxKey.readInt() + 1), addRandomPutDeadlines = true, addRandomRemoveDeadlines = true, addRandomGroups = true))
        val group3 = randomGroup(randomizedIntKeyValues(3000, startId = Some(group2.maxKey.maxKey.readInt() + 1), addRandomPutDeadlines = true, addRandomRemoveDeadlines = true, addRandomGroups = true))

        val groups = Seq(group1, group2, group3).updateStats
        val rootGroup = Slice(randomGroup(groups))

        val unzippedKeyValues = unzipGroups(rootGroup)
        println("unzippedKeyValues.size: " + unzippedKeyValues.size)
        val nearestDeadline = unzippedKeyValues.foldLeft(Option.empty[Deadline])(Segment.getNearestDeadline)
        println("nearestDeadline: " + nearestDeadline)

        SegmentWriter.write(
          keyValues = rootGroup,
          bloomFilterFalsePositiveRate = 0.1
        ).assertGet._2 shouldBe nearestDeadline
      }
    }
  }

  "writing key-values with duplicate values" should {
    "use the same valueOffset and not create duplicate values" in {
      val value: Slice[Byte] = 100

      val keyValues =
        Seq(
          Memory.Put(1, value),
          Memory.Update(2, value),
          Memory.Put(3, value),
          Memory.Put(4, value),
          Memory.Update(5, value),
          Memory.Put(6, value),
          Memory.Update(7, value),
          Memory.Update(8, value),
          Memory.Put(9, value),
          Memory.Update(10, value)
        ).toTransient

      val (bytes, deadline) = SegmentWriter.write(keyValues, 0.1).assertGet
      //      println(bytes)

      deadline shouldBe empty

      //only the first 4 bytes for the value 100 should be (0, 0, 0, 100) and the next byte should be the start of index
      //as values are not duplicated
      bytes.take(value.size) shouldBe value
      //drop the first 4 bytes that are value bytes and the next value bytes (value of the next key-value) should not be (0, 0, 0, 100)
      bytes.drop(value.size).take(value.size) should not be value

      val readKeyValues = readAll(bytes).assertGet
      readKeyValues should have size keyValues.size

      //assert that all valueOffsets of all key-values are the same
      readKeyValues.foldLeft(Option.empty[Int]) {
        case (previousOffsetOption, fixed: Persistent.Fixed) =>
          previousOffsetOption match {
            case Some(previousOffset) =>
              fixed.valueOffset shouldBe previousOffset
              fixed.valueLength shouldBe value.size
              previousOffsetOption

            case None =>
              Some(fixed.valueOffset)
          }

        case keyValue =>
          fail(s"Got: ${keyValue.getClass.getSimpleName}. Didn't expect any other key-value other than Put")

      }
    }
  }

}