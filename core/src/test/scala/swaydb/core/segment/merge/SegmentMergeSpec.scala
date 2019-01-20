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

package swaydb.core.segment.merge

import scala.collection.mutable.ListBuffer
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TryAssert._
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestData, TestTimeGenerator}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

class SegmentMergeSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val timeGenerator: TestTimeGenerator = TestTimeGenerator.Empty
  implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None

  val keyValueCount = 100

  import keyOrder._

  "completeMerge" should {

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for persistent key-values" in {
      implicit val timeGenerator: TestTimeGenerator = TestTimeGenerator.Empty
      implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None

      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true))
      segment1.+=(Transient.put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true)) //total segmentSize is 70.bytes

      val smallerLastSegment = ListBuffer.empty[KeyValue.WriteOnly]
      smallerLastSegment.+=(Transient.put(key = 1, value = 1, previous = None, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true)) //total segmentSize is 60.bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1, smallerLastSegment)

      //minSegmentSize is 70.bytes but lastSegment size is 60.bytes. Expected result should move lastSegment's KeyValues to previous segment
      val newSegments = SegmentMerger.completeMerge(segments, 70.bytes, forMemory = false, bloomFilterFalsePositiveRate = TestData.falsePositiveRate).assertGet
      newSegments.size shouldBe 1
      newSegments.head(0).key equiv segment1.head.key
      newSegments.head(1).key equiv segment1.last.key
      newSegments.head(2).key equiv smallerLastSegment.head.key
    }

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for memory key-values" in {

      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true))
      segment1.+=(Transient.put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true)) //total segmentSize is 79.bytes

      val smallerLastSegment = ListBuffer.empty[KeyValue.WriteOnly]
      smallerLastSegment.+=(Transient.put(key = 1, value = 1, previous = None, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true)) //total segmentSize is 69.bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1, smallerLastSegment)

      //minSegmentSize is 20.bytes but lastSegment size is 24.bytes. Expected result should move lastSegment's KeyValues to previous segment
      val newSegments = SegmentMerger.completeMerge(segments, 50.bytes, forMemory = true, bloomFilterFalsePositiveRate = TestData.falsePositiveRate).assertGet
      newSegments.size shouldBe 1
      newSegments.head(0).key equiv segment1.head.key
      newSegments.head(1).key equiv segment1.last.key
      newSegments.head(2).key equiv smallerLastSegment.head.key
    }

    "make no change if there is only one segment" in {

      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true))
      segment1.+=(Transient.put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true)) //total segmentSize is 37

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1)

      segments.size shouldBe 1

      SegmentMerger.completeMerge(segments, 37.bytes, forMemory = false, bloomFilterFalsePositiveRate = TestData.falsePositiveRate).assertGet.size shouldBe 1
      SegmentMerger.completeMerge(segments, 16.bytes, forMemory = true, bloomFilterFalsePositiveRate = TestData.falsePositiveRate).assertGet.size shouldBe 1
    }

    "split KeyValues into equal chunks" in {

      val oldKeyValues: Slice[Memory] = Slice(Memory.put(1, 1), Memory.put(2, 2), Memory.put(3, 3), Memory.put(4, 4))
      val newKeyValues: Slice[Memory] = Slice(Memory.put(1, 22), Memory.put(2, 22), Memory.put(3, 22), Memory.put(4, 22))

      def assert(segments: Array[Iterable[KeyValue.WriteOnly]]) = {
        segments.length shouldBe 4

        segments(0).size shouldBe 1
        segments(0).head shouldBe newKeyValues(0)

        segments(1).size shouldBe 1
        segments(1).head.key equiv newKeyValues(1).key

        segments(2).size shouldBe 1
        segments(2).head.key equiv newKeyValues(2).key

        segments(3).size shouldBe 1
        segments(3).head.key equiv newKeyValues(3).key
      }

      assert(SegmentMerger.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.byte, isLastLevel = false, forInMemory = false, bloomFilterFalsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true).assertGet.toArray)
      assert(SegmentMerger.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.byte, isLastLevel = false, forInMemory = true, bloomFilterFalsePositiveRate = TestData.falsePositiveRate, compressDuplicateValues = true).assertGet.toArray)
    }
  }

  "split" should {
    "split key-values" in {

      val keyValues: Slice[Memory] = Slice(Memory.put(1, 1), Memory.remove(2), Memory.put(3, 3), Memory.put(4, 4), Memory.Range(5, 10, Some(Value.remove(None)), Value.update(5)))

      val split1 =
        SegmentMerger.split(
          keyValues = keyValues,
          minSegmentSize = 1.byte,
          isLastLevel = false,
          forInMemory = false,
          compressDuplicateValues = true,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ).assertGet

      split1 should have size 5
      split1 should contain only
        (ListBuffer(Transient.put(1, 1)),
          ListBuffer(Transient.remove(2)),
          ListBuffer(Transient.put(3, 3)),
          ListBuffer(Transient.put(4, 4)), //51.byte Segment size
          ListBuffer(Transient.Range.create[FromValue, RangeValue](5, 10, Some(Value.remove(None)), Value.update(5))) //56.bytes (segment size)
        )

      val split2 =
        SegmentMerger.split(
          keyValues = keyValues,
          minSegmentSize = 255.bytes,
          isLastLevel = false,
          forInMemory = false,
          compressDuplicateValues = true,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        )

      val expected =
        Seq(
          Transient.put(1, 1),
          Transient.remove(2),
          Transient.put(3, 3),
          Transient.put(4, 4), //51.byte Segment size
          Transient.Range.create[FromValue, RangeValue](5, 10, Some(Value.remove(None)), Value.update(5)) //56.bytes (segment size)
        ).updateStats

      split2.assertGet.flatten shouldBe expected
    }
  }

  "Merging fixed into Group" should {
    "return the same result as merging a list of Fixed key-values into Fixed" in {
      val fixedKeyValues = randomKeyValues(count = keyValueCount, addRandomRemoves = true, startId = Some(1))
      val oldKeyValues = randomKeyValues(count = keyValueCount, startId = Some(fixedKeyValues.head.key.readInt()), addRandomRemoves = true, addRandomRanges = true)

      val mergeResultWithoutGroup = SegmentMerger.merge(fixedKeyValues, oldKeyValues, 100.mb, false, false, TestData.falsePositiveRate, compressDuplicateValues = true).assertGet
      mergeResultWithoutGroup should have size 1

      val group = Transient.Group(oldKeyValues, randomCompression(), randomCompression(), TestData.falsePositiveRate, None).assertGet.toMemory
      val mergeResultWithGroup = SegmentMerger.merge(fixedKeyValues, Slice(group), 100.mb, false, false, TestData.falsePositiveRate, compressDuplicateValues = true).assertGet
      mergeResultWithGroup should have size 1

      mergeResultWithoutGroup.head shouldBe mergeResultWithGroup.head
    }
  }

  "SegmentMerger performance" in {
    val keyValues = randomKeyValues(100000)
    Benchmark("SegmentMerger performance") {
      SegmentMerger.merge(keyValues, keyValues, 100.mb, false, false, TestData.falsePositiveRate, compressDuplicateValues = true).assertGet
    }
  }
}
