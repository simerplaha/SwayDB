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

package swaydb.core.segment.merge

import swaydb.core.TestBase
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data._
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class SegmentMergeSpec extends TestBase {

  override implicit val ordering = KeyOrder.default
  implicit val compression = groupingStrategy
  val keyValueCount = 100

  import ordering._


  "SegmentMerger.split" should {
    "split key-values" in {
      val keyValues: Slice[Memory] = Slice(Memory.Put(1, 1), Memory.Remove(2), Memory.Put(3, 3), Memory.Put(4, 4), Memory.Range(5, 10, Some(Value.Remove(None)), Value.Update(5)))

      val split1 =
        SegmentMerger.split(
          keyValues = keyValues,
          minSegmentSize = 1.byte,
          isLastLevel = false,
          forInMemory = false,
          compressDuplicateValues = true,
          bloomFilterFalsePositiveRate = 0.1
        ).assertGet

      split1 should have size 5
      split1 should contain only
        (ListBuffer(Transient.Put(1, 1)),
          ListBuffer(Transient.Remove(2)),
          ListBuffer(Transient.Put(3, 3)),
          ListBuffer(Transient.Put(4, 4)), //51.byte Segment size
          ListBuffer(Transient.Range[FromValue, RangeValue](5, 10, Some(Value.Remove(None)), Value.Update(5))) //56.bytes (segment size)
        )

      val split2 =
        SegmentMerger.split(
          keyValues = keyValues,
          minSegmentSize = 255.bytes,
          isLastLevel = false,
          forInMemory = false,
          compressDuplicateValues = true,
          bloomFilterFalsePositiveRate = 0.1
        )

      val expected =
        Seq(
          Transient.Put(1, 1),
          Transient.Remove(2),
          Transient.Put(3, 3),
          Transient.Put(4, 4), //51.byte Segment size
          Transient.Range[FromValue, RangeValue](5, 10, Some(Value.Remove(None)), Value.Update(5)) //56.bytes (segment size)
        ).updateStats

      split2.assertGet.flatten shouldBe expected
    }
  }

  "SegmentMerger.completeMerge" should {

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for persistent key-values" in {

      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1, compressDuplicateValues = true))
      segment1.+=(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1, compressDuplicateValues = true)) //total segmentSize is 60.bytes

      val smallerLastSegment = ListBuffer.empty[KeyValue.WriteOnly]
      smallerLastSegment.+=(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1, compressDuplicateValues = true)) //total segmentSize is 49.bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1, smallerLastSegment)

      //minSegmentSize is 60.bytes but lastSegment size is 49.bytes. Expected result should move lastSegment's KeyValues to previous segment
      val newSegments = SegmentMerger.completeMerge(segments, 60.bytes, forMemory = false, bloomFilterFalsePositiveRate = 0.1).assertGet
      newSegments.size shouldBe 1
      newSegments.head(0).key equiv segment1.head.key
      newSegments.head(1).key equiv segment1.last.key
      newSegments.head(2).key equiv smallerLastSegment.head.key
    }

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for memory key-values" in {
      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1, compressDuplicateValues = true))
      segment1.+=(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1, compressDuplicateValues = true)) //total segmentSize is 16.bytes

      val smallerLastSegment = ListBuffer.empty[KeyValue.WriteOnly]
      smallerLastSegment.+=(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1, compressDuplicateValues = true)) //total segmentSize is 8.bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1, smallerLastSegment)

      //minSegmentSize is 20.bytes but lastSegment size is 24.bytes. Expected result should move lastSegment's KeyValues to previous segment
      val newSegments = SegmentMerger.completeMerge(segments, 20.bytes, forMemory = true, bloomFilterFalsePositiveRate = 0.1).assertGet
      newSegments.size shouldBe 1
      newSegments.head(0).key equiv segment1.head.key
      newSegments.head(1).key equiv segment1.last.key
      newSegments.head(2).key equiv smallerLastSegment.head.key
    }

    "make no change if there is only one segment" in {

      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1, compressDuplicateValues = true))
      segment1.+=(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1, compressDuplicateValues = true)) //total segmentSize is 37

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1)

      segments.size shouldBe 1

      SegmentMerger.completeMerge(segments, 37.bytes, forMemory = false, bloomFilterFalsePositiveRate = 0.1).assertGet.size shouldBe 1
      SegmentMerger.completeMerge(segments, 16.bytes, forMemory = true, bloomFilterFalsePositiveRate = 0.1).assertGet.size shouldBe 1
    }

    "split KeyValues into equal chunks" in {
      val oldKeyValues: Slice[Memory] = Slice(Memory.Put(1, 1), Memory.Put(2, 2), Memory.Put(3, 3), Memory.Put(4, 4))
      val newKeyValues: Slice[Memory] = Slice(Memory.Put(1, 22), Memory.Put(2, 22), Memory.Put(3, 22), Memory.Put(4, 22))

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

      assert(SegmentMerger.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.byte, isLastLevel = false, forInMemory = false, bloomFilterFalsePositiveRate = 0.1, hasTimeLeftAtLeast = 10.seconds, compressDuplicateValues = true).assertGet.toArray)
      assert(SegmentMerger.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.byte, isLastLevel = false, forInMemory = true, bloomFilterFalsePositiveRate = 0.1, hasTimeLeftAtLeast = 10.seconds, compressDuplicateValues = true).assertGet.toArray)
    }
  }

  "Merging fixed into Group" should {
    "return the same result as merging a list of Fixed key-values into Fixed" in {
      val fixedKeyValues = randomIntKeyValues(count = keyValueCount, addRandomRemoves = true, startId = Some(1))
      val oldKeyValues = randomIntKeyValues(count = keyValueCount, startId = Some(fixedKeyValues.head.key.readInt()), addRandomRemoves = true, addRandomRanges = true)

      val mergeResultWithoutGroup = SegmentMerger.merge(fixedKeyValues, oldKeyValues, 100.mb, false, false, 0.1, 0.seconds, compressDuplicateValues = true).assertGet
      mergeResultWithoutGroup should have size 1

      val group = Transient.Group(oldKeyValues, randomCompression(), randomCompression(), 0.1, None).assertGet.toMemory
      val mergeResultWithGroup = SegmentMerger.merge(fixedKeyValues, Slice(group), 100.mb, false, false, 0.1, 0.seconds, compressDuplicateValues = true).assertGet
      mergeResultWithGroup should have size 1

      mergeResultWithoutGroup.head shouldBe mergeResultWithGroup.head

    }
  }

}
