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
import swaydb.core.data.{KeyValueWriteOnly, Transient}
import swaydb.core.data.Transient.Remove
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer

class SegmentMergeSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  import ordering._

  "SegmentMerge.add" should {
    val defaultSegmentsKeyValuesCount = 10

    "add KeyValue to existing split and close the split if the total segmentSize is minSegmentSize for persistent key-values" in {

      val initialSegment = Slice.create[KeyValueWriteOnly](10)
      initialSegment.add(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1))
      initialSegment.add(Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 60 bytes

      val segments = ListBuffer[Slice[KeyValueWriteOnly]](initialSegment)
      //this KeyValue's segment size without footer is 14 bytes
      val keyValue = Transient.Put(3, 3)

      //minSegmentSize is 70.bytes. Adding the above keyValues should create a segment of total size
      // 60 + 14 - 3 (common bytes between 2 and 3) which is over the limit = 71.bytes.
      // this should result is closing the existing segment and starting a new segment
      SegmentMerge.add(keyValue, 1, segments, 70.bytes, forInMemory = false, bloomFilterFalsePositiveRate = 0.1)

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 2

      val closedSegment = segments.head
      closedSegment(0).key equiv initialSegment.head.key
      closedSegment(1).key equiv initialSegment.last.key
      closedSegment(2).key equiv keyValue.key
      closedSegment.last.stats.segmentSize shouldBe 71.bytes

      //since the previous segment is closed a new segment shouldBe created for next KeyValues to be added.
      val newSegment = segments.last
      newSegment.written shouldBe 0
    }

    "add KeyValue to existing split and close the split if the total segmentSize is minSegmentSize for memory key-values" in {

      val initialSegment = Slice.create[KeyValueWriteOnly](10)
      initialSegment.add(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1))
      initialSegment.add(Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 16 bytes

      val segments = ListBuffer[Slice[KeyValueWriteOnly]](initialSegment)
      //this KeyValue's segment size without footer is 8 bytes
      val keyValue = Transient.Put(3, 3)

      //minSegmentSize is 23.bytes. Adding the above keyValues should create a segment of total size
      // 16 + 8 = 24.bytes which is over the limit = 23.bytes.
      // this should result is closing the existing segment and starting a new segment
      SegmentMerge.add(keyValue, 1, segments, 23.bytes, forInMemory = true, bloomFilterFalsePositiveRate = 0.1)

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 2

      val closedSegment = segments.head
      closedSegment(0).key equiv initialSegment.head.key
      closedSegment(1).key equiv initialSegment.last.key
      closedSegment(2).key equiv keyValue.key
      closedSegment.last.stats.memorySegmentSize shouldBe 24.bytes

      //since the previous segment is closed a new segment shouldBe created for next KeyValues to be added.
      val newSegment = segments.last
      newSegment.written shouldBe 0
    }

    "add KeyValue to current split if the total segmentSize with the new KeyValue < minSegmentSize for persistent key-values" in {

      val initialSegment = Slice.create[KeyValueWriteOnly](10)
      initialSegment.add(Transient.Put(key = 1, value = 1, previous = initialSegment.lastOption, falsePositiveRate = 0.1))
      initialSegment.add(Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 60.bytes

      val segments = ListBuffer.empty[Slice[KeyValueWriteOnly]] += initialSegment
      val keyValue = Transient.Put(1, 1) //this KeyValue's segment size without footer is 14 bytes

      //minSegmentSize is 72.bytes. Adding the above keyValue should create a segment of
      // 60 + 14 - 3 (common bytes between 2 and 3) = 71.bytes.
      //which is under the limit of 72.bytes. This should result in adding the next keyValue to existing segment without starting a new segment
      SegmentMerge.add(keyValue, defaultSegmentsKeyValuesCount, segments, 72.bytes, forInMemory = false, bloomFilterFalsePositiveRate = 0.1)

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 1

      val closedSegment = segments.head
      closedSegment(0).key equiv initialSegment.head.key
      closedSegment(1).key equiv initialSegment.last.key
      closedSegment(2).key equiv keyValue.key
      closedSegment.last.stats.segmentSize shouldBe 71.bytes
    }

    "add KeyValue to current split if the total segmentSize with the new KeyValue < minSegmentSize for memory key-values" in {

      val initialSegment = Slice.create[KeyValueWriteOnly](10)
      initialSegment.add(Transient.Put(key = 1, value = 1, previous = initialSegment.lastOption, falsePositiveRate = 0.1))
      initialSegment.add(Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 16 bytes

      val segments = ListBuffer.empty[Slice[KeyValueWriteOnly]] += initialSegment
      val keyValue = Transient.Put(1, 1) //this KeyValue's segment size without footer is 8 bytes

      //minSegmentSize is 25.bytes. Adding the above keyValue should create a segment of
      // 16 + 8 = 24.bytes which is under the limit 25.bytes.
      //This should result in adding the next keyValue to existing segment without starting a new segment
      SegmentMerge.add(keyValue, defaultSegmentsKeyValuesCount, segments, 25.bytes, forInMemory = true, bloomFilterFalsePositiveRate = 0.1)

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 1

      val closedSegment = segments.head
      closedSegment(0).key equiv initialSegment.head.key
      closedSegment(1).key equiv initialSegment.last.key
      closedSegment(2).key equiv keyValue.key
      closedSegment.last.stats.memorySegmentSize shouldBe 24.bytes
    }

  }

  "SegmentMerge.transferLastMayBe" should {

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for persistent key-values" in {

      val segment1 = Slice.create[KeyValueWriteOnly](10)
      segment1.add(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1))
      segment1.add(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 60.bytes

      val smallerLastSegment = Slice.create[KeyValueWriteOnly](10)
      smallerLastSegment.add(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1)) //total segmentSize is 49.bytes

      val segments = ListBuffer[Slice[KeyValueWriteOnly]](segment1, smallerLastSegment)

      //minSegmentSize is 60.bytes but lastSegment size is 49.bytes. Expected result should move lastSegment's KeyValues to previous segment
      val newSegments = SegmentMerge.mergeSmallerSegmentWithPrevious(segments, 60.bytes, forMemory = false, bloomFilterFalsePositiveRate = 0.1)
      newSegments.size shouldBe 1
      newSegments.head(0).key equiv segment1.head.key
      newSegments.head(1).key equiv segment1.last.key
      newSegments.head(2).key equiv smallerLastSegment.head.key
    }

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for memory key-values" in {
      val segment1 = Slice.create[KeyValueWriteOnly](10)
      segment1.add(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1))
      segment1.add(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 16.bytes

      val smallerLastSegment = Slice.create[KeyValueWriteOnly](10)
      smallerLastSegment.add(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1)) //total segmentSize is 8.bytes

      val segments = ListBuffer[Slice[KeyValueWriteOnly]](segment1, smallerLastSegment)

      //minSegmentSize is 20.bytes but lastSegment size is 24.bytes. Expected result should move lastSegment's KeyValues to previous segment
      val newSegments = SegmentMerge.mergeSmallerSegmentWithPrevious(segments, 20.bytes, forMemory = true, bloomFilterFalsePositiveRate = 0.1)
      newSegments.size shouldBe 1
      newSegments.head(0).key equiv segment1.head.key
      newSegments.head(1).key equiv segment1.last.key
      newSegments.head(2).key equiv smallerLastSegment.head.key
    }

    "make no change if there is only one segment" in {

      val segment1 = Slice.create[KeyValueWriteOnly](10)
      segment1.add(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1))
      segment1.add(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 37

      val segments = ListBuffer[Slice[KeyValueWriteOnly]](segment1)

      segments.size shouldBe 1

      SegmentMerge.mergeSmallerSegmentWithPrevious(segments, 37.bytes, forMemory = false, bloomFilterFalsePositiveRate = 0.1).size shouldBe 1
      SegmentMerge.mergeSmallerSegmentWithPrevious(segments, 16.bytes, forMemory = true, bloomFilterFalsePositiveRate = 0.1).size shouldBe 1
    }
  }

  "SegmentMerge.merge" should {

    "should split KeyValues to equal chunks" in {

      val oldKeyValues: Slice[KeyValueWriteOnly] = Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3), Transient.Put(4, 4)).updateStats
      val newKeyValues: Slice[KeyValueWriteOnly] = Slice(Transient.Put(1, 22), Transient.Put(2, 22), Transient.Put(3, 22), Transient.Put(4, 22)).updateStats

      def assert(segments: Array[Slice[KeyValueWriteOnly]]) = {
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

      assert(SegmentMerge.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.byte, removeDeletes = false, forInMemory = false, bloomFilterFalsePositiveRate = 0.1).toArray)
      assert(SegmentMerge.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.byte, removeDeletes = false, forInMemory = true, bloomFilterFalsePositiveRate = 0.1).toArray)
    }

    "should merge should remove Deleted key-values if removeDeletes is true" in {

      val oldKeyValues: Slice[KeyValueWriteOnly] = Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3), Transient.Put(4, 4)).updateStats
      val newKeyValues: Slice[KeyValueWriteOnly] = Slice(Remove(0), Transient.Put(1, 11), Remove(2), Transient.Put(3, 33), Remove(4), Remove(5)).updateStats

      def assert(segments: Iterable[Slice[KeyValueWriteOnly]]) = {
        segments should have size 1

        val mergedKeyValues = segments.head

        mergedKeyValues.toArray should have size 2

        mergedKeyValues.head.key shouldBe (1: Slice[Byte])
        mergedKeyValues.head.getOrFetchValue.assertGet shouldBe (11: Slice[Byte])

        mergedKeyValues(1).key shouldBe (3: Slice[Byte])
        mergedKeyValues(1).getOrFetchValue.assertGet shouldBe (33: Slice[Byte])
      }

      assert(SegmentMerge.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.mb, removeDeletes = true, forInMemory = false, bloomFilterFalsePositiveRate = 0.1))
      assert(SegmentMerge.merge(newKeyValues, oldKeyValues, minSegmentSize = 1.mb, removeDeletes = true, forInMemory = true, bloomFilterFalsePositiveRate = 0.1))

    }
  }

}
