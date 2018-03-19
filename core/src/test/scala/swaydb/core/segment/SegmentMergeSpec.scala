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
import swaydb.core.data.{KeyValue, Transient}
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer

class SegmentMergeSpec extends TestBase {

  implicit val ordering = KeyOrder.default

  import ordering._

  "SegmentMerge.addKeyValue" should {
    "add KeyValue to existing split and close the split if the total segmentSize is minSegmentSize for persistent key-values" in {

      val initialSegment = ListBuffer[KeyValue.WriteOnly]()
      initialSegment += Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1)
      initialSegment += Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1) //total segmentSize is 62 bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](initialSegment)
      //this KeyValue's segment size without footer is 14 bytes
      val keyValue = Transient.Put(3, 3)

      //minSegmentSize is 72.bytes. Adding the above keyValues should create a segment of total size
      // 62 + 14 - 3 (common bytes between 2 and 3) which is over the limit = 73.bytes.
      // this should result is closing the existing segment and starting a new segment
      SegmentMerge.addKeyValue(keyValue, segments, 72.bytes, forInMemory = false, bloomFilterFalsePositiveRate = 0.1, isLastLevel = false)

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 2

      val closedSegment = segments.head
      closedSegment(0).key equiv initialSegment.head.key
      closedSegment(1).key equiv initialSegment.last.key
      closedSegment(2).key equiv keyValue.key
      closedSegment.last.stats.segmentSize shouldBe 73.bytes

      //since the previous segment is closed a new segment shouldBe created for next KeyValues to be added.
      segments.last shouldBe empty
    }

    "add KeyValue to existing split and close the split if the total segmentSize is minSegmentSize for memory key-values" in {

      val initialSegment = ListBuffer.empty[KeyValue.WriteOnly]
      initialSegment.+=(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1))
      initialSegment.+=(Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 16 bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](initialSegment)
      //this KeyValue's segment size without footer is 8 bytes
      val keyValue = Transient.Put(3, 3)

      //minSegmentSize is 23.bytes. Adding the above keyValues should create a segment of total size
      // 16 + 8 = 24.bytes which is over the limit = 23.bytes.
      // this should result is closing the existing segment and starting a new segment
      SegmentMerge.addKeyValue(keyValue, segments, 23.bytes, forInMemory = true, bloomFilterFalsePositiveRate = 0.1, isLastLevel = false)

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 2

      val closedSegment = segments.head
      closedSegment(0).key equiv initialSegment.head.key
      closedSegment(1).key equiv initialSegment.last.key
      closedSegment(2).key equiv keyValue.key
      closedSegment.last.stats.memorySegmentSize shouldBe 24.bytes

      //since the previous segment is closed a new segment shouldBe created for next KeyValues to be added.
      segments.last shouldBe empty
    }

    "add KeyValue to current split if the total segmentSize with the new KeyValue < minSegmentSize for persistent key-values" in {

      val initialSegment = ListBuffer.empty[KeyValue.WriteOnly]
      initialSegment.+=(Transient.Put(key = 1, value = 1, previous = initialSegment.lastOption, falsePositiveRate = 0.1))
      initialSegment.+=(Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 62.bytes

      val segments = ListBuffer.empty[ListBuffer[KeyValue.WriteOnly]] += initialSegment
      val keyValue = Transient.Put(1, 1) //this KeyValue's segment size without footer is 14 bytes

      //minSegmentSize is 72.bytes. Adding the above keyValue should create a segment of
      // 62 + 14 - 3 (common bytes between 2 and 3) = 73.bytes.
      //which is under the limit of 74.bytes. This should result in adding the next keyValue to existing segment without starting a new segment
      SegmentMerge.addKeyValue(keyValue, segments, 74.bytes, forInMemory = false, bloomFilterFalsePositiveRate = 0.1, isLastLevel = false)

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 1

      val closedSegment = segments.head
      closedSegment(0).key equiv initialSegment.head.key
      closedSegment(1).key equiv initialSegment.last.key
      closedSegment(2).key equiv keyValue.key
      closedSegment.last.stats.segmentSize shouldBe 73.bytes
    }

    "add KeyValue to current split if the total segmentSize with the new KeyValue < minSegmentSize for memory key-values" in {

      val initialSegment = ListBuffer.empty[KeyValue.WriteOnly]
      initialSegment.+=(Transient.Put(key = 1, value = 1, previous = initialSegment.lastOption, falsePositiveRate = 0.1))
      initialSegment.+=(Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 16 bytes

      val segments = ListBuffer.empty[ListBuffer[KeyValue.WriteOnly]] += initialSegment
      val keyValue = Transient.Put(1, 1) //this KeyValue's segment size without footer is 8 bytes

      //minSegmentSize is 25.bytes. Adding the above keyValue should create a segment of
      // 16 + 8 = 24.bytes which is under the limit 25.bytes.
      //This should result in adding the next keyValue to existing segment without starting a new segment
      SegmentMerge.addKeyValue(keyValue, segments, 25.bytes, forInMemory = true, bloomFilterFalsePositiveRate = 0.1, isLastLevel = false)

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

      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1))
      segment1.+=(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 60.bytes

      val smallerLastSegment = ListBuffer.empty[KeyValue.WriteOnly]
      smallerLastSegment.+=(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1)) //total segmentSize is 49.bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1, smallerLastSegment)

      //minSegmentSize is 60.bytes but lastSegment size is 49.bytes. Expected result should move lastSegment's KeyValues to previous segment
      val newSegments = SegmentMerge.mergeSmallerSegmentWithPrevious(segments, 60.bytes, forMemory = false, bloomFilterFalsePositiveRate = 0.1)
      newSegments.size shouldBe 1
      newSegments.head(0).key equiv segment1.head.key
      newSegments.head(1).key equiv segment1.last.key
      newSegments.head(2).key equiv smallerLastSegment.head.key
    }

    "transfer the last segment's KeyValues to previous segment, if the last segment's segmentSize is < minSegmentSize for memory key-values" in {
      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1))
      segment1.+=(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 16.bytes

      val smallerLastSegment = ListBuffer.empty[KeyValue.WriteOnly]
      smallerLastSegment.+=(Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1)) //total segmentSize is 8.bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1, smallerLastSegment)

      //minSegmentSize is 20.bytes but lastSegment size is 24.bytes. Expected result should move lastSegment's KeyValues to previous segment
      val newSegments = SegmentMerge.mergeSmallerSegmentWithPrevious(segments, 20.bytes, forMemory = true, bloomFilterFalsePositiveRate = 0.1)
      newSegments.size shouldBe 1
      newSegments.head(0).key equiv segment1.head.key
      newSegments.head(1).key equiv segment1.last.key
      newSegments.head(2).key equiv smallerLastSegment.head.key
    }

    "make no change if there is only one segment" in {

      val segment1 = ListBuffer.empty[KeyValue.WriteOnly]
      segment1.+=(Transient.Put(key = 1, value = 1, previous = segment1.lastOption, falsePositiveRate = 0.1))
      segment1.+=(Transient.Put(key = 2, value = 2, previous = segment1.lastOption, falsePositiveRate = 0.1)) //total segmentSize is 37

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](segment1)

      segments.size shouldBe 1

      SegmentMerge.mergeSmallerSegmentWithPrevious(segments, 37.bytes, forMemory = false, bloomFilterFalsePositiveRate = 0.1).size shouldBe 1
      SegmentMerge.mergeSmallerSegmentWithPrevious(segments, 16.bytes, forMemory = true, bloomFilterFalsePositiveRate = 0.1).size shouldBe 1
    }
  }
}
