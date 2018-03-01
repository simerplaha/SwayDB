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
import swaydb.core.util.FileUtil._
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class SegmentAssignerSpec1 extends SegmentAssignerSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentAssignerSpec2 extends SegmentAssignerSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentAssignerSpec3 extends SegmentAssignerSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class SegmentAssignerSpec extends TestBase {
  implicit val ordering = KeyOrder.default

  "SegmentAssign.assign" should {

    "assign KeyValues to the first Segment if there is only one Segment" in {
      val keyValues = randomIntKeyValues(count = 10)

      val segment = TestSegment().assertGet

      val result = SegmentAssigner.assign(keyValues, List(segment))
      result.size shouldBe 1
      result.keys.head.path.fileId.assertGet._1 shouldBe 1
      result.values.head shouldBe(keyValues, ignoreValueOffset = true)
    }

    "assign KeyValues to second Segment when none of the keys belong to the first Segment" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).get
      val segment2 = TestSegment(Slice(Transient.Put(3)).updateStats).get
      val segments = Set(segment1, segment2)

      val result = SegmentAssigner.assign(Slice(Transient.Put(4), Transient.Put(5), Transient.Put(6)).updateStats, segments)
      result.size shouldBe 1
      result.keys.head.path shouldBe segment2.path
    }

    "assign gap KeyValue to the first Segment, if the first Segment is smaller then the Second segment " +
      "and at least one key-value is already assigned to the first Segment" in {
      val segment1 = TestSegment(Slice(Transient.Put(1)).updateStats).get
      val segment2 = TestSegment(Slice(Transient.Put(3), Transient.Put(4)).updateStats).get
      val segments = Set(segment1, segment2)

      //1 will get assigned to first segment, 2 is a gap key and since first segment is not empty,
      // 2 gets assigned to first Segment
      val keyValues = Slice(Transient.Put(1, 1), Transient.Put(2))
      val result = SegmentAssigner.assign(keyValues, segments)
      result.size shouldBe 1
      result.keys.head.path shouldBe segment1.path
      result.values.head.shouldBe(keyValues, ignoreStats = true)
    }

    "assign gap KeyValue to the second Segment, if the first Segment is smaller then the Second segment but " +
      "first Segment current has no other key-values assigned to it" in {
      val segment1 = TestSegment(Slice(Transient.Put(1)).updateStats).get
      val segment2 = TestSegment(Slice(Transient.Put(3), Transient.Put(4)).updateStats).get
      val segments = Set(segment1, segment2)

      val result = SegmentAssigner.assign(Slice(Transient.Put(2)), segments)
      result.size shouldBe 1
      result.keys.head.path shouldBe segment2.path
    }

    "assign key value to the first segment when the key is the new smallest" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).get
      val segment2 = TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get
      val segments = Set(segment1, segment2)

      val result = SegmentAssigner.assign(Slice(Transient.Put(0)), segments)
      result.size shouldBe 1
      result.keys.head.path shouldBe segment1.path
    }

    "assign key value to the last segment when the key is the new largest" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).get
      val segment2 = TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get
      val segment3 = TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get
      val segment4 = TestSegment(Slice(Transient.Put(8), Transient.Put(9)).updateStats).get
      val segments = Set(segment1, segment2, segment3, segment4)

      val result = SegmentAssigner.assign(Slice(Transient.Put(10)), segments)
      result.size shouldBe 1
      result.keys.head.path shouldBe segment4.path
    }

    "assign all KeyValues to their target Segments" in {
      val keyValues = Slice(Transient.Put(1), Transient.Put(2), Transient.Put(3), Transient.Put(4), Transient.Put(5)).updateStats
      val segment1 = TestSegment(Slice(Transient.Put(key = 1, value = 1))).assertGet
      val segment2 = TestSegment(Slice(Transient.Put(key = 2, value = 2))).assertGet
      val segment3 = TestSegment(Slice(Transient.Put(key = 3, value = 3))).assertGet
      val segment4 = TestSegment(Slice(Transient.Put(key = 4, value = 4))).assertGet
      val segment5 = TestSegment(Slice(Transient.Put(key = 5, value = 5))).assertGet

      val segments = List(segment1, segment2, segment3, segment4, segment5)

      val result = SegmentAssigner.assign(keyValues, segments)
      result.size shouldBe 5

      //sort them by the fileId, so it's easier to test
      val resultArray = result.toArray.sortBy(_._1.path.fileId.get._1)

      resultArray(0)._1.path shouldBe segment1.path
      resultArray(0)._2 should have size 1
      resultArray(0)._2.head.key shouldBe 1

      resultArray(1)._1.path shouldBe segment2.path
      resultArray(1)._2 should have size 1
      resultArray(1)._2.head.key shouldBe 2

      resultArray(2)._1.path shouldBe segment3.path
      resultArray(2)._2 should have size 1
      resultArray(2)._2.head.key shouldBe 3

      resultArray(3)._1.path shouldBe segment4.path
      resultArray(3)._2 should have size 1
      resultArray(3)._2.head.key shouldBe 4

      resultArray(4)._1.path shouldBe segment5.path
      resultArray(4)._2 should have size 1
      resultArray(4)._2.head.key shouldBe 5
    }
  }
}
