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
import swaydb.core.data.{Transient, Value}
import swaydb.core.util.FileUtil._
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.core.util.PipeOps._

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
      val keyValues = randomIntKeyValues(count = 10, addRandomDeletes = true, addRandomRanges = true)

      val segment = TestSegment().assertGet

      val result = SegmentAssigner.assign(keyValues, List(segment)).assertGet
      result.size shouldBe 1
      result.keys.head.path shouldBe segment.path
      result.values.head shouldBe(keyValues, ignoreValueOffset = true)
    }

    "assign KeyValues to second Segment when none of the keys belong to the first Segment" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove)).updateStats).assertGet
      val segment2 = TestSegment(Slice(Transient.Put(10)).updateStats).assertGet
      val segments = Seq(segment1, segment2)

      val result =
        SegmentAssigner.assign(
          keyValues =
            Slice(
              Transient.Put(10),
              Transient.Range[Value.Fixed, Value.Fixed](11, 20, None, Value.Put(11)),
              Transient.Remove(20)
            ).updateStats,
          segments = segments
        ).assertGet

      result.size shouldBe 1
      result.keys.head.path shouldBe segment2.path
    }

    "assign gap KeyValue to the first Segment if the first Segment already has a key-value assigned to it" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove)).updateStats).assertGet
      val segment2 = TestSegment(Slice(Transient.Remove(20)).updateStats).assertGet
      val segments = Seq(segment1, segment2)

      //1 belongs to first Segment, 15 is a gap key and since first segment is not empty, it will get assigned 15.
      val keyValues =
        Slice(
          Transient.Put(1, 1),
          Transient.Put(15)
        )

      val result = SegmentAssigner.assign(keyValues, segments).assertGet
      result.size shouldBe 1
      result.keys.head.path shouldBe segment1.path
      result.values.head.shouldBe(keyValues, ignoreStats = true)
    }

    "assign gap KeyValue to the second Segment if the first Segment has no key-value assigned to it" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove)).updateStats).assertGet
      val segment2 = TestSegment(Slice(Transient.Remove(20)).updateStats).assertGet
      val segments = Seq(segment1, segment2)

      //15 is a gap key but no key-values are assigned to segment1 so segment2 will get this key-value.
      val keyValues =
        Slice(
          Transient.Put(15)
        )

      val result = SegmentAssigner.assign(keyValues, segments).assertGet
      result.size shouldBe 1
      result.keys.head.path shouldBe segment2.path
      result.values.head.shouldBe(keyValues, ignoreStats = true)
    }

    "assign gap Range KeyValue to all Segments that fall within the Range's toKey" in {
      // 1 - 10(exclusive)
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Range[Value.Fixed, Value.Fixed](2, 10, None, Value.Remove)).updateStats).assertGet
      // 20 - 20
      val segment2 = TestSegment(Slice(Transient.Remove(20)).updateStats).assertGet
      //21 - 30
      val segment3 = TestSegment(Slice(Transient.Range[Value.Fixed, Value.Fixed](21, 30, None, Value.Remove), Transient.Put(30)).updateStats).assertGet
      //40 - 60
      val segment4 = TestSegment(Slice(Transient.Remove(40), Transient.Range[Value.Fixed, Value.Fixed](41, 50, None, Value.Remove), Transient.Put(60)).updateStats).assertGet
      //70 - 80
      val segment5 = TestSegment(Slice(Transient.Put(70), Transient.Remove(80)).updateStats).assertGet
      val segments = Seq(segment1, segment2, segment3, segment4, segment5)

      //15 is a gap key but no key-values are assigned to segment1 so segment2 will get this key-value.
      val keyValues =
        Slice(
          Transient.Range[Value.Fixed, Value.Fixed](15, 50, Some(Value.Remove), Value.Put(10))
        )

      val assignments = SegmentAssigner.assign(keyValues, segments).assertGet
      assignments.size shouldBe 3
      assignments.find(_._1 == segment2).assertGet._2 should contain only Transient.Range[Value.Fixed, Value.Fixed](15, 21, Some(Value.Remove), Value.Put(10))
      assignments.find(_._1 == segment3).assertGet._2 should contain only Transient.Range[Value.Fixed, Value.Fixed](21, 40, None, Value.Put(10))
      assignments.find(_._1 == segment4).assertGet._2 should contain only Transient.Range[Value.Fixed, Value.Fixed](40, 50, None, Value.Put(10))
    }

    "assign key value to the first segment when the key is the new smallest" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).assertGet
      val segment2 = TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).assertGet

      //segment1 - 1 - 2
      //segment2 - 4 - 5
      val segments = Seq(segment1, segment2)

      SegmentAssigner.assign(Slice(Transient.Put(0)), segments).assertGet ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment1.path
      }
    }

    "assign key value to the first segment and split out to other Segment when the key is the new smallest and the range spreads onto other Segments" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).assertGet
      val segment2 = TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).assertGet
      val segment3 = TestSegment(Slice(Transient.Range[Value.Fixed, Value.Fixed](6, 10, Some(Value.Remove), Value.Put(10)), Transient.Remove(10)).updateStats).assertGet

      //segment1 - 1 - 2
      //segment2 - 4 - 5
      //segment3 - 6 - 10
      val segments = Seq(segment1, segment2, segment3)

      //insert range 0 - 20. This overlaps all 3 Segment and key-values will get sliced and distributed to all Segments.
      SegmentAssigner.assign(Slice(Transient.Range[Value.Fixed, Value.Fixed](0, 20, Some(Value.Put(0)), Value.Remove)), segments).assertGet ==> {
        assignments =>
          assignments.size shouldBe 3
          assignments.find(_._1 == segment1).assertGet._2 should contain only Transient.Range[Value.Fixed, Value.Fixed](0, 4, Some(Value.Put(0)), Value.Remove)
          assignments.find(_._1 == segment2).assertGet._2 should contain only Transient.Range[Value.Fixed, Value.Fixed](4, 6, None, Value.Remove)
          assignments.find(_._1 == segment3).assertGet._2 should contain only Transient.Range[Value.Fixed, Value.Fixed](6, 20, None, Value.Remove)
      }
    }

    "assign key value to the last segment when the key is the new largest" in {
      val segment1 = TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).assertGet
      val segment2 = TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).assertGet
      val segment3 = TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).assertGet
      val segment4 = TestSegment(Slice(Transient.Put(8), Transient.Put(9)).updateStats).assertGet
      val segments = Seq(segment1, segment2, segment3, segment4)

      SegmentAssigner.assign(Slice(Transient.Put(10, "ten")), segments).assertGet ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment4.path
          result.values.head should contain only Transient.Put(10, "ten")
      }

      SegmentAssigner.assign(Slice(Transient.Remove(10)), segments).assertGet ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment4.path
          result.values.head should contain only Transient.Remove(10)
      }

      SegmentAssigner.assign(Slice(Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Put(10)), Value.Remove)), segments).assertGet ==> {
        result =>
          result.size shouldBe 1
          result.keys.head.path shouldBe segment4.path
          result.values.head should contain only Transient.Range[Value.Fixed, Value.Fixed](10, 20, Some(Value.Put(10)), Value.Remove)
      }
    }

    "assign all KeyValues to their target Segments" in {
      val keyValues = Slice(Transient.Put(1), Transient.Put(2), Transient.Put(3), Transient.Put(4), Transient.Put(5)).updateStats
      val segment1 = TestSegment(Slice(Transient.Put(key = 1, value = 1))).assertGet
      val segment2 = TestSegment(Slice(Transient.Put(key = 2, value = 2))).assertGet
      val segment3 = TestSegment(Slice(Transient.Put(key = 3, value = 3))).assertGet
      val segment4 = TestSegment(Slice(Transient.Put(key = 4, value = 4))).assertGet
      val segment5 = TestSegment(Slice(Transient.Put(key = 5, value = 5))).assertGet

      val segments = List(segment1, segment2, segment3, segment4, segment5)

      val result = SegmentAssigner.assign(keyValues, segments).assertGet
      result.size shouldBe 5

      //sort them by the fileId, so it's easier to test
      val resultArray = result.toArray.sortBy(_._1.path.fileId.assertGet._1)

      resultArray(0)._1.path shouldBe segment1.path
      resultArray(0)._2 should have size 1
      resultArray(0)._2.head.key shouldBe (1: Slice[Byte])

      resultArray(1)._1.path shouldBe segment2.path
      resultArray(1)._2 should have size 1
      resultArray(1)._2.head.key shouldBe (2: Slice[Byte])

      resultArray(2)._1.path shouldBe segment3.path
      resultArray(2)._2 should have size 1
      resultArray(2)._2.head.key shouldBe (3: Slice[Byte])

      resultArray(3)._1.path shouldBe segment4.path
      resultArray(3)._2 should have size 1
      resultArray(3)._2.head.key shouldBe (4: Slice[Byte])

      resultArray(4)._1.path shouldBe segment5.path
      resultArray(4)._2 should have size 1
      resultArray(4)._2.head.key shouldBe (5: Slice[Byte])
    }
  }
}
