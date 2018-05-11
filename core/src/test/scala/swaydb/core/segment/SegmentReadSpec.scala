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

import java.nio.file.{Files, NoSuchFileException}

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.TestBase
import swaydb.core.data.Transient.Remove
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data._
import swaydb.core.map.serializer.RangeValueSerializers
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.one.SegmentWriter
import swaydb.data.segment.MaxKey.{Fixed, Range}
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.util.Random

//@formatter:off
class SegmentReadSpec0 extends SegmentReadSpec {
  val keyValuesCount = 100
}

class SegmentReadSpec1 extends SegmentReadSpec {
  val keyValuesCount = 100
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentReadSpec2 extends SegmentReadSpec {
  val keyValuesCount = 100
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentReadSpec3 extends SegmentReadSpec {
  val keyValuesCount = 1000
  override def inMemoryStorage = true
}
//@formatter:on

trait SegmentReadSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val ordering = KeyOrder.default
  val keyValuesCount: Int

  "Segment.belongsTo" should {
    "return true if the input key-value belong to the Segment else false when the Segment contains no Range key-value" in {
      val segment = TestSegment(Slice(Transient.Put(1), Transient.Remove(5)).updateStats).assertGet

      Segment.belongsTo(Transient.Put(0), segment) shouldBe false

      Segment.belongsTo(Transient.Put(1), segment) shouldBe true
      Segment.belongsTo(Transient.Put(2), segment) shouldBe true
      Segment.belongsTo(Remove(3), segment) shouldBe true
      Segment.belongsTo(Transient.Range[FromValue, RangeValue](3, 10, randomFromValueOption(), randomRangeValue()), segment) shouldBe true
      Segment.belongsTo(Transient.Put(4), segment) shouldBe true
      Segment.belongsTo(Remove(5), segment) shouldBe true
      Segment.belongsTo(Transient.Range[FromValue, RangeValue](5, 10, randomFromValueOption(), randomRangeValue()), segment) shouldBe true

      Segment.belongsTo(Remove(6), segment) shouldBe false
      Segment.belongsTo(Transient.Range[FromValue, RangeValue](6, 10, randomFromValueOption(), randomRangeValue()), segment) shouldBe false

      segment.close.assertGet
    }

    "return true if the input key-value belong to the Segment else false when the Segment's max key is a Range key-value" in {
      val segment = TestSegment(Slice(Transient.Put(1), Transient.Range[FromValue, RangeValue](5, 10, None, Value.Remove(None))).updateStats).assertGet

      Segment.belongsTo(Transient.Put(0), segment) shouldBe false

      Segment.belongsTo(Transient.Put(1), segment) shouldBe true
      Segment.belongsTo(Transient.Put(2), segment) shouldBe true
      Segment.belongsTo(Remove(3), segment) shouldBe true
      Segment.belongsTo(Transient.Range[FromValue, RangeValue](3, 10, randomFromValueOption(), randomRangeValue()), segment) shouldBe true
      Segment.belongsTo(Transient.Put(4), segment) shouldBe true
      Segment.belongsTo(Remove(5), segment) shouldBe true
      Segment.belongsTo(Remove(6), segment) shouldBe true
      Segment.belongsTo(Remove(7), segment) shouldBe true
      Segment.belongsTo(Remove(10), segment) shouldBe false

      Segment.belongsTo(Transient.Range[FromValue, RangeValue](9, 10, randomFromValueOption(), randomRangeValue()), segment) shouldBe true
      Segment.belongsTo(Transient.Range[FromValue, RangeValue](10, 11, randomFromValueOption(), randomRangeValue()), segment) shouldBe false

      segment.close.assertGet
    }

    "return true if the input key-value belong to the Segment else false when the Segment's min key is a Range key-value" in {
      val segment = TestSegment(Slice(Transient.Range[FromValue, RangeValue](1, 10, None, Value.Remove(None)), Transient.Put(11)).updateStats).assertGet

      Segment.belongsTo(Transient.Put(0), segment) shouldBe false

      Segment.belongsTo(Transient.Put(1), segment) shouldBe true
      Segment.belongsTo(Transient.Put(2), segment) shouldBe true
      Segment.belongsTo(Remove(3), segment) shouldBe true
      Segment.belongsTo(Transient.Range[FromValue, RangeValue](3, 10, None, Value.Remove(None)), segment) shouldBe true
      Segment.belongsTo(Transient.Put(4), segment) shouldBe true
      Segment.belongsTo(Remove(5), segment) shouldBe true
      Segment.belongsTo(Remove(6), segment) shouldBe true
      Segment.belongsTo(Remove(7), segment) shouldBe true
      Segment.belongsTo(Remove(10), segment) shouldBe true

      Segment.belongsTo(Transient.Range[FromValue, RangeValue](9, 10, randomFromValueOption(), randomRangeValue()), segment) shouldBe true
      Segment.belongsTo(Transient.Range[FromValue, RangeValue](10, 11, randomFromValueOption(), randomRangeValue()), segment) shouldBe true

      segment.close.assertGet
    }
  }

  "Segment.rangeBelongsTo" should {
    "return true for overlapping KeyValues else false for Segments if the Segment's last key-value is not a Range" in {
      val segment = TestSegment(Slice(Transient.Put(1), Transient.Remove(5)).updateStats).assertGet

      //0 - 0
      //      1 - 5
      Segment.overlaps(0, 0, segment) shouldBe false
      //  0 - 1
      //      1 - 5
      Segment.overlaps(0, 1, segment) shouldBe true
      //    0 - 2
      //      1 - 5
      Segment.overlaps(0, 2, segment) shouldBe true
      //    0   - 5
      //      1 - 5
      Segment.overlaps(0, 5, segment) shouldBe true
      //    0   -   6
      //      1 - 5
      Segment.overlaps(0, 6, segment) shouldBe true


      //      1-2
      //      1 - 5
      Segment.overlaps(1, 2, segment) shouldBe true
      //      1-4
      //      1 - 5
      Segment.overlaps(1, 4, segment) shouldBe true
      //      1 - 5
      //      1 - 5
      Segment.overlaps(1, 5, segment) shouldBe true
      //      1 -  6
      //      1 - 5
      Segment.overlaps(1, 6, segment) shouldBe true


      //       2-4
      //      1 - 5
      Segment.overlaps(2, 4, segment) shouldBe true
      //       2- 5
      //      1 - 5
      Segment.overlaps(2, 5, segment) shouldBe true
      //        2 - 6
      //      1 - 5
      Segment.overlaps(2, 6, segment) shouldBe true
      //          5 - 6
      //      1 - 5
      Segment.overlaps(5, 6, segment) shouldBe true
      //            6 - 7
      //      1 - 5
      Segment.overlaps(6, 7, segment) shouldBe false

      //wide outer overlap
      //    0   -   6
      //      1 - 5
      Segment.overlaps(0, 6, segment) shouldBe true

      segment.close.assertGet
    }

    "return true for overlapping KeyValues else false for Segments if the Segment's last key-value is a Range" in {
      val segment = TestSegment(Slice(Transient.Put(1), Transient.Range[FromValue, RangeValue](5, 10, None, Value.Remove(None))).updateStats).assertGet


      //0 - 0
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 0, segment) shouldBe false
      //  0 - 1
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 1, segment) shouldBe true
      //    0 - 2
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 2, segment) shouldBe true
      //    0 -    5
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 5, segment) shouldBe true
      //    0   -    7
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 7, segment) shouldBe true
      //    0     -    10
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 10, segment) shouldBe true
      //    0      -      11
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 11, segment) shouldBe true

      //      1 - 5
      //      1 - (5 - 10(EX))
      Segment.overlaps(1, 5, segment) shouldBe true
      //      1 -   6
      //      1 - (5 - 10(EX))
      Segment.overlaps(1, 6, segment) shouldBe true
      //      1 -      10
      //      1 - (5 - 10(EX))
      Segment.overlaps(1, 10, segment) shouldBe true
      //      1 -          11
      //      1 - (5 - 10(EX))
      Segment.overlaps(1, 11, segment) shouldBe true

      //       2-4
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 4, segment) shouldBe true
      //       2 - 5
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 5, segment) shouldBe true
      //       2 -   6
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 6, segment) shouldBe true
      //       2   -    10
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 10, segment) shouldBe true
      //       2     -    11
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 11, segment) shouldBe true


      //          5 - 6
      //      1 - (5 - 10(EX))
      Segment.overlaps(5, 6, segment) shouldBe true
      //          5 -  10
      //      1 - (5 - 10(EX))
      Segment.overlaps(5, 10, segment) shouldBe true
      //          5   -   11
      //      1 - (5 - 10(EX))
      Segment.overlaps(5, 11, segment) shouldBe true
      //            6 - 7
      //      1 - (5 - 10(EX))
      Segment.overlaps(6, 7, segment) shouldBe true
      //             8 - 9
      //      1 - (5   -   10(EX))
      Segment.overlaps(8, 9, segment) shouldBe true
      //             8   - 10
      //      1 - (5   -   10(EX))
      Segment.overlaps(8, 10, segment) shouldBe true
      //               9 - 10
      //      1 - (5   -   10(EX))
      Segment.overlaps(9, 10, segment) shouldBe true
      //               9 -   11
      //      1 - (5   -   10(EX))
      Segment.overlaps(9, 11, segment) shouldBe true
      //                   10  -   11
      //      1 - (5   -   10(EX))
      Segment.overlaps(10, 11, segment) shouldBe false

      //                      11  -   11
      //      1 - (5   -   10(EX))
      Segment.overlaps(11, 11, segment) shouldBe false

      //wide outer overlap
      //    0   -   6
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 6, segment) shouldBe true

      segment.close.assertGet
    }
  }

  "Segment.partitionOverlapping" should {
    "partition overlapping and non-overlapping Segments" in {
      //0-1, 2-3
      //         4-5, 6-7
      var segments1 = Seq(TestSegment(Slice(Transient.Put(0), Transient.Remove(1)).updateStats).assertGet, TestSegment(Slice(Transient.Put(2), Transient.Remove(3)).updateStats).assertGet)
      var segments2 = Seq(TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet, TestSegment(Slice(Transient.Put(6), Transient.Remove(7)).updateStats).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

      //0-1,   3-4
      //         4-5, 6-7
      segments1 = Seq(TestSegment(Slice(Transient.Put(0), Transient.Remove(1)).updateStats).assertGet, TestSegment(Slice(Transient.Put(3), Transient.Remove(4)).updateStats).assertGet)
      segments2 = Seq(TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet, TestSegment(Slice(Transient.Put(6), Transient.Remove(7)).updateStats).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))

      //0-1,   3 - 5
      //         4-5, 6-7
      segments1 = Seq(TestSegment(Slice(Transient.Put(0), Transient.Remove(1)).updateStats).assertGet, TestSegment(Slice(Transient.Put(3), Transient.Remove(5)).updateStats).assertGet)
      segments2 = Seq(TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet, TestSegment(Slice(Transient.Put(6), Transient.Remove(7)).updateStats).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))


      //0-1,      6-8
      //      4-5,    10-20
      segments1 = Seq(TestSegment(Slice(Transient.Put(0), Transient.Remove(1)).updateStats).assertGet, TestSegment(Slice(Transient.Put(6), Transient.Remove(8)).updateStats).assertGet)
      segments2 = Seq(TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet, TestSegment(Slice(Transient.Put(10), Transient.Remove(20)).updateStats).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

      //0-1,             20 - 21
      //      4-5,    10-20
      segments1 = Seq(TestSegment(Slice(Transient.Put(0), Transient.Remove(1)).updateStats).assertGet, TestSegment(Slice(Transient.Put(20), Transient.Remove(21)).updateStats).assertGet)
      segments2 = Seq(TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet, TestSegment(Slice(Transient.Put(10), Transient.Remove(20)).updateStats).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))

      //0-1,               21 - 22
      //      4-5,    10-20
      segments1 = Seq(TestSegment(Slice(Transient.Put(0), Transient.Remove(1)).updateStats).assertGet, TestSegment(Slice(Transient.Put(21), Transient.Remove(22)).updateStats).assertGet)
      segments2 = Seq(TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet, TestSegment(Slice(Transient.Put(10), Transient.Remove(20)).updateStats).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

      //0          -          22
      //      4-5,    10-20
      segments1 = Seq(TestSegment(Slice(Transient.Range[FromValue, RangeValue](0, 22, None, Value.Remove(None))).updateStats).assertGet)
      segments2 = Seq(TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet, TestSegment(Slice(Transient.Put(10), Transient.Remove(20)).updateStats).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(segments1, Seq.empty)
    }
  }

  "Segment.overlaps" should {
    "return true for overlapping Segments else false for Segments without Ranges" in {
      //0 1
      //    2 3
      var segment1 = TestSegment(Slice(Transient.Put(0), Transient.Remove(1)).updateStats).assertGet
      var segment2 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //1 2
      //  2 3
      segment1 = TestSegment(Slice(Transient.Put(1), Transient.Remove(2)).updateStats).assertGet
      segment2 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //2 3
      //2 3
      segment1 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Put(2), Transient.Remove(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //  3 4
      //2 3
      segment1 = TestSegment(Slice(Remove(3), Transient.Put(4)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Put(2), Transient.Remove(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //    4 5
      //2 3
      segment1 = TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Put(2), Transient.Remove(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //0       10
      //   2 3
      segment1 = TestSegment(Slice(Remove(0), Transient.Put(10)).updateStats).assertGet
      segment2 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //   2 3
      //0       10
      segment1 = TestSegment(Slice(Transient.Put(2), Transient.Put(3)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Put(0), Transient.Put(10)).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      segment1.close.assertGet
      segment2.close.assertGet
    }

    "return true for overlapping Segments if the target Segment's maxKey is a Range key" in {
      //0 1
      //    2 3
      var segment1 = TestSegment(Slice(Transient.Put(0), Transient.Remove(1)).updateStats).assertGet
      var segment2 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false
      //range over range
      segment1 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](0, 1, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //1 2
      //  2 3
      segment1 = TestSegment(Slice(Transient.Put(1), Transient.Remove(2)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](1, 2, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //1   3
      //  2 3
      segment1 = TestSegment(Slice(Transient.Put(1), Transient.Remove(3)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](1, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //2 3
      //2 3
      segment1 = TestSegment(Slice(Remove(2), Transient.Put(3)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //  3 4
      //2 3
      segment1 = TestSegment(Slice(Remove(3), Transient.Put(4)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false
      segment1 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](3, 4, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //    4 5
      //2 3
      segment1 = TestSegment(Slice(Transient.Put(4), Transient.Remove(5)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false
      segment1 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](4, 5, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //0       10
      //   2 3
      segment1 = TestSegment(Slice(Remove(0), Transient.Put(10)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](0, 10, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //   2 3
      //0       10
      segment1 = TestSegment(Slice(Transient.Put(2), Transient.Put(3)).updateStats).assertGet
      segment2 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](0, 10, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(Transient.Range[FromValue, RangeValue](2, 3, None, Value.Remove(None))).updateStats).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      segment1.close.assertGet
      segment2.close.assertGet
    }
  }

  "Segment.nonOverlapping and overlapping" should {
    "return non overlapping Segments" in {
      //0-1, 2-3
      //         4-5, 6-7
      var segments1 = List(TestSegment(Slice(Transient.Put(0), Transient.Put(1)).updateStats).get, TestSegment(Slice(Transient.Put(2), Transient.Put(3)).updateStats).get)
      var segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) shouldBe segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) shouldBe segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty


      //2-3, 4-5
      //     4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(2), Transient.Put(3)).updateStats).get, TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain only segments1.head.path
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain only segments2.last.path
      Segment.overlaps(segments1, segments2).map(_.path) should contain only segments1.last.path
      Segment.overlaps(segments2, segments1).map(_.path) should contain only segments2.head.path

      //4-5, 6-7
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) shouldBe empty
      Segment.nonOverlapping(segments2, segments1).map(_.path) shouldBe empty
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe segments1.map(_.path)
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe segments2.map(_.path)

      //     6-7, 8-9
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get, TestSegment(Slice(Transient.Put(8), Transient.Put(9)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain only segments1.last.path
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain only segments2.head.path
      Segment.overlaps(segments1, segments2).map(_.path) should contain only segments1.head.path
      Segment.overlaps(segments2, segments1).map(_.path) should contain only segments2.last.path

      //         8-9, 10-11
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(8), Transient.Put(9)).updateStats).get, TestSegment(Slice(Transient.Put(10), Transient.Put(11)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain allElementsOf segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain allElementsOf segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty

      //1-2            10-11
      //     4-5, 6-7
      segments1 = List(TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats).get, TestSegment(Slice(Transient.Put(10), Transient.Put(11)).updateStats).get)
      segments2 = List(TestSegment(Slice(Transient.Put(4), Transient.Put(5)).updateStats).get, TestSegment(Slice(Transient.Put(6), Transient.Put(7)).updateStats).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain allElementsOf segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain allElementsOf segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty
    }
  }

  "Segment.tempMinMaxKeyValues" should {
    "return key-values with Segments min and max keys only" in {
      val segment1 = TestSegment(randomIntKeyValues(keyValuesCount, addRandomRemoves = true, addRandomRanges = true)).assertGet
      val segment2 = TestSegment(randomIntKeyValues(keyValuesCount, startId = Some(segment1.maxKey.maxKey.read[Int] + 1), addRandomRemoves = true, addRandomRanges = true)).assertGet
      val segment3 = TestSegment(randomIntKeyValues(keyValuesCount, startId = Some(segment2.maxKey.maxKey.read[Int] + 1), addRandomRemoves = true, addRandomRanges = true)).assertGet
      val segment4 = TestSegment(randomIntKeyValues(keyValuesCount, startId = Some(segment3.maxKey.maxKey.read[Int] + 1), addRandomRemoves = true, addRandomRanges = true)).assertGet

      val segments = Seq(segment1, segment2, segment3, segment4)

      val expectedTempKeyValues: Seq[Transient] =
        segments flatMap {
          segment =>
            segment.maxKey match {
              case Fixed(maxKey) =>
                Seq(Transient.Put(segment.minKey), Transient.Put(maxKey))
              case Range(fromKey, maxKey) =>
                Seq(Transient.Put(segment.minKey), Transient.Range[FromValue, RangeValue](fromKey, maxKey, None, Value.Update(maxKey)))
            }
        }

      Segment.tempMinMaxKeyValues(segments) shouldBe expectedTempKeyValues
    }
  }

  "Segment.overlapsWithBusySegments" should {
    "return true or false if input Segments overlap or do not overlap with busy Segments respectively" in {

      val targetSegments = {
        TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats) ::
          TestSegment(Slice(Transient.Put(3), Transient.Put(4)).updateStats) ::
          TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats) ::
          TestSegment(Slice(Transient.Put(9), Transient.Put(10)).updateStats) ::
          Nil
      }.map(_.assertGet)

      //0-1
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      var inputSegments = Seq(TestSegment(Slice(Transient.Put(0), Transient.Put(1)).updateStats)).map(_.assertGet)
      var busySegments = Seq(TestSegment(Slice(Transient.Put(3), Transient.Put(4)).updateStats), TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe false

      //     1-2
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe false

      //          3-4
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(3), Transient.Put(2)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe true

      //               5-6
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(5), Transient.Put(6)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe true

      //                         9-10
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(9), Transient.Put(10)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe false

      //               5-6
      //     1-2            7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(Transient.Put(5), Transient.Put(6)).updateStats)).map(_.assertGet)
      busySegments = {
        TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats) ::
          TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats) ::
          Nil
      }.map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe true

      //               5-6
      //     1-2                 9-10
      //     1-2, 3-4, ---, 7-8, 9-10
      busySegments = Seq(TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats), TestSegment(Slice(Transient.Put(9), Transient.Put(10)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe false
    }

    "return true or false if input map overlap or do not overlap with busy Segments respectively" in {

      val targetSegments = {
        TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats) ::
          TestSegment(Slice(Transient.Put(3), Transient.Put(4)).updateStats) ::
          TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats) ::
          TestSegment(Slice(Transient.Put(9), Transient.Put(10)).updateStats) ::
          Nil
      }.map(_.assertGet)

      //0-1
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      var inputMap = TestMap(Slice(Memory.Put(0), Memory.Put(1)))
      var busySegments = Seq(TestSegment(Slice(Transient.Put(3), Transient.Put(4)).updateStats), TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe false

      //     1-2
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(Memory.Put(1), Memory.Put(2)))
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe false

      //          3-4
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(Memory.Put(3), Memory.Put(2)))
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe true

      //               5-6
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(Memory.Put(5), Memory.Put(6)))
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe true

      //                         9-10
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(Memory.Put(9), Memory.Put(10)))
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe false

      //               5-6
      //     1-2            7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(Memory.Put(5), Memory.Put(6)))
      busySegments = {
        TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats) ::
          TestSegment(Slice(Transient.Put(7), Transient.Put(8)).updateStats) ::
          Nil
      }.map(_.assertGet)
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe true

      //               5-6
      //     1-2                 9-10
      //     1-2, 3-4, ---, 7-8, 9-10
      busySegments = Seq(TestSegment(Slice(Transient.Put(1), Transient.Put(2)).updateStats), TestSegment(Slice(Transient.Put(9), Transient.Put(10)).updateStats)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe false
    }
  }

  "Segment.getAllKeyValues" should {
    "get KeyValues from multiple Segments" in {
      val keyValues1 = randomIntKeyValues(keyValuesCount, addRandomRemoves = true)
      val keyValues2 = randomIntKeyValues(keyValuesCount, addRandomRemoves = true)
      val keyValues3 = randomIntKeyValues(keyValuesCount, addRandomRemoves = true)

      val segment1 = TestSegment(keyValues1).assertGet
      val segment2 = TestSegment(keyValues2).assertGet
      val segment3 = TestSegment(keyValues3).assertGet

      val all = Slice((keyValues1 ++ keyValues2 ++ keyValues3).toArray).updateStats
      val (slice, deadline) = SegmentWriter.toSlice(all, 0.1).assertGet
      slice.size shouldBe all.last.stats.segmentSize
      deadline shouldBe empty

      val readKeyValues = Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).assertGet

      readKeyValues shouldBe all
    }

    "fail read if reading any one Segment fails for persistent Segments" in {
      val keyValues1 = randomIntKeyValues(keyValuesCount, addRandomRemoves = true)
      val keyValues2 = randomIntKeyValues(keyValuesCount, addRandomRemoves = true)
      val keyValues3 = randomIntKeyValues(keyValuesCount, addRandomRemoves = true)

      val segment1 = TestSegment(keyValues1).assertGet
      val segment2 = TestSegment(keyValues2).assertGet
      val segment3 = TestSegment(keyValues3).assertGet

      segment3.delete.assertGet //delete a segment so that there is a failure.

      Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).failed.assertGet shouldBe a[NoSuchFileException]
    }

    "fail read if reading any one Segment file is corrupted" in {
      if (persistent) {
        val keyValues1 = randomIntKeyValues(keyValuesCount, addRandomRemoves = true)
        val keyValues2 = randomIntKeyStringValues(keyValuesCount)
        val keyValues3 = randomIntKeyStringValues(keyValuesCount)

        val segment1 = TestSegment(keyValues1).assertGet
        val segment2 = TestSegment(keyValues2).assertGet
        val segment3 = TestSegment(keyValues3).assertGet

        val bytes = Files.readAllBytes(segment2.path)

        Files.write(segment2.path, bytes.drop(1))
        Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).failed.assertGet shouldBe a[SegmentCorruptionException]

        Files.write(segment2.path, bytes.dropRight(1))
        Segment.getAllKeyValues(0.1, Seq(segment2)).failed.assertGet shouldBe a[SegmentCorruptionException]

        Files.write(segment2.path, bytes.drop(10))
        Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).failed.assertGet shouldBe a[SegmentCorruptionException]

        Files.write(segment2.path, bytes.dropRight(1))
        Segment.getAllKeyValues(0.1, Seq(segment1, segment2, segment3)).failed.assertGet shouldBe a[SegmentCorruptionException]
      } else {
        //memory files do not require this test
      }
    }
  }

  "Segment.getAll" should {
    "read full index" in {
      val keyValues = randomIntKeyValues(keyValuesCount, addRandomRemoves = true, addRandomRanges = true)
      val segment = TestSegment(keyValues).assertGet

      if (persistent) segment.isCacheEmpty shouldBe true

      val segmentKeyValues = segment.getAll().assertGet.toSlice

      (0 until keyValues.size).foreach {
        index =>
          val actualKeyValue = keyValues(index)
          val segmentKeyValue = segmentKeyValues(index)

          //ensure that indexEntry's values are not already read as they are lazily fetched from the file.
          //values with Length 0 and non Range key-values always have isValueDefined set to true as they do not required disk seek.
          segmentKeyValue match {
            case persistent: Persistent if !persistent.isRemove =>
              persistent.isValueDefined shouldBe false

            case _ =>
          }

          actualKeyValue shouldBe segmentKeyValue //after comparison values should be populated.

          segmentKeyValue match {
            case persistent: Persistent =>
              persistent.isValueDefined shouldBe true
            case _ =>
          }
      }
    }
  }

  "Segment.getNearestDeadline" should {
    "return None for empty deadlines" in {
      Segment.getNearestDeadline(None, None) shouldBe empty
    }

    "return the a Segment with earliest deadline" in {
      val deadline1 = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow

      Segment.getNearestDeadline(Some(deadline1), None) should contain(deadline1)
      Segment.getNearestDeadline(Some(deadline2), None) should contain(deadline2)

      Segment.getNearestDeadline(None, Some(deadline1)) should contain(deadline1)
      Segment.getNearestDeadline(None, Some(deadline2)) should contain(deadline2)

      Segment.getNearestDeadline(Some(deadline1), Some(deadline2)) should contain(deadline1)
      Segment.getNearestDeadline(Some(deadline2), Some(deadline1)) should contain(deadline1)
    }

    "return earliest deadline from key-values" in {
      runThis(10.times) {
        val deadlines = (1 to 8).map(_.seconds.fromNow)

        val shuffledDeadlines = Random.shuffle(deadlines)

        //populated deadlines from shuffled deadlines.
        val keyValues =
          Slice(
            Memory.Remove(1, shuffledDeadlines(0)),
            Memory.Put(2, 1, shuffledDeadlines(1)),
            Memory.Update(3, 10, shuffledDeadlines(2)),
            Memory.Range(4, 10, None, Value.Remove(shuffledDeadlines(3))),
            Memory.Range(5, 10, Value.Put(10, shuffledDeadlines(4)), Value.Remove(shuffledDeadlines(5))),
            Memory.Range(6, 10, Value.Put(10, shuffledDeadlines(6)), Value.Update(None, Some(shuffledDeadlines(7))))
          )

        Segment.getNearestDeadline(keyValues).assertGet shouldBe deadlines.head
      }
    }
  }

  "Segment.getNearestDeadlineSegment" should {
    "return None deadline if non of the key-values in the Segments contains deadline" in {

      val segment1 = TestSegment(randomIntKeyValues(100)).assertGet
      val segment2 = TestSegment(randomIntKeyValues(100)).assertGet

      Segment.getNearestDeadlineSegment(segment1, segment2) shouldBe empty

    }

    "return deadline if one of the Segments contains deadline" in {

      val keyValues = randomIntKeyValuesMemory(100)

      val deadline = 100.seconds.fromNow
      val keyValuesWithDeadline = Memory.Remove(keyValues.last.key.readInt() + 10000, deadline)

      val segment1 = TestSegment((keyValues ++ Seq(keyValuesWithDeadline)).toTransient).assertGet
      val segment2 = TestSegment(randomIntKeyValues(100)).assertGet

      Segment.getNearestDeadlineSegment(segment1, segment2).flatMap(_.nearestExpiryDeadline) should contain(deadline)
      Segment.getNearestDeadlineSegment(segment2, segment1).flatMap(_.nearestExpiryDeadline) should contain(deadline)
    }

    "return deadline" in {

      val keyValues1 = randomizedIntKeyValues(100)
      val keyValues2 = randomizedIntKeyValues(100)

      val segment1 = TestSegment(keyValues1).assertGet
      val segment2 = TestSegment(keyValues2).assertGet

      def nearest(previous: Option[Deadline], next: Option[Deadline]) =
        (previous, next) match {
          case (None, None) => None
          case (None, next @ Some(_)) => next
          case (previous @ Some(_), None) => previous
          case (previous @ Some(deadline1), next @ Some(deadline2)) =>
            if (deadline1 < deadline2)
              previous
            else
              next
        }

      val deadline =
        (keyValues1 ++ keyValues2).foldLeft(Option.empty[Deadline]) {
          case (previous, keyValue) =>
            keyValue match {
              case fixed: KeyValue.WriteOnly.Fixed =>
                nearest(previous, fixed.deadline)
              case range: KeyValue.WriteOnly.Range =>
                range.fetchFromAndRangeValue.assertGet match {
                  case (Some(fromValue), rangeValue) =>
                    nearest(nearest(previous, fromValue.deadline), rangeValue.deadline)
                  case (None, rangeValue) =>
                    nearest(previous, rangeValue.deadline)
                }
            }
        }

      Segment.getNearestDeadlineSegment(segment1, segment2).flatMap(_.nearestExpiryDeadline) shouldBe deadline
      Segment.getNearestDeadlineSegment(segment2, segment1).flatMap(_.nearestExpiryDeadline) shouldBe deadline
      Segment.getNearestDeadlineSegment(segment1 :: segment2 :: Nil).flatMap(_.nearestExpiryDeadline) shouldBe deadline
    }
  }

}
