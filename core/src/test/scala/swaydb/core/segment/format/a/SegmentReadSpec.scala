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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a

import java.nio.file.{Files, NoSuchFileException}
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.duration._
import scala.util.Random
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.IOAssert._
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.segment.Segment
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.{TestBase, TestData, TestTimer}
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

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
  val keyValuesCount = 100
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait SegmentReadSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val keyOrder = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.random

  def keyValuesCount: Int

  implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] =
    randomGroupingStrategyOption(keyValuesCount)

  "belongsTo" should {
    "return true if the input key-value belong to the Segment else false when the Segment contains no Range key-value" in {
      val segment = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(5)).toTransient).assertGet

      runThis(10.times) {
        Segment.belongsTo(randomFixedKeyValue(0), segment) shouldBe false

        //inner
        (1 to 5) foreach {
          i =>
            randomizedKeyValues(10, startId = Some(i)) foreach {
              keyValue =>
                if (keyValue.key.readInt() <= 5)
                  Segment.belongsTo(keyValue, segment) shouldBe true
            }
        }

        //outer
        (6 to 20) foreach {
          i =>
            randomizedKeyValues(10, startId = Some(i)) foreach {
              keyValue =>
                Segment.belongsTo(keyValue, segment) shouldBe false
            }
        }
      }
      segment.close.assertGet
    }

    "return true if the input key-value belong to the Segment else false when the Segment's max key is a Range key-value" in {
      val segment = TestSegment(Slice(randomFixedKeyValue(1), randomRangeKeyValue(5, 10)).toTransient).assertGet

      runThis(10.times) {
        Segment.belongsTo(randomFixedKeyValue(0), segment) shouldBe false

        //inner
        (1 to 9) foreach {
          i =>
            randomizedKeyValues(10, startId = Some(i)) foreach {
              keyValue =>
                if (keyValue.key.readInt() < 10)
                  Segment.belongsTo(keyValue, segment) shouldBe true
            }
        }

        //outer
        (10 to 20) foreach {
          i =>
            randomizedKeyValues(10, startId = Some(i)) foreach {
              keyValue =>
                Segment.belongsTo(keyValue, segment) shouldBe false
            }
        }
      }
      segment.close.assertGet
    }

    "return true if the input key-value belong to the Segment else false when the Segment's min key is a Range key-value" in {
      val segment = TestSegment(Slice(randomRangeKeyValue(1, 10), randomFixedKeyValue(11)).toTransient).assertGet

      runThis(10.times) {
        Segment.belongsTo(randomFixedKeyValue(0), segment) shouldBe false

        //inner
        (1 to 11) foreach {
          i =>
            randomizedKeyValues(10, startId = Some(i)) foreach {
              keyValue =>
                if (keyValue.key.readInt() < 10)
                  Segment.belongsTo(keyValue, segment) shouldBe true
            }
        }

        //outer
        (12 to 20) foreach {
          i =>
            randomizedKeyValues(10, startId = Some(i)) foreach {
              keyValue =>
                Segment.belongsTo(keyValue, segment) shouldBe false
            }
        }
      }

      segment.close.assertGet
    }

    "for randomizedKeyValues" in {
      val keyValues = randomizedKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet

      val minKeyInt = keyValues.head.key.readInt()
      val maxKey = getMaxKey(keyValues.last)
      val maxKeyInt = maxKey.maxKey.readInt()

      val leftOutKeysRange = minKeyInt - keyValuesCount until minKeyInt
      val innerKeyRange = if (maxKey.inclusive) minKeyInt to maxKeyInt else minKeyInt until maxKeyInt
      val rightOutKeyRange = if (maxKey.inclusive) maxKeyInt + 1 to maxKeyInt + keyValuesCount else maxKeyInt to maxKeyInt + keyValuesCount

      runThis(10.times) {
        leftOutKeysRange foreach {
          key =>
            Segment.belongsTo(randomFixedKeyValue(key), segment) shouldBe false
        }

        //inner
        innerKeyRange foreach {
          i =>
            val keyValues = randomizedKeyValues(keyValuesCount, startId = Some(i))
            keyValues foreach {
              keyValue =>
                if (keyValue.key.readInt() <= innerKeyRange.last)
                  Segment.belongsTo(keyValue, segment) shouldBe true
            }
        }

        //outer
        rightOutKeyRange foreach {
          i =>
            val keyValues = randomizedKeyValues(keyValuesCount, startId = Some(i))
            keyValues foreach {
              keyValue =>
                Segment.belongsTo(keyValue, segment) shouldBe false
            }
        }
      }
      segment.close.assertGet
    }
  }

  "rangeBelongsTo" should {
    "return true for overlapping KeyValues else false for Segments if the Segment's last key-value is not a Range" in {
      val segment = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(5)).toTransient).assertGet

      //0 - 0
      //      1 - 5
      Segment.overlaps(0, 0, true, segment) shouldBe false
      Segment.overlaps(0, 0, false, segment) shouldBe false
      //  0 - 1
      //      1 - 5
      Segment.overlaps(0, 1, true, segment) shouldBe true
      Segment.overlaps(0, 1, false, segment) shouldBe false
      //    0 - 2
      //      1 - 5
      Segment.overlaps(0, 2, true, segment) shouldBe true
      Segment.overlaps(0, 2, false, segment) shouldBe true
      //    0   - 5
      //      1 - 5
      Segment.overlaps(0, 5, true, segment) shouldBe true
      Segment.overlaps(0, 5, false, segment) shouldBe true
      //    0   -   6
      //      1 - 5
      Segment.overlaps(0, 6, true, segment) shouldBe true
      Segment.overlaps(0, 6, false, segment) shouldBe true


      //      1-2
      //      1 - 5
      Segment.overlaps(1, 2, true, segment) shouldBe true
      Segment.overlaps(1, 2, false, segment) shouldBe true
      //      1-4
      //      1 - 5
      Segment.overlaps(1, 4, true, segment) shouldBe true
      Segment.overlaps(1, 4, false, segment) shouldBe true
      //      1 - 5
      //      1 - 5
      Segment.overlaps(1, 5, true, segment) shouldBe true
      Segment.overlaps(1, 5, false, segment) shouldBe true
      //      1 -  6
      //      1 - 5
      Segment.overlaps(1, 6, true, segment) shouldBe true
      Segment.overlaps(1, 6, false, segment) shouldBe true


      //       2-4
      //      1 - 5
      Segment.overlaps(2, 4, true, segment) shouldBe true
      Segment.overlaps(2, 4, false, segment) shouldBe true
      //       2- 5
      //      1 - 5
      Segment.overlaps(2, 5, true, segment) shouldBe true
      Segment.overlaps(2, 5, false, segment) shouldBe true
      //        2 - 6
      //      1 - 5
      Segment.overlaps(2, 6, true, segment) shouldBe true
      Segment.overlaps(2, 6, false, segment) shouldBe true
      //          5 - 6
      //      1 - 5
      Segment.overlaps(5, 6, true, segment) shouldBe true
      Segment.overlaps(5, 6, false, segment) shouldBe true
      //            6 - 7
      //      1 - 5
      Segment.overlaps(6, 7, true, segment) shouldBe false
      Segment.overlaps(6, 7, false, segment) shouldBe false

      //wide outer overlap
      //    0   -   6
      //      1 - 5
      Segment.overlaps(0, 6, true, segment) shouldBe true
      Segment.overlaps(0, 6, false, segment) shouldBe true

      segment.close.assertGet
    }

    "return true for overlapping KeyValues else false for Segments if the Segment's last key-value is a Range" in {
      val segment = TestSegment(Slice(randomFixedKeyValue(1), randomRangeKeyValue(5, 10)).toTransient).assertGet


      //0 - 0
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 0, true, segment) shouldBe false
      Segment.overlaps(0, 0, false, segment) shouldBe false
      //  0 - 1
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 1, true, segment) shouldBe true
      Segment.overlaps(0, 1, false, segment) shouldBe false
      //    0 - 2
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 2, true, segment) shouldBe true
      Segment.overlaps(0, 2, false, segment) shouldBe true
      //    0 -    5
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 5, true, segment) shouldBe true
      Segment.overlaps(0, 5, false, segment) shouldBe true
      //    0   -    7
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 7, true, segment) shouldBe true
      Segment.overlaps(0, 7, false, segment) shouldBe true
      //    0     -    10
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 10, true, segment) shouldBe true
      Segment.overlaps(0, 10, false, segment) shouldBe true
      //    0      -      11
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 11, true, segment) shouldBe true
      Segment.overlaps(0, 11, false, segment) shouldBe true

      //      1 - 5
      //      1 - (5 - 10(EX))
      Segment.overlaps(1, 5, true, segment) shouldBe true
      Segment.overlaps(1, 5, false, segment) shouldBe true
      //      1 -   6
      //      1 - (5 - 10(EX))
      Segment.overlaps(1, 6, true, segment) shouldBe true
      Segment.overlaps(1, 6, false, segment) shouldBe true
      //      1 -      10
      //      1 - (5 - 10(EX))
      Segment.overlaps(1, 10, true, segment) shouldBe true
      Segment.overlaps(1, 10, false, segment) shouldBe true
      //      1 -          11
      //      1 - (5 - 10(EX))
      Segment.overlaps(1, 11, true, segment) shouldBe true
      Segment.overlaps(1, 11, false, segment) shouldBe true

      //       2-4
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 4, true, segment) shouldBe true
      Segment.overlaps(2, 4, false, segment) shouldBe true
      //       2 - 5
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 5, true, segment) shouldBe true
      Segment.overlaps(2, 5, false, segment) shouldBe true
      //       2 -   6
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 6, true, segment) shouldBe true
      Segment.overlaps(2, 6, false, segment) shouldBe true
      //       2   -    10
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 10, true, segment) shouldBe true
      Segment.overlaps(2, 10, false, segment) shouldBe true
      //       2     -    11
      //      1 - (5 - 10(EX))
      Segment.overlaps(2, 11, true, segment) shouldBe true
      Segment.overlaps(2, 11, false, segment) shouldBe true


      //          5 - 6
      //      1 - (5 - 10(EX))
      Segment.overlaps(5, 6, true, segment) shouldBe true
      Segment.overlaps(5, 6, false, segment) shouldBe true
      //          5 -  10
      //      1 - (5 - 10(EX))
      Segment.overlaps(5, 10, true, segment) shouldBe true
      Segment.overlaps(5, 10, false, segment) shouldBe true
      //          5   -   11
      //      1 - (5 - 10(EX))
      Segment.overlaps(5, 11, true, segment) shouldBe true
      Segment.overlaps(5, 11, false, segment) shouldBe true
      //            6 - 7
      //      1 - (5 - 10(EX))
      Segment.overlaps(6, 7, true, segment) shouldBe true
      Segment.overlaps(6, 7, false, segment) shouldBe true
      //             8 - 9
      //      1 - (5   -   10(EX))
      Segment.overlaps(8, 9, true, segment) shouldBe true
      Segment.overlaps(8, 9, false, segment) shouldBe true
      //             8   - 10
      //      1 - (5   -   10(EX))
      Segment.overlaps(8, 10, true, segment) shouldBe true
      Segment.overlaps(8, 10, false, segment) shouldBe true
      //               9 - 10
      //      1 - (5   -   10(EX))
      Segment.overlaps(9, 10, true, segment) shouldBe true
      Segment.overlaps(9, 10, false, segment) shouldBe true
      //               9 -   11
      //      1 - (5   -   10(EX))
      Segment.overlaps(9, 11, true, segment) shouldBe true
      Segment.overlaps(9, 11, false, segment) shouldBe true
      //                   10  -   11
      //      1 - (5   -   10(EX))
      Segment.overlaps(10, 11, true, segment) shouldBe false
      Segment.overlaps(10, 11, false, segment) shouldBe false

      //                      11  -   11
      //      1 - (5   -   10(EX))
      Segment.overlaps(11, 11, true, segment) shouldBe false
      Segment.overlaps(11, 11, false, segment) shouldBe false

      //wide outer overlap
      //    0   -   6
      //      1 - (5 - 10(EX))
      Segment.overlaps(0, 6, true, segment) shouldBe true

      segment.close.assertGet
    }
  }

  "partitionOverlapping" should {
    "partition overlapping and non-overlapping Segments" in {
      //0-1, 2-3
      //         4-5, 6-7
      var segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet)
      var segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

      //0-1,   3-4
      //         4-5, 6-7
      segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)).toTransient).assertGet)
      segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))

      //0-1,   3 - 5
      //         4-5, 6-7
      segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(5)).toTransient).assertGet)
      segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))


      //0-1,      6-8
      //      4-5,    10-20
      segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(8)).toTransient).assertGet)
      segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(20)).toTransient).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

      //0-1,             20 - 21
      //      4-5,    10-20
      segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(20), randomFixedKeyValue(21)).toTransient).assertGet)
      segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(20)).toTransient).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))

      //0-1,               21 - 22
      //      4-5,    10-20
      segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(21), randomFixedKeyValue(22)).toTransient).assertGet)
      segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(20)).toTransient).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

      //0          -          22
      //      4-5,    10-20
      segments1 = Seq(TestSegment(Slice(randomRangeKeyValue(0, 22)).toTransient).assertGet)
      segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet, TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(20)).toTransient).assertGet)
      Segment.partitionOverlapping(segments1, segments2) shouldBe(segments1, Seq.empty)
    }
  }

  "overlaps" should {
    "return true for overlapping Segments else false for Segments without Ranges" in {
      //0 1
      //    2 3
      var segment1 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).assertGet
      var segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //1 2
      //  2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //2 3
      //2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //  3 4
      //2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //    4 5
      //2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //0       10
      //   2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(10)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //   2 3
      //0       10
      segment1 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(10)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      segment1.close.assertGet
      segment2.close.assertGet
    }

    "return true for overlapping Segments if the target Segment's maxKey is a Range key" in {
      //0 1
      //    2 3
      var segment1 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).assertGet
      var segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false
      //range over range
      segment1 = TestSegment(Slice(randomRangeKeyValue(0, 1)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //1 2
      //  2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(randomRangeKeyValue(1, 2)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //1   3
      //  2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(3)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(randomRangeKeyValue(1, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //2 3
      //2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //  3 4
      //2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false
      segment1 = TestSegment(Slice(randomRangeKeyValue(3, 4)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //    4 5
      //2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false
      segment1 = TestSegment(Slice(randomRangeKeyValue(4, 5)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe false
      Segment.overlaps(segment2, segment1) shouldBe false

      //0       10
      //   2 3
      segment1 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(10)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(randomRangeKeyValue(0, 10)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      //   2 3
      //0       10
      segment1 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).assertGet
      segment2 = TestSegment(Slice(randomRangeKeyValue(0, 10)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true
      segment1 = TestSegment(Slice(randomRangeKeyValue(2, 3)).toTransient).assertGet
      Segment.overlaps(segment1, segment2) shouldBe true
      Segment.overlaps(segment2, segment1) shouldBe true

      segment1.close.assertGet
      segment2.close.assertGet
    }
  }

  "nonOverlapping and overlapping" should {
    "return non overlapping Segments" in {
      //0-1, 2-3
      //         4-5, 6-7
      var segments1 = List(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).get)
      var segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) shouldBe segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) shouldBe segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty


      //2-3, 4-5
      //     4-5, 6-7
      segments1 = List(TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).get)
      segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain only segments1.head.path
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain only segments2.last.path
      Segment.overlaps(segments1, segments2).map(_.path) should contain only segments1.last.path
      Segment.overlaps(segments2, segments1).map(_.path) should contain only segments2.head.path

      //4-5, 6-7
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).get)
      segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) shouldBe empty
      Segment.nonOverlapping(segments2, segments1).map(_.path) shouldBe empty
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe segments1.map(_.path)
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe segments2.map(_.path)

      //     6-7, 8-9
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(8), randomFixedKeyValue(9)).toTransient).get)
      segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain only segments1.last.path
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain only segments2.head.path
      Segment.overlaps(segments1, segments2).map(_.path) should contain only segments1.head.path
      Segment.overlaps(segments2, segments1).map(_.path) should contain only segments2.last.path

      //         8-9, 10-11
      //4-5, 6-7
      segments1 = List(TestSegment(Slice(randomFixedKeyValue(8), randomFixedKeyValue(9)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(11)).toTransient).get)
      segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain allElementsOf segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain allElementsOf segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty

      //1-2            10-11
      //     4-5, 6-7
      segments1 = List(TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(11)).toTransient).get)
      segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)).toTransient).get, TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7)).toTransient).get)
      Segment.nonOverlapping(segments1, segments2).map(_.path) should contain allElementsOf segments1.map(_.path)
      Segment.nonOverlapping(segments2, segments1).map(_.path) should contain allElementsOf segments2.map(_.path)
      Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
      Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty
    }
  }

  "tempMinMaxKeyValues" should {
    "return key-values with Segments min and max keys only" in {
      implicit def testTimer: TestTimer = TestTimer.Empty

      val segment1 = TestSegment(randomizedKeyValues(keyValuesCount)).assertGet
      val segment2 = TestSegment(randomizedKeyValues(keyValuesCount, startId = Some(segment1.maxKey.maxKey.read[Int] + 1))).assertGet
      val segment3 = TestSegment(randomizedKeyValues(keyValuesCount, startId = Some(segment2.maxKey.maxKey.read[Int] + 1))).assertGet
      val segment4 = TestSegment(randomizedKeyValues(keyValuesCount, startId = Some(segment3.maxKey.maxKey.read[Int] + 1))).assertGet

      val segments = Seq(segment1, segment2, segment3, segment4)

      val expectedTempKeyValues: Seq[Transient] =
        segments flatMap {
          segment =>
            segment.maxKey match {
              case MaxKey.Fixed(maxKey) =>
                Seq(Transient.put(segment.minKey), Transient.put(maxKey))
              case MaxKey.Range(fromKey, maxKey) =>
                Seq(Transient.put(segment.minKey), Transient.Range.create[FromValue, RangeValue](fromKey, maxKey, None, Value.update(maxKey)))
            }
        }

      Segment.tempMinMaxKeyValues(segments) shouldBe expectedTempKeyValues
    }
  }

  "overlapsWithBusySegments" should {
    "return true or false if input Segments overlap or do not overlap with busy Segments respectively" in {

      val targetSegments = {
        TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient) ::
          TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)).toTransient) ::
          TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8)).toTransient) ::
          TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10)).toTransient) ::
          Nil
      }.map(_.assertGet)

      //0-1
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      var inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)).toTransient)).map(_.assertGet)
      var busySegments = Seq(TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)).toTransient), TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8)).toTransient)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe false

      //     1-2
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe false

      //          3-4
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(2)).toTransient)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe true

      //               5-6
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(5), randomFixedKeyValue(6)).toTransient)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe true

      //                         9-10
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10)).toTransient)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe false

      //               5-6
      //     1-2            7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(5), randomFixedKeyValue(6)).toTransient)).map(_.assertGet)
      busySegments = {
        TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient) ::
          TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8)).toTransient) ::
          Nil
      }.map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe true

      //               5-6
      //     1-2                 9-10
      //     1-2, 3-4, ---, 7-8, 9-10
      busySegments = Seq(TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient), TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10)).toTransient)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments).assertGet shouldBe false
    }

    "return true or false if input map overlap or do not overlap with busy Segments respectively" in {

      val targetSegments = {
        TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient) ::
          TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)).toTransient) ::
          TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8)).toTransient) ::
          TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10)).toTransient) ::
          Nil
      }.map(_.assertGet)

      //0-1
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      var inputMap = TestMap(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)))
      var busySegments = Seq(TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)).toTransient), TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8)).toTransient)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe false

      //     1-2
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)))
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe false

      //          3-4
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(randomFixedKeyValue(3), randomFixedKeyValue(2)))
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe true

      //               5-6
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(randomFixedKeyValue(5), randomFixedKeyValue(6)))
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe true

      //                         9-10
      //          3-4       7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10)))
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe false

      //               5-6
      //     1-2            7-8
      //     1-2, 3-4, ---, 7-8, 9-10
      inputMap = TestMap(Slice(randomFixedKeyValue(5), randomFixedKeyValue(6)))
      busySegments = {
        TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient) ::
          TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8)).toTransient) ::
          Nil
      }.map(_.assertGet)
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe true

      //               5-6
      //     1-2                 9-10
      //     1-2, 3-4, ---, 7-8, 9-10
      busySegments = Seq(TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)).toTransient), TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10)).toTransient)).map(_.assertGet)
      Segment.overlapsWithBusySegments(inputMap, busySegments, targetSegments).assertGet shouldBe false
    }
  }

  "getAllKeyValues" should {
    "getFromHashIndex KeyValues from multiple Segments" in {
      runThis(10.times) {
        val keyValues1 = randomizedKeyValues(keyValuesCount)
        val keyValues2 = randomizedKeyValues(keyValuesCount)
        val keyValues3 = randomizedKeyValues(keyValuesCount)

        val segment1 = TestSegment(keyValues1).assertGet
        val segment2 = TestSegment(keyValues2).assertGet
        val segment3 = TestSegment(keyValues3).assertGet

        val all = Slice((keyValues1 ++ keyValues2 ++ keyValues3).toArray).updateStats

        val (slice, deadline) =
          SegmentWriter.write(
            keyValues = all,
            createdInLevel = 0,
            maxProbe = TestData.maxProbe,
            falsePositiveRate = TestData.falsePositiveRate
          ).assertGet.flatten

        deadline shouldBe nearestDeadline(all)

        val readKeyValues = Segment.getAllKeyValues(Seq(segment1, segment2, segment3)).assertGet

        readKeyValues shouldBe all
      }
    }

    "fail read if reading any one Segment fails for persistent Segments" in {
      val keyValues1 = randomizedKeyValues(keyValuesCount)
      val keyValues2 = randomizedKeyValues(keyValuesCount)
      val keyValues3 = randomizedKeyValues(keyValuesCount)

      val segment1 = TestSegment(keyValues1).assertGet
      val segment2 = TestSegment(keyValues2).assertGet
      val segment3 = TestSegment(keyValues3).assertGet

      segment3.delete.assertGet //delete a segment so that there is a failure.

      Segment.getAllKeyValues(Seq(segment1, segment2, segment3)).failed.assertGet.exception shouldBe a[NoSuchFileException]
    }

    "fail read if reading any one Segment file is corrupted" in {
      if (persistent) {
        runThis(10.times) {
          val keyValues1 = randomizedKeyValues(keyValuesCount)
          val keyValues2 = randomizedKeyValues(keyValuesCount)
          val keyValues3 = randomizedKeyValues(keyValuesCount)

          val segment1 = TestSegment(keyValues1).assertGet
          val segment2 = TestSegment(keyValues2).assertGet
          val segment3 = TestSegment(keyValues3).assertGet

          val bytes = Files.readAllBytes(segment2.path)

          Files.write(segment2.path, bytes.drop(1))
          Segment.getAllKeyValues(Seq(segment1, segment2, segment3)).failed.assertGet.exception shouldBe a[SegmentCorruptionException]

          Files.write(segment2.path, bytes.dropRight(1))
          Segment.getAllKeyValues(Seq(segment2)).failed.assertGet.exception shouldBe a[SegmentCorruptionException]

          Files.write(segment2.path, bytes.drop(10))
          Segment.getAllKeyValues(Seq(segment1, segment2, segment3)).failed.assertGet.exception shouldBe a[SegmentCorruptionException]

          Files.write(segment2.path, bytes.dropRight(1))
          Segment.getAllKeyValues(Seq(segment1, segment2, segment3)).failed.assertGet.exception shouldBe a[SegmentCorruptionException]
        }
      } else {
        //memory files do not require this test
      }
    }
  }

  "getAll" should {
    "read full index" in {
      runThis(10.times) {
        //ensure groups are not added because ones read their values are populated in memory
        val keyValues = randomizedKeyValues(keyValuesCount, addRandomGroups = false)
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
              case persistent: Persistent if !persistent.isInstanceOf[Persistent.Remove] =>
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
  }

  "getNearestDeadline" should {
    "return earliest deadline from key-values" in {
      runThis(10.times) {
        val deadlines = (1 to 8).map(_.seconds.fromNow)

        val shuffledDeadlines = Random.shuffle(deadlines)

        //populated deadlines from shuffled deadlines.
        val keyValues =
          Slice(
            Memory.remove(1, shuffledDeadlines(0)),
            Memory.put(2, 1, shuffledDeadlines(1)),
            Memory.update(3, 10, shuffledDeadlines(2)),
            Memory.Range(4, 10, None, Value.remove(shuffledDeadlines(3))),
            Memory.Range(5, 10, Value.put(10, shuffledDeadlines(4)), Value.remove(shuffledDeadlines(5))),
            Memory.Range(6, 10, Value.put(10, shuffledDeadlines(6)), Value.update(None, Some(shuffledDeadlines(7))))
          )

        Segment.getNearestDeadline(keyValues).assertGet shouldBe deadlines.head
      }
    }
  }

  "getNearestDeadlineSegment" should {
    "return None deadline if non of the key-values in the Segments contains deadline" in {

      runThis(100.times) {
        val segment1 = TestSegment(randomizedKeyValues(keyValuesCount, addRandomPutDeadlines = false, addRandomRemoveDeadlines = false, addRandomUpdateDeadlines = false)).assertGet
        val segment2 = TestSegment(randomizedKeyValues(keyValuesCount, addRandomPutDeadlines = false, addRandomRemoveDeadlines = false, addRandomUpdateDeadlines = false)).assertGet

        Segment.getNearestDeadlineSegment(segment1, segment2) shouldBe empty

        segment1.close.assertGet
        segment2.close.assertGet
      }
    }

    "return deadline if one of the Segments contains deadline" in {
      runThisParallel(10.times) {
        val keyValues = randomizedKeyValues(keyValuesCount, addRandomPutDeadlines = false, addRandomRemoveDeadlines = false, addRandomUpdateDeadlines = false).toMemory

        //only a single key-value with a deadline.
        val deadline = 100.seconds.fromNow
        val keyValueWithDeadline =
          eitherOne(
            left = Memory.remove(keyValues.last.key.readInt() + 10000, deadline),
            mid =
              eitherOne(
                Memory.put(keyValues.last.key.readInt() + 10000, randomStringOption, deadline),
                randomRangeKeyValueForDeadline(keyValues.last.key.readInt() + 10000, keyValues.last.key.readInt() + 20000, deadline = deadline)
              ),
            right =
              eitherOne(
                Memory.update(keyValues.last.key.readInt() + 10000, randomStringOption, deadline),
                Memory.PendingApply(keyValues.last.key.readInt() + 10000, randomAppliesWithDeadline(deadline = deadline))
              )
          )

        val keyValuesWithDeadline = (keyValues ++ Seq(keyValueWithDeadline)).toTransient
        val keyValuesNoDeadline = randomizedKeyValues(keyValuesCount, addRandomPutDeadlines = false, addRandomRemoveDeadlines = false, addRandomUpdateDeadlines = false)

        val segment1 = TestSegment(keyValuesWithDeadline).assertGet
        val segment2 = TestSegment(keyValuesNoDeadline).assertGet

        Segment.getNearestDeadlineSegment(segment1, segment2).flatMap(_.nearestExpiryDeadline) should contain(deadline)
        Segment.getNearestDeadlineSegment(segment2, segment1).flatMap(_.nearestExpiryDeadline) should contain(deadline)
        Segment.getNearestDeadlineSegment(Random.shuffle(Seq(segment2, segment1, segment2, segment2))).flatMap(_.nearestExpiryDeadline) should contain(deadline)
        Segment.getNearestDeadlineSegment(Random.shuffle(Seq(segment1, segment2, segment2, segment2))).flatMap(_.nearestExpiryDeadline) should contain(deadline)
      }
    }

    "return deadline" in {
      runThisParallel(10.times) {
        val keyValues1 = randomizedKeyValues(1000)
        val keyValues2 = randomizedKeyValues(1000)

        val segment1 = TestSegment(keyValues1).assertGet
        val segment2 = TestSegment(keyValues2).assertGet

        val deadline = nearestDeadline(keyValues1 ++ keyValues2)

        Segment.getNearestDeadlineSegment(segment1, segment2).flatMap(_.nearestExpiryDeadline) shouldBe deadline
        Segment.getNearestDeadlineSegment(segment2, segment1).flatMap(_.nearestExpiryDeadline) shouldBe deadline
        Segment.getNearestDeadlineSegment(segment1 :: segment2 :: Nil).flatMap(_.nearestExpiryDeadline) shouldBe deadline
      }
    }
  }
}
