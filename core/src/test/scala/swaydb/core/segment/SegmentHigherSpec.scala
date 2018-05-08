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

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.TestBase
import swaydb.core.data.Transient.Remove
import swaydb.core.data._
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class SegmentHigherSpec1 extends SegmentHigherSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentHigherSpec2 extends SegmentHigherSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentHigherSpec3 extends SegmentHigherSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class SegmentHigherSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val ordering = KeyOrder.default
  val keyValuesCount = 100

  "Segment.higher" should {
    "get the higher key from the segment that has only 1 Remove key" in {
      assertOnSegment(
        keyValues = Slice(Memory.Remove(1)),
        assertion =
          segment => {
            segment.higher(0).assertGet shouldBe Memory.Remove(1)
            segment.higher(1).assertGetOpt shouldBe empty
            segment.higher(2).assertGetOpt shouldBe empty
          }
      )
    }

    "get the higher key from the segment that has only 1 Put key" in {
      assertOnSegment(
        keyValues = Slice(Memory.Put(1, 10)),
        assertion =
          segment => {
            segment.higher(0).assertGet shouldBe Memory.Put(1, 10)
            segment.higher(1).assertGetOpt shouldBe empty
            segment.higher(2).assertGetOpt shouldBe empty
          }
      )
    }

    "get the higher key from the segment that has only 1 Range key" in {
      assertOnSegment(
        keyValues = Slice(Memory.Range(1, 10, None, Value.Update("ten"))),
        assertion =
          segment => {
            (0 to 9) foreach {
              i =>
                segment.higher(i).assertGet shouldBe Memory.Range(1, 10, None, Value.Update("ten"))
            }

            (10 to 15) foreach {
              i =>
                segment.higher(i).assertGetOpt shouldBe empty
            }
          }
      )
    }

    "get the higher from the segment when there are no Range key-values" in {
      //1, 2, 3

      assertOnSegment(
        keyValues = Slice(Memory.Put(1, 1), Memory.Remove(2), Memory.Put(3, 3)),
        assertion =
          segment => {
            segment.higher(0).assertGet shouldBe Memory.Put(1, 1)
            segment.higher(1).assertGet shouldBe Memory.Remove(2)
            segment.higher(2).assertGet shouldBe Memory.Put(3, 3)
            (4 to 10) foreach {
              i =>
                segment.higher(i).assertGetOpt shouldBe empty
            }
          }
      )
    }

    "get the higher from the segment when there are Range key-values" in {
      //1, (2 - 5), 10, (11 - 20), (20 - 30)
      assertOnSegment(
        keyValues = Slice(
          Memory.Put(1, 1),
          Memory.Range(2, 5, None, Value.Remove(None)),
          Memory.Remove(10),
          Memory.Range(11, 20, None, Value.Update(11)),
          Memory.Range(20, 30, None, Value.Update(20))
        ),
        assertionWithKeyValues =
          (keyValues, segment) => {
            //0
            //  1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(0).assertGet shouldBe keyValues(0)
            //1
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(1).assertGet shouldBe keyValues(1)
            //    2
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(2).assertGet shouldBe keyValues(1)
            //     3
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(3).assertGet shouldBe keyValues(1)
            //       4
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(4).assertGet shouldBe keyValues(1)
            //        5
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(5).assertGet shouldBe keyValues(2)
            //          6
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(6).assertGet shouldBe keyValues(2)
            //            10
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(10).assertGet shouldBe keyValues(3)
            //                 11
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(11).assertGet shouldBe keyValues(3)
            //                   12
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(12).assertGet shouldBe keyValues(3)
            //                    19
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(19).assertGet shouldBe keyValues(3)
            //                      20
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(20).assertGet shouldBe keyValues(4)
            //                              21
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(21).assertGet shouldBe keyValues(4)
            //                                29
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(29).assertGet shouldBe keyValues(4)
            //                                 30
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(30).assertGetOpt shouldBe empty
            //                                    31
            //1, (2 - 5), 10, (11 - 20), (20 - 30)
            segment.higher(31).assertGetOpt shouldBe empty
          }
      )
    }

    "get the higher key from the segment that has many keys" in {
      assertOnSegment(
        keyValues = randomIntKeyValuesMemory(keyValuesCount, addRandomRemoves = true, addRandomRanges = true),
        assertionWithKeyValues = assertHigher(_, _)
      )
    }
  }
}
