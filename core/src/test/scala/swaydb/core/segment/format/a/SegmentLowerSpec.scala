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

package swaydb.core.segment.format.a

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.core.CommonAssertions._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.TryAssert._
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.RunThis._

//@formatter:off
class SegmentLowerSpec0 extends SegmentLowerSpec {
  val keyValuesCount = 100
}

class SegmentLowerSpec1 extends SegmentLowerSpec {
  val keyValuesCount = 100

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentLowerSpec2 extends SegmentLowerSpec {
  val keyValuesCount = 100

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentLowerSpec3 extends SegmentLowerSpec {
  val keyValuesCount = 1000

  override def inMemoryStorage = true
}
//@formatter:on

sealed trait SegmentLowerSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val keyOrder = KeyOrder.default

  def keyValuesCount: Int

  implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] =
    randomGroupingStrategyOption(keyValuesCount)

  "Segment.lower" should {
    "get the lower key from the segment that has only 1 fixed key-value" in {
      assertOnSegment(
        keyValues = Slice(randomFixedKeyValue(1)),
        assert =
          (keyValues, segment) => {
            segment.lower(0).assertGetOpt shouldBe empty
            segment.lower(1).assertGetOpt shouldBe empty
            segment.lower(2).assertGet shouldBe keyValues.head
          }
      )
    }

    "get the lower from the segment when there are no Range key-values" in {
      //1, 2, 3
      assertOnSegment(
        keyValues = Slice(randomFixedKeyValue(1), randomFixedKeyValue(2), randomFixedKeyValue(3)),
        assert =
          (keyValues, segment) => {
            segment.lower(0).assertGetOpt shouldBe empty //smallest key in this segment is 1
            segment.lower(1).assertGetOpt shouldBe empty

            segment.lower(2).assertGet shouldBe keyValues.head
            segment.lower(3).assertGet shouldBe keyValues(1)
            (4 to 10) foreach {
              i =>
                segment.lower(i).assertGet shouldBe keyValues(2)
            }
          }
      )
    }

    "get the lower from the segment when there are Range key-values" in {
      //1, (2 - 5), 10, (11 - 20), (20 - 30)
      runThis(10.times) {
        assertOnSegment(
          keyValues = Slice(
            randomFixedKeyValue(1),
            randomRangeKeyValue(2, 5),
            randomFixedKeyValue(10),
            randomRangeKeyValue(11, 20),
            randomRangeKeyValue(20, 30)
          ),
          assert =
            (keyValues, segment) => {
              //0
              //  1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(0).assertGetOpt shouldBe empty
              //1
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(1).assertGetOpt shouldBe empty
              //    2
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(2).assertGet shouldBe keyValues(0)
              //     3
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(3).assertGet shouldBe keyValues(1)
              //       4
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(4).assertGet shouldBe keyValues(1)
              //        5
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(5).assertGet shouldBe keyValues(1)
              //          6
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(6).assertGet shouldBe keyValues(1)
              //            10
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(10).assertGet shouldBe keyValues(1)
              //                 11
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(11).assertGet shouldBe keyValues(2)
              //                   12
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(12).assertGet shouldBe keyValues(3)
              //                    19
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(19).assertGet shouldBe keyValues(3)
              //                      20
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(20).assertGet shouldBe keyValues(3)
              //                              21
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(21).assertGet shouldBe keyValues(4)
              //                                29
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(29).assertGet shouldBe keyValues(4)
              //                                 30
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(30).assertGet shouldBe keyValues(4)
              //                                    31
              //1, (2 - 5), 10, (11 - 20), (20 - 30)
              segment.lower(31).assertGet shouldBe keyValues(4)
            }
        )
      }
    }

    "random" in {
      assertOnSegment(
        keyValues = randomizedKeyValues(keyValuesCount),
        assert = assertLower(_, _)
      )
    }
  }
}
