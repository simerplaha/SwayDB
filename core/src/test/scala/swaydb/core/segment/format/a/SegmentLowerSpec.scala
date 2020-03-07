/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a

import org.scalatest.OptionValues._
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.segment.ThreadReadState
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

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

sealed trait SegmentLowerSpec extends TestBase with ScalaFutures with PrivateMethodTester {

  implicit val keyOrder = KeyOrder.default

  def keyValuesCount: Int

  "Segment.lower" should {
    "value the lower key from the segment that has only 1 fixed key-value" in {
      assertSegment(
        keyValues =
          Slice(randomFixedKeyValue(1)),

        assert =
          (keyValues, segment) => {
            val readState = ThreadReadState.random
            segment.lower(0, readState).toOptional shouldBe empty
            segment.lower(1, readState).toOptional shouldBe empty
            segment.lower(2, readState).toOptional.value shouldBe keyValues.head
          }
      )
    }

    "value the lower from the segment when there are no Range key-values" in {
      //1, 2, 3
      assertSegment(
        keyValues =
          Slice(randomFixedKeyValue(1), randomFixedKeyValue(2), randomFixedKeyValue(3)),

        assert =
          (keyValues, segment) => {
            val readState = ThreadReadState.random
            
            segment.lower(0, readState).toOptional shouldBe empty //smallest key in this segment is 1
            segment.lower(1, readState).toOptional shouldBe empty

            segment.lower(2, readState).toOptional.value shouldBe keyValues.head
            segment.lower(3, readState).toOptional.value shouldBe keyValues(1)
            (4 to 10) foreach {
              i =>
                segment.lower(i, readState).toOptional.value shouldBe keyValues(2)
            }
          }
      )
    }

    "value the lower from the segment when there are Range key-values" in {
      //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
      runThis(1000.times) {
        assertSegment(
          keyValues = Slice(
            randomFixedKeyValue(1),
            randomRangeKeyValue(2, 5),
            randomFixedKeyValue(10),
            randomRangeKeyValue(11, 20),
            randomRangeKeyValue(20, 30)
          ),

          assert =
            (keyValues, segment) => {
              val readState = ThreadReadState.random
              
              //0
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(0, readState).toOptional shouldBe empty
              //1
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(1, readState).toOptional shouldBe empty
              //    2
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(2, readState).getUnsafe shouldBe keyValues(0)
              //     3
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(3, readState).getUnsafe shouldBe keyValues(1)
              //       4
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(4, readState).getUnsafe shouldBe keyValues(1)
              //        5
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(5, readState).getUnsafe shouldBe keyValues(1)
              //          6
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(6, readState).getUnsafe shouldBe keyValues(1)
              //            10
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(10, readState).getUnsafe shouldBe keyValues(1)
              //                 11
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(11, readState).getUnsafe shouldBe keyValues(2)
              //                   12
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(12, readState).getUnsafe shouldBe keyValues(3)
              //                    19
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(19, readState).getUnsafe shouldBe keyValues(3)
              //                      20
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(20, readState).getUnsafe shouldBe keyValues(3)
              //                              21
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(21, readState).getUnsafe shouldBe keyValues(4)
              //                                29
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(29, readState).getUnsafe shouldBe keyValues(4)
              //                                 30
              //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
              segment.lower(30, readState).getUnsafe shouldBe keyValues(4)
            }
        )
      }
    }

    "random" in {
      assertSegment(
        keyValues = randomizedKeyValues(keyValuesCount, addUpdates = true),
        assert = assertLower(_, _)
      )
    }
  }
}
