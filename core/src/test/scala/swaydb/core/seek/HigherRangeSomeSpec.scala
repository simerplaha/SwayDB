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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.seek

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import swaydb.data.io.IO
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.IOAssert._
import swaydb.core.data._

import swaydb.core.{TestData, TestTimeGenerator}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class HigherRangeSomeSpec extends WordSpec with Matchers with MockFactory {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = TestData.functionStore
  implicit val timeGenerator = TestTimeGenerator.Empty

  "return Some" when {

    //0
    //0  - 3
    // 1,2,3,4
    "1" in {
      //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperRange = randomRangeKeyValue(0, 3, rangeValue = randomRemoveOrUpdateOrFunctionRemoveValue())
        val toKeyGet = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        inSequence {
          current.higher _ expects (0: Slice[Byte]) returning IO(Some(upperRange))
          if (upperRange.rangeValue.isInstanceOf[Value.Function]) {
            next.higher _ expects (0: Slice[Byte]) returning IO(Some(randomPutKeyValue(1, deadline = randomDeadlineOption(false)))).asAsync
            next.higher _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(2, deadline = randomDeadlineOption(false)))).asAsync
            next.higher _ expects (2: Slice[Byte]) returning IO(Some(randomPutKeyValue(3, deadline = randomDeadlineOption(false)))).asAsync
          }
          current.get _ expects (3: Slice[Byte]) returning IO(Some(toKeyGet)).asAsync
        }
        Higher(0: Slice[Byte]).assertGet shouldBe toKeyGet
      }
    }

    // 1
    //0  - 3
    // 1,2,3,4
    "2" in {
      //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperRange = randomRangeKeyValue(0, 3, rangeValue = randomRemoveOrUpdateOrFunctionRemoveValue())
        val toKeyGet = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        inSequence {
          current.higher _ expects (1: Slice[Byte]) returning IO(Some(upperRange))
          if (upperRange.rangeValue.isInstanceOf[Value.Function]) {
            next.higher _ expects (1: Slice[Byte]) returning IO(Some(randomPutKeyValue(2, deadline = randomDeadlineOption(false)))).asAsync
            next.higher _ expects (2: Slice[Byte]) returning IO(Some(randomPutKeyValue(3, deadline = randomDeadlineOption(false)))).asAsync
          }
          current.get _ expects (3: Slice[Byte]) returning IO(Some(toKeyGet)).asAsync
        }
        Higher(1: Slice[Byte]).assertGet shouldBe toKeyGet
      }
    }

    //   2
    //0  - 3
    // 1,2,3,4
    "3" in {
      //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val upperRange = randomRangeKeyValue(0, 3, rangeValue = randomRemoveOrUpdateOrFunctionRemoveValue())
        val toKeyGet = randomPutKeyValue(1, deadline = randomDeadlineOption(false))

        inSequence {
          current.higher _ expects (2: Slice[Byte]) returning IO(Some(upperRange))
          if (upperRange.rangeValue.isInstanceOf[Value.Function]) {
            next.higher _ expects (2: Slice[Byte]) returning IO(Some(randomPutKeyValue(3, deadline = randomDeadlineOption(false)))).asAsync
          }
          current.get _ expects (3: Slice[Byte]) returning IO(Some(toKeyGet)).asAsync
        }
        Higher(2: Slice[Byte]).assertGet shouldBe toKeyGet
      }
    }

    //     3
    //0  - 3
    // 1,2,3,4
    "4" in {
      //in this test lower level is read for upper Level's higher toKey and the input key is not read since it's removed.
      runThis(100.times) {

        implicit val current = mock[CurrentWalker]
        implicit val next = mock[NextWalker]

        val result = randomPutKeyValue(4, deadline = randomDeadlineOption(false))

        inSequence {
          current.higher _ expects (3: Slice[Byte]) returning IO.none
          next.higher _ expects (3: Slice[Byte]) returning IO(Some(result)).asAsync
        }
        Higher(3: Slice[Byte]).assertGet shouldBe result
      }
    }
  }
}
