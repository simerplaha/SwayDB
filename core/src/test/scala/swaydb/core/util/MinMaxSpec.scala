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

package swaydb.core.util

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.IOAssert._
import swaydb.core.data.{Time, Value}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.TestData._
import swaydb.core.RunThis._

import scala.util.Random

class MinMaxSpec extends WordSpec with Matchers {

  "min" should {
    "return minimum of two" in {
      MinMax.min(Some(1: Slice[Byte]), Some(2: Slice[Byte]))(KeyOrder.default) should contain(1: Slice[Byte])
      MinMax.min(Some(2: Slice[Byte]), Some(1: Slice[Byte]))(KeyOrder.default) should contain(1: Slice[Byte])

      MinMax.min(Some(Int.MinValue: Slice[Byte]), Some(Int.MaxValue: Slice[Byte]))(Ordering.Int.on[Slice[Byte]](_.readInt())) should contain(Int.MinValue: Slice[Byte])
      MinMax.min(Some(Int.MaxValue: Slice[Byte]), Some(Int.MinValue: Slice[Byte]))(Ordering.Int.on[Slice[Byte]](_.readInt())) should contain(Int.MinValue: Slice[Byte])

      MinMax.min(Some(Long.MinValue: Slice[Byte]), Some(Long.MaxValue: Slice[Byte]))(Ordering.Long.on[Slice[Byte]](_.readLong())) should contain(Long.MinValue: Slice[Byte])
      MinMax.min(Some(Long.MaxValue: Slice[Byte]), Some(Long.MinValue: Slice[Byte]))(Ordering.Long.on[Slice[Byte]](_.readLong())) should contain(Long.MinValue: Slice[Byte])

      MinMax.min(Some(Int.MinValue), Some(Int.MaxValue))(Ordering.Int) should contain(Int.MinValue)
      MinMax.min(Some(Int.MaxValue), Some(Int.MinValue))(Ordering.Int) should contain(Int.MinValue)
    }

    "return left if both are equal" in {
      val left = (Slice.writeInt(1) ++ Slice.writeInt(2)).dropRight(ByteSizeOf.int)
      left.underlyingArraySize should be > 4

      val min = MinMax.min(Some(left), Some(1: Slice[Byte]))(KeyOrder.default).assertGet

      min shouldBe left
      min.underlyingArraySize shouldBe left.underlyingArraySize
    }

    "return left if right is none" in {
      val left = (Slice.writeInt(1) ++ Slice.writeInt(2)).dropRight(ByteSizeOf.int)
      left.underlyingArraySize should be > 4

      val min = MinMax.min(Some(left), None)(KeyOrder.default).assertGet

      min shouldBe left
      min.underlyingArraySize shouldBe left.underlyingArraySize
    }

    "return right if left is none" in {
      val right = (Slice.writeInt(1) ++ Slice.writeInt(2)).dropRight(ByteSizeOf.int)
      right.underlyingArraySize should be > 4

      val min = MinMax.min(None, Some(right))(KeyOrder.default).assertGet

      min shouldBe right
      min.underlyingArraySize shouldBe right.underlyingArraySize
    }

    "return None is both are none" in {
      MinMax.min(None, None)(KeyOrder.default) shouldBe empty
    }
  }

  "max" should {
    "return maximum of two" in {
      MinMax.max(Some(1L: Slice[Byte]), Some(2L: Slice[Byte]))(KeyOrder.default) should contain(2L: Slice[Byte])
      MinMax.max(Some(2L: Slice[Byte]), Some(1L: Slice[Byte]))(KeyOrder.default) should contain(2L: Slice[Byte])

      MinMax.max(Some(Long.MinValue: Slice[Byte]), Some(Long.MaxValue: Slice[Byte]))(Ordering.Long.on[Slice[Byte]](_.readLong())) should contain(Long.MaxValue: Slice[Byte])
      MinMax.max(Some(Long.MaxValue: Slice[Byte]), Some(Long.MinValue: Slice[Byte]))(Ordering.Long.on[Slice[Byte]](_.readLong())) should contain(Long.MaxValue: Slice[Byte])

      MinMax.max(Some(Long.MinValue: Slice[Byte]), Some(Long.MaxValue: Slice[Byte]))(Ordering.Long.on[Slice[Byte]](_.readLong())) should contain(Long.MaxValue: Slice[Byte])
      MinMax.max(Some(Long.MaxValue: Slice[Byte]), Some(Long.MinValue: Slice[Byte]))(Ordering.Long.on[Slice[Byte]](_.readLong())) should contain(Long.MaxValue: Slice[Byte])

      MinMax.max(Some(Long.MinValue), Some(Long.MaxValue))(Ordering.Long) should contain(Long.MaxValue)
      MinMax.max(Some(Long.MaxValue), Some(Long.MinValue))(Ordering.Long) should contain(Long.MaxValue)
    }

    "return left if both are equal" in {
      val left = (Slice.writeInt(1) ++ Slice.writeInt(2)).dropRight(ByteSizeOf.int)
      left.underlyingArraySize should be > 4

      val max = MinMax.max(Some(left), Some(1: Slice[Byte]))(KeyOrder.default).assertGet

      max shouldBe left
      max.underlyingArraySize shouldBe left.underlyingArraySize
    }

    "return left if right is None" in {
      val left = (Slice.writeInt(1) ++ Slice.writeInt(2)).dropRight(ByteSizeOf.int)
      left.underlyingArraySize should be > 4

      val max = MinMax.max(Some(left), None)(KeyOrder.default).assertGet

      max shouldBe left
      max.underlyingArraySize shouldBe left.underlyingArraySize
    }

    "return right if left is None" in {
      val right = (Slice.writeInt(1) ++ Slice.writeInt(2)).dropRight(ByteSizeOf.int)
      right.underlyingArraySize should be > 4

      val max = MinMax.max(None, Some(right))(KeyOrder.default).assertGet

      max shouldBe right
      max.underlyingArraySize shouldBe right.underlyingArraySize
    }

    "return None is both are none" in {
      MinMax.max(None, None)(KeyOrder.default) shouldBe empty
    }
  }

  "contains" should {
    "check belongs" in {
      //0
      //  1
      MinMax.contains(0, MinMax(1, None)) shouldBe false
      //0
      //  1-1
      MinMax.contains(0, MinMax(1, Some(1))) shouldBe false
      //0
      //  1-5
      MinMax.contains(0, MinMax(1, Some(5))) shouldBe false
      //  1
      //  1
      MinMax.contains(1, MinMax(1, None)) shouldBe true
      //  1
      //  1-1
      MinMax.contains(1, MinMax(1, Some(1))) shouldBe true
      //     2
      //  1-1
      MinMax.contains(2, MinMax(1, Some(1))) shouldBe false
      //  1
      //  1 - 5
      (1 to 5) foreach {
        i =>
          MinMax.contains(i, MinMax(1, Some(5))) shouldBe true
      }
      //        6
      //  1 - 5
      MinMax.contains(6, MinMax(1, Some(5))) shouldBe false
    }
  }

  "minMaxFunction" should {
    "return min and max functionIds" in {
      implicit val ordering = KeyOrder.default

      //0
      //None
      MinMax.minMaxFunction(
        function = Some(Value.Function(0, Time.empty): Value),
        current = None
      ) should contain(MinMax(0: Slice[Byte], None))

      //0
      //  1
      MinMax.minMaxFunction(
        function = Some(Value.Function(0, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], None))
      ) should contain(MinMax(0: Slice[Byte], Some(1: Slice[Byte])))

      //0
      //  1 - 1
      MinMax.minMaxFunction(
        function = Some(Value.Function(0, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], Some(1: Slice[Byte])))
      ) should contain(MinMax(0: Slice[Byte], Some(1: Slice[Byte])))

      //0
      //  1 - 3
      MinMax.minMaxFunction(
        function = Some(Value.Function(0, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))
      ) should contain(MinMax(0: Slice[Byte], Some(3: Slice[Byte])))

      //  1
      //  None
      MinMax.minMaxFunction(
        function = Some(Value.Function(1, Time.empty): Value),
        current = None
      ) should contain(MinMax(1: Slice[Byte], None))

      //  1
      //  1
      MinMax.minMaxFunction(
        function = Some(Value.Function(1, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], None))
      ) should contain(MinMax(1: Slice[Byte], None))

      //  1
      //  1 - 1
      MinMax.minMaxFunction(
        function = Some(Value.Function(1, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], Some(1: Slice[Byte])))
      ) should contain(MinMax(1: Slice[Byte], Some(1: Slice[Byte])))

      //  1
      //  1 - 3
      MinMax.minMaxFunction(
        function = Some(Value.Function(1, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))
      ) should contain(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))

      //    2
      //  1 - 3
      MinMax.minMaxFunction(
        function = Some(Value.Function(2, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))
      ) should contain(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))

      //      3
      //  1 - 3
      MinMax.minMaxFunction(
        function = Some(Value.Function(3, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))
      ) should contain(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))

      //        4
      //  1 - 3
      MinMax.minMaxFunction(
        function = Some(Value.Function(4, Time.empty): Value),
        current = Some(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))
      ) should contain(MinMax(1: Slice[Byte], Some(4: Slice[Byte])))
    }
  }

  "minMax on values" in {
    implicit val ordering = KeyOrder.default
    runThis(10.times) {
      val values =
        Random.shuffle(
          Slice(
            Value.Function(1, Time.empty),
            Value.Function(2, Time.empty),
            Value.Update(Some(0), randomDeadlineOption(), Time.empty),
            Value.Remove(randomDeadlineOption(), Time.empty),
            Value.PendingApply(
              Random.shuffle(
                Slice(
                  Value.Function(3, Time.empty),
                  Value.Update(Some(100), randomDeadlineOption(), Time.empty),
                  Value.Remove(randomDeadlineOption(), Time.empty),
                )
              )
            )
          )
        )

      MinMax.minMaxFunction(values, None) should contain(MinMax(1: Slice[Byte], Some(3: Slice[Byte])))
    }
  }

  "minMax on MinMax" should {
    "set min and max" in {
      implicit val ordering = Ordering.Int
      //None
      //None
      MinMax.minMax(
        left = None,
        right = None
      ) shouldBe empty

      //None
      //  1
      MinMax.minMax(
        left = None,
        right = Some(MinMax(1, None))
      ) shouldBe Some(MinMax(1, None))

      //None
      //  1 - 3
      MinMax.minMax(
        left = None,
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(3)))

      //0
      //   1 - 3
      MinMax.minMax(
        left = Some(MinMax(0, None)),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(0, Some(3)))

      //0 - 1
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(0, Some(1))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(0, Some(3)))

      //0 -   2
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(0, Some(2))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(0, Some(3)))

      //0 -     3
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(0, Some(3))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(0, Some(3)))

      //0 -       4
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(0, Some(4))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(0, Some(4)))

      //    1-1
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(1, Some(1))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(3)))

      //    1-2
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(1, Some(2))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(3)))

      //    1 - 3
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(1, Some(3))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(3)))

      //      2
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(2, None)),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(3)))

      //      2-2
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(2, Some(2))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(3)))


      //        3
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(3, None)),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(3)))

      //        3 - 4
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(3, Some(4))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(4)))

      //          4 - 4
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(4, Some(4))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(4)))

      //          4 - 5
      //    1 - 3
      MinMax.minMax(
        left = Some(MinMax(4, Some(5))),
        right = Some(MinMax(1, Some(3)))
      ) shouldBe Some(MinMax(1, Some(5)))
    }
  }
}
