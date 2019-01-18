package swaydb.core.util

import org.scalatest.{Matchers, WordSpec}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.core.TryAssert._

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

}
