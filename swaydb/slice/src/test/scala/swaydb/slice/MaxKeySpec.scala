package swaydb.slice

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.slice.order.KeyOrder

class MaxKeySpec extends AnyWordSpec {

  implicit val keyOrder = KeyOrder.default

  "within" when {
    implicit def toSlice(int: Int): Slice[Byte] = Slice.writeInt(int)

    implicit def toSliceMaxKey(int: MaxKey.Fixed[Int]): MaxKey[Slice[Byte]] = MaxKey.Fixed(Slice.writeInt(int.maxKey))

    implicit def toSliceMaxKeyRange(int: MaxKey.Range[Int]): MaxKey[Slice[Byte]] = MaxKey.Range(Slice.writeInt(int.fromKey), Slice.writeInt(int.maxKey))

    "max key is Fixed" in {
      //0
      //  1 - 10
      MaxKey.within(key = 0, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe false
      //  1
      //  1 - 10
      MaxKey.within(key = 1, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe true
      //    5
      //  1 - 10
      MaxKey.within(key = 5, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe true
      //      10
      //  1 - 10
      MaxKey.within(key = 10, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe true
      //        11
      //  1 - 10
      MaxKey.within(key = 11, minKey = 1, maxKey = MaxKey.Fixed(10)) shouldBe false
    }

    "max key is Range" in {
      //0
      //  1 - (10 - 20)
      MaxKey.within(key = 0, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe false
      //  1
      //  1 - (10 - 20)
      MaxKey.within(key = 1, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //    5
      //  1 - (10 - 20)
      MaxKey.within(key = 5, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //      10
      //  1 - (10 - 20)
      MaxKey.within(key = 10, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //        11
      //  1 - (10 - 20)
      MaxKey.within(key = 11, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //           19
      //  1 - (10 - 20)
      MaxKey.within(key = 19, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe true
      //            20
      //  1 - (10 - 20)
      MaxKey.within(key = 20, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe false
      //                21
      //  1 - (10 - 20)
      MaxKey.within(key = 21, minKey = 1, maxKey = MaxKey.Range(10, 20)) shouldBe false
    }

    "fixed on maxKey" in {
      //0
      //        10   -   20
      MaxKey.within(MaxKey.Fixed(0), MaxKey.Range(10, 20)) shouldBe false
      //        10
      //        10   -   20
      MaxKey.within(MaxKey.Fixed(10), MaxKey.Range(10, 20)) shouldBe true
      //           11
      //        10   -   20
      MaxKey.within(MaxKey.Fixed(11), MaxKey.Range(10, 20)) shouldBe true
      //              19
      //        10   -   20
      MaxKey.within(MaxKey.Fixed(19), MaxKey.Range(10, 20)) shouldBe true
      //                 20
      //        10   -   20
      MaxKey.within(MaxKey.Fixed(20), MaxKey.Range(10, 20)) shouldBe false
      //                    21
      //        10   -   20
      MaxKey.within(MaxKey.Fixed(21), MaxKey.Range(10, 20)) shouldBe false

    }

    "maxKey on maxKey" in {
      //0   -   10
      //        10   -   20
      MaxKey.within(MaxKey.Range(0, 10), MaxKey.Range(10, 20)) shouldBe false
      //0    -    11
      //        10   -   20
      MaxKey.within(MaxKey.Range(0, 11), MaxKey.Range(10, 20)) shouldBe false
      //0           -    20
      //        10   -   20
      MaxKey.within(MaxKey.Range(0, 20), MaxKey.Range(10, 20)) shouldBe false
      //0           -       21
      //        10   -   20
      MaxKey.within(MaxKey.Range(0, 21), MaxKey.Range(10, 20)) shouldBe false
      //        10|10
      //        10   -   20
      MaxKey.within(MaxKey.Range(10, 10), MaxKey.Range(10, 20)) shouldBe true
      //        10 - 11
      //        10   -   20
      MaxKey.within(MaxKey.Range(10, 11), MaxKey.Range(10, 20)) shouldBe true
      //        10   - 19
      //        10   -   20
      MaxKey.within(MaxKey.Range(10, 19), MaxKey.Range(10, 20)) shouldBe true
      //        10   -   20
      //        10   -   20
      MaxKey.within(MaxKey.Range(10, 20), MaxKey.Range(10, 20)) shouldBe true
      //        10   -     21
      //        10   -   20
      MaxKey.within(MaxKey.Range(10, 21), MaxKey.Range(10, 20)) shouldBe false
      //          11 - 12
      //        10   -   20
      MaxKey.within(MaxKey.Range(11, 12), MaxKey.Range(10, 20)) shouldBe true
      //          11 -   20
      //        10   -   20
      MaxKey.within(MaxKey.Range(11, 20), MaxKey.Range(10, 20)) shouldBe true
      //             19- 20
      //        10   -   20
      MaxKey.within(MaxKey.Range(19, 20), MaxKey.Range(10, 20)) shouldBe true
      //                 20|20
      //        10   -   20
      MaxKey.within(MaxKey.Range(20, 20), MaxKey.Range(10, 20)) shouldBe false
      //                 20  - 21
      //        10   -   20
      MaxKey.within(MaxKey.Range(20, 21), MaxKey.Range(10, 20)) shouldBe false

    }

    "maxKey on fixed" in {
      //0   -   10
      //        10
      MaxKey.within(MaxKey.Range(0, 10), MaxKey.Fixed(10)) shouldBe false
      //0    -    11
      //        10
      MaxKey.within(MaxKey.Range(0, 11), MaxKey.Fixed(10)) shouldBe false
      //        10|10
      //        10
      MaxKey.within(MaxKey.Range(10, 10), MaxKey.Fixed(10)) shouldBe true
      //        10 - 11
      //        10
      MaxKey.within(MaxKey.Range(10, 11), MaxKey.Fixed(10)) shouldBe false
      //           11 - 12
      //        10
      MaxKey.within(MaxKey.Range(11, 12), MaxKey.Fixed(10)) shouldBe false
    }
  }

}
