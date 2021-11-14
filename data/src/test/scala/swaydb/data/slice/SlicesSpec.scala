package swaydb.data.slice

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SlicesSpec extends AnyWordSpec with Matchers {

  "get" when {
    "empty" in {
      assertThrows[NoSuchElementException](Slices[Int](Array.empty))
    }

    "all slices are equal" in {
      val slices = Slices(Array(Slice.range(0, 4), Slice.range(5, 9), Slice.range(10, 14)))

      slices.blockSize shouldBe 5
      slices.size shouldBe 15

      Slice.range(0, 14) foreach {
        int =>
          slices.get(int) shouldBe int
          slices.getUnchecked_Unsafe(int) shouldBe int
      }

      assertThrows[ArrayIndexOutOfBoundsException](slices.get(15))
    }

    "it contains a small last slice" in {
      val slices = Slices(Array(Slice.range(0, 4), Slice.range(5, 9), Slice.range(10, 11)))

      slices.blockSize shouldBe 5
      slices.size shouldBe 12

      Slice.range(0, 11) foreach {
        int =>
          slices.get(int) shouldBe int
          slices.getUnchecked_Unsafe(int) shouldBe int
      }

      assertThrows[ArrayIndexOutOfBoundsException](slices.get(12))
    }
  }
}
