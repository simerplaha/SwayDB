package swaydb.data.slice

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SliceNativeSpec extends AnyWordSpec with Matchers {

  "create" in {
    val slice = SliceNative(1, 2, 3)
    slice.toList shouldBe List(1, 2, 3)
    slice.free()
  }

}
