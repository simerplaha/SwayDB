package swaydb.core.util

import org.scalatest.{FlatSpec, Matchers}
import swaydb.core.TestData._
import swaydb.core.RunThis._
import swaydb.data.slice.Slice

class CRC32Spec extends FlatSpec with Matchers {

  it should "apply CRC on bytes" in {
    runThis(100.times) {
      val bytes = randomBytesSlice(randomIntMax(100) + 1)
      CRC32.forBytes(bytes) should be >= 1L
    }
  }

  it should "return 0 for empty bytes" in {
    CRC32.forBytes(Slice.emptyBytes) shouldBe 0L
  }

}
