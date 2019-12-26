package swaydb.core.segment

import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData._

class ReadStateSpec extends WordSpec with Matchers {

  "it" should {
    "return true for non existing keys" in {

      val state = ReadState.limitHashMap(10)

      (1 to 100) foreach {
        _ =>
          state.getSegmentStateOrNull(Paths.get(randomString)) shouldBe null
      }
    }

    "assign" in {
      val state = ReadState.limitHashMap(100, 100)

      val keys =
        (1 to 100) map {
          _ =>
            val key = Paths.get(randomString)
            state.setSegmentState(key, null)
            key
        }

      keys foreach {
        key =>
          state.getSegmentStateOrNull(key) should be(null)
      }
    }
  }

}
