package swaydb.core.segment

import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData._

class ReadStateSpec extends WordSpec with Matchers {

  "SlotState" should {
    "return true for non existing keys" in {

      val state = ReadState.limitHashMap(10)

      (1 to 100) foreach {
        _ =>
          state.isSequential(Paths.get(randomString)) shouldBe true
      }
    }

    "assign" in {
      val state = ReadState.limitHashMap(10)

      val keys =
        (1 to 1000) map {
          _ =>
            val key = Paths.get(randomString)
            state.setSequential(key, false)
            key
        }

      keys foreach {
        key =>
          state.isSequential(key) shouldBe false
      }
    }
  }

}
