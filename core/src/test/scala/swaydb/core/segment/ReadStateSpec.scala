package swaydb.core.segment

import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData._
import swaydb.core.segment.ThreadReadState.SegmentState

class ReadStateSpec extends WordSpec with Matchers {

  "it" should {
    "return true for non existing keys" in {

      val state = ThreadReadState.limitHashMap(10)

      (1 to 100) foreach {
        _ =>
          state.getSegmentState(Paths.get(randomString)) shouldBe SegmentState.Null
      }
    }

    "assign" in {
      val state = ThreadReadState.limitHashMap(100, 100)

      val keys =
        (1 to 100) map {
          _ =>
            val key = Paths.get(randomString)
            state.setSegmentState(key, null)
            key
        }

      keys foreach {
        key =>
          state.getSegmentState(key) shouldBe SegmentState.Null
      }
    }
  }

}
