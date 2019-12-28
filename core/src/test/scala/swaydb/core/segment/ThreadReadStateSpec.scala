package swaydb.core.segment

import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData._
import swaydb.core.segment.SegmentReadState

class ThreadReadStateSpec extends WordSpec with Matchers {

  "it" should {
    "return Null" when {
      "not states exist" in {
        val state = ThreadReadState.random

        (1 to 100) foreach {
          _ =>
            state.getSegmentState(Paths.get(randomString)) shouldBe SegmentReadState.Null
        }
      }

      "states exists but queries states do not exist" in {
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
            state.getSegmentState(key) shouldBe SegmentReadState.Null
        }
      }
    }
  }
}
