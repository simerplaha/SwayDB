/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.ref.search

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.testkit.TestKit._

import java.nio.file.Paths

class ThreadReadStateSpec extends AnyWordSpec {

  "it" should {
    "return Null" when {
      "not states exist" in {
        val state = ThreadReadState.random

        (1 to 100) foreach {
          _ =>
            state.getSegmentState(Paths.get(randomString())) shouldBe SegmentReadState.Null
        }
      }

      "states exists but queries states do not exist" in {
        val state = ThreadReadState.limitHashMap(100, 100)

        val keys =
          (1 to 100) map {
            _ =>
              val key = Paths.get(randomString())
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
