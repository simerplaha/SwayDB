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

package swaydb.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.utils.Maybe._

import scala.util.Success

class MaybeSpec extends AnyWordSpec with Matchers {

  "some" should {
    val some = Maybe.some("some")

    "create a tagged value" in {
      some shouldBe "some"
      some.isSome shouldBe true
      some.isNone shouldBe false
    }

    "map" in {
      some.mapMayBe(string => string + " one") shouldBe "some one"
    }

    "flatMap" in {
      some.flatMapMayBe(string => Maybe.some(string + " one")) shouldBe "some one"
    }

    "foreach" in {
      var ran = false
      some.foreachMayBe {
        _ =>
          ran = true
      }
      ran shouldBe true
    }

    "get" in {
      some.getUnsafe shouldBe "some"
    }

    "orElse" in {
      some.orElseMayBe(Maybe.some("")) shouldBe "some"
    }

    "getOrElse" in {
      some.getOrElseMayBe("") shouldBe "some"
    }

    "foldLeft" in {
      some.foldLeftMayBe(1) {
        case (string, output) =>
          string shouldBe "some"
          output + 1
      } shouldBe 2
    }

    "toOption" in {
      some.toOption shouldBe Some("some")
    }

    "toTry" in {
      some.toTry shouldBe Success("some")
    }
  }

  "none" should {
    val none = Maybe.none[String]

    "create a tagged value" in {
      none shouldBe null
      none.isNone shouldBe true
      none.isSome shouldBe false
      none.toOption shouldBe empty
    }

    "map" in {
      none.mapMayBe(string => string + " one") shouldBe null
    }

    "flatMap" in {
      none.flatMapMayBe(string => Maybe.some(string + " one")) shouldBe null
    }

    "foreach" in {
      var ran = false
      none.foreachMayBe {
        _ =>
          ran = true
      }
      ran shouldBe false
    }

    "get" in {
      assertThrows[NoSuchElementException] {
        none.getUnsafe
      }
    }

    "orElse" in {
      none.orElseMayBe(Maybe.some("")) shouldBe ""
    }

    "getOrElse" in {
      none.getOrElseMayBe("") shouldBe ""
    }

    "foldLeft" in {
      none.foldLeftMayBe(1) {
        case (string, output) =>
          string shouldBe "some"
          output
      } shouldBe 1
    }

    "toOption" in {
      none.toOption shouldBe None
    }

    "toTry" in {
      none.toTry.isFailure shouldBe true
    }
  }
}
