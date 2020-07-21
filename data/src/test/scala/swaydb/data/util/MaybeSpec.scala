/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.data.util.Maybe._

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
