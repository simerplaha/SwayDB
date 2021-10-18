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

import scala.util.Random

class SomeOrNoneSpec extends AnyWordSpec with Matchers {

  private sealed trait Option extends SomeOrNone[Option, Option.Some] {
    override def noneS: Option = Option.None
  }

  private object Option {
    final object None extends Option {
      override def isNoneS: Boolean = true
      override def getS: Option.Some =
        throw new Exception("Not a some value")
    }

    case class Some(value: String = Random.nextString(10)) extends Option {
      override def isNoneS: Boolean = false
      override def getS: Option.Some = this
    }
  }

  "none" in {
    val some: Option = Option.Some()
    some.noneS shouldBe Option.None

    val none: Option = Option.None
    none.noneS shouldBe Option.None
  }

  "isEmpty" in {
    val some: Option = Option.Some()
    some.isNoneS shouldBe false

    val none: Option = Option.None
    none.isNoneS shouldBe true
  }

  "get" in {
    val some: Option = Option.Some("some")
    some.getS shouldBe Option.Some("some")

    val none: Option = Option.None
    assertThrows[Exception](none.getS)
  }

  "flatMap" in {
    val some: Option = Option.Some("some")
    some.flatMapS {
      s =>
        s shouldBe some
        Option.Some("other")
    } shouldBe Option.Some("other")

    val none: Option = Option.None
    none.flatMapS(_ => fail()) shouldBe none
  }

  "foreach" in {
    val some: Option = Option.Some("some")
    var invoked = false
    some.foreachS {
      _ =>
        invoked = true
    }
    invoked shouldBe true

    val none: Option = Option.None
    none.foreachS(_ => fail())
  }

  "getOrElse" in {
    val some: Option = Option.Some("some")
    some.getOrElseS(fail()) shouldBe some

    val none: Option = Option.None
    none.getOrElseS(Option.Some("")) shouldBe Option.Some("")
  }

  "orElse" in {
    val some: Option = Option.Some("some")
    some.orElseS(fail()) shouldBe some

    val none: Option = Option.None
    none.orElseS(Option.Some("")) shouldBe Option.Some("")
  }

  "exists" in {
    val some: Option = Option.Some("some")
    some.existsS(_ => true) shouldBe true
    some.existsS(_ => false) shouldBe false

    val none: Option = Option.None
    none.existsS(_ => fail()) shouldBe false
  }

  "forAll" in {
    val some: Option = Option.Some("some")
    some.forallS(_ => true) shouldBe true
    some.forallS(_ => false) shouldBe false

    val none: Option = Option.None
    none.forallS(_ => fail()) shouldBe true
  }

  "flatMapSomeOrNone" in {
    sealed trait Option2 extends SomeOrNone[Option2, Option2.Some2] {
      override def noneS: Option2 = Option2.None2
    }

    object Option2 {
      implicit final object None2 extends Option2 {
        override def isNoneS: Boolean = true
        override def getS: Option2.Some2 =
          throw new Exception("Not a some value")
      }

      case class Some2(value: String = Random.nextString(10)) extends Option2 {
        override def isNoneS: Boolean = false
        override def getS: Option2.Some2 = this
      }
    }

    val someOption: Option = Option.Some("some")

    val result: Option2 =
      someOption.flatMapSomeS(Option2.None2: Option2) {
        some =>
          some shouldBe someOption
          Option2.Some2("some")
      }

    result shouldBe Option2.Some2("some")

    Option.None.flatMapSomeS(Option2.None2: Option2) {
      some =>
        fail()
    } shouldBe Option2.None2
  }

  "onSomeSideEffect" in {

    val some = Option.Some()
    some.onSomeSideEffectS(_ shouldBe some) shouldBe some

    Option.None.onSomeSideEffectS(_ => fail()) shouldBe Option.None
  }
}
