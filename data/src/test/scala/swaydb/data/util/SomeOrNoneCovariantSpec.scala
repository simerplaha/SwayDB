/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.data.util

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class SomeOrNoneCovariantSpec extends WordSpec with Matchers {

  private sealed trait Option extends SomeOrNoneCovariant[Option, Option.Some] {
    override def noneC: Option = Option.None
  }

  private object Option {
    final object None extends Option {
      override def isNoneC: Boolean = true
      override def getC: Option.Some =
        throw new Exception("Not a some value")
    }

    case class Some(value: String = Random.nextString(10)) extends Option {
      override def isNoneC: Boolean = false
      override def getC: Option.Some = this
    }
  }

  "none" in {
    val some: Option = Option.Some()
    some.noneC shouldBe Option.None

    val none: Option = Option.None
    none.noneC shouldBe Option.None
  }

  "isEmpty" in {
    val some: Option = Option.Some()
    some.isNoneC shouldBe false

    val none: Option = Option.None
    none.isNoneC shouldBe true
  }

  "get" in {
    val some: Option = Option.Some("some")
    some.getC shouldBe Option.Some("some")

    val none: Option = Option.None
    assertThrows[Exception](none.getC)
  }

  "flatMap" in {
    val some: Option = Option.Some("some")
    some.flatMapC {
      s =>
        s shouldBe some
        Option.Some("other")
    } shouldBe Option.Some("other")

    val none: Option = Option.None
    none.flatMapC(_ => fail()) shouldBe none
  }

  "foreach" in {
    val some: Option = Option.Some("some")
    var invoked = false
    some.foreachC {
      _ =>
        invoked = true
    }
    invoked shouldBe true

    val none: Option = Option.None
    none.foreachC(_ => fail())
  }

  "getOrElse" in {
    val some: Option = Option.Some("some")
    some.getOrElseC(fail()) shouldBe some

    val none: Option = Option.None
    none.getOrElseC(Option.Some("")) shouldBe Option.Some("")
  }

  "orElse" in {
    val some: Option = Option.Some("some")
    some.orElseC(fail()) shouldBe some

    val none: Option = Option.None
    none.orElseC(Option.Some("")) shouldBe Option.Some("")
  }

  "exists" in {
    val some: Option = Option.Some("some")
    some.existsC(_ => true) shouldBe true
    some.existsC(_ => false) shouldBe false

    val none: Option = Option.None
    none.existsC(_ => fail()) shouldBe false
  }

  "forAll" in {
    val some: Option = Option.Some("some")
    some.forallC(_ => true) shouldBe true
    some.forallC(_ => false) shouldBe false

    val none: Option = Option.None
    none.forallC(_ => fail()) shouldBe true
  }

  "flatMapSomeOrNone" in {
    sealed trait Option2 extends SomeOrNoneCovariant[Option2, Option2.Some2] {
      override def noneC: Option2 = Option2.None2
    }

    object Option2 {
      implicit final object None2 extends Option2 {
        override def isNoneC: Boolean = true
        override def getC: Option2.Some2 =
          throw new Exception("Not a some value")

      }

      case class Some2(value: String = Random.nextString(10)) extends Option2 {
        override def isNoneC: Boolean = false
        override def getC: Option2.Some2 = this
      }
    }

    val someOption: Option = Option.Some("some")

    val result: Option2 =
      someOption.flatMapSomeC(Option2.None2: Option2) {
        some =>
          some shouldBe someOption
          Option2.Some2("some")
      }

    result shouldBe Option2.Some2("some")

    Option.None.flatMapSomeC(Option2.None2: Option2) {
      some =>
        fail()
    } shouldBe Option2.None2

  }

  "onSomeSideEffect" in {
    val some = Option.Some()
    some.onSomeSideEffectC(_ shouldBe some) shouldBe some

    Option.None.onSomeSideEffectC(_ => fail()) shouldBe Option.None
  }
}
