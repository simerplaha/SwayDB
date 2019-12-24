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
    override def noneSONC: Option = Option.None
  }

  private object Option {
    final object None extends Option {
      override def isNoneSONC: Boolean = true
      override def getSONC: Option.Some =
        throw new Exception("Not a some value")
    }

    case class Some(value: String = Random.nextString(10)) extends Option {
      override def isNoneSONC: Boolean = false
      override def getSONC: Option.Some = this
    }
  }

  "none" in {
    val some: Option = Option.Some()
    some.noneSONC shouldBe Option.None

    val none: Option = Option.None
    none.noneSONC shouldBe Option.None
  }

  "isEmpty" in {
    val some: Option = Option.Some()
    some.isNoneSONC shouldBe false

    val none: Option = Option.None
    none.isNoneSONC shouldBe true
  }

  "get" in {
    val some: Option = Option.Some("some")
    some.getSONC shouldBe Option.Some("some")

    val none: Option = Option.None
    assertThrows[Exception](none.getSONC)
  }

  "flatMap" in {
    val some: Option = Option.Some("some")
    some.flatMapSONC {
      s =>
        s shouldBe some
        Option.Some("other")
    } shouldBe Option.Some("other")

    val none: Option = Option.None
    none.flatMapSONC(_ => fail()) shouldBe none
  }

  "foreach" in {
    val some: Option = Option.Some("some")
    var invoked = false
    some.foreachSONC {
      _ =>
        invoked = true
    }
    invoked shouldBe true

    val none: Option = Option.None
    none.foreachSONC(_ => fail())
  }

  "getOrElse" in {
    val some: Option = Option.Some("some")
    some.getOrElseSONC(fail()) shouldBe some

    val none: Option = Option.None
    none.getOrElseSONC(Option.Some("")) shouldBe Option.Some("")
  }

  "orElse" in {
    val some: Option = Option.Some("some")
    some.orElseSONC(fail()) shouldBe some

    val none: Option = Option.None
    none.orElseSONC(Option.Some("")) shouldBe Option.Some("")
  }

  "exists" in {
    val some: Option = Option.Some("some")
    some.existsSONC(_ => true) shouldBe true
    some.existsSONC(_ => false) shouldBe false

    val none: Option = Option.None
    none.existsSONC(_ => fail()) shouldBe false
  }

  "forAll" in {
    val some: Option = Option.Some("some")
    some.forallSONC(_ => true) shouldBe true
    some.forallSONC(_ => false) shouldBe false

    val none: Option = Option.None
    none.forallSONC(_ => fail()) shouldBe true
  }

  "flatMapSomeOrNone" in {
    sealed trait Option2 extends SomeOrNoneCovariant[Option2, Option2.Some2] {
      override def noneSONC: Option2 = Option2.None2
    }

    object Option2 {
      implicit final object None2 extends Option2 {
        override def isNoneSONC: Boolean = true
        override def getSONC: Option2.Some2 =
          throw new Exception("Not a some value")
      }

      case class Some2(value: String = Random.nextString(10)) extends Option2 {
        override def isNoneSONC: Boolean = false
        override def getSONC: Option2.Some2 = this
      }
    }

    val someOption: Option = Option.Some("some")

    val result: Option2 =
      someOption.flatMapSomeSONC(Option2.None2: Option2) {
        some =>
          some shouldBe someOption
          Option2.Some2("some")
      }

    result shouldBe Option2.Some2("some")

    Option.None.flatMapSomeSONC(Option2.None2: Option2) {
      some =>
        fail()
    } shouldBe Option2.None2

  }
}
