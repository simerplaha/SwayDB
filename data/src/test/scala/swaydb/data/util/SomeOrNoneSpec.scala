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

class SomeOrNoneSpec extends WordSpec with Matchers {

  private sealed trait Option extends SomeOrNone[Option, Option.Some] {
    override def none: Option = Option.None
  }

  private object Option {
    final object None extends Option {
      override def isEmpty: Boolean = true
      override def get: Option.Some =
        throw new Exception("Not a some value")
    }

    case class Some(value: String = Random.nextString(10)) extends Option {
      override def isEmpty: Boolean = false
      override def get: Option.Some = this
    }
  }

  "none" in {
    val some: Option = Option.Some()
    some.none shouldBe Option.None

    val none: Option = Option.None
    none.none shouldBe Option.None
  }

  "isEmpty" in {
    val some: Option = Option.Some()
    some.isEmpty shouldBe false

    val none: Option = Option.None
    none.isEmpty shouldBe true
  }

  "get" in {
    val some: Option = Option.Some("some")
    some.get shouldBe Option.Some("some")

    val none: Option = Option.None
    assertThrows[Exception](none.get)
  }

  "flatMap" in {
    val some: Option = Option.Some("some")
    some.flatMap {
      s =>
        s shouldBe some
        Option.Some("other")
    } shouldBe Option.Some("other")

    val none: Option = Option.None
    none.flatMap(_ => fail()) shouldBe none
  }

  "foreach" in {
    val some: Option = Option.Some("some")
    var invoked = false
    some.foreach {
      _ =>
        invoked = true
    }
    invoked shouldBe true

    val none: Option = Option.None
    none.foreach(_ => fail())
  }

  "getOrElse" in {
    val some: Option = Option.Some("some")
    some.getOrElse(fail()) shouldBe some

    val none: Option = Option.None
    none.getOrElse(Option.Some("")) shouldBe Option.Some("")
  }

  "orElse" in {
    val some: Option = Option.Some("some")
    some.orElse(fail()) shouldBe some

    val none: Option = Option.None
    none.orElse(Option.Some("")) shouldBe Option.Some("")
  }

  "exists" in {
    val some: Option = Option.Some("some")
    some.exists(_ => true) shouldBe true
    some.exists(_ => false) shouldBe false

    val none: Option = Option.None
    none.exists(_ => fail()) shouldBe false
  }

  "forAll" in {
    val some: Option = Option.Some("some")
    some.forall(_ => true) shouldBe true
    some.forall(_ => false) shouldBe false

    val none: Option = Option.None
    none.forall(_ => fail()) shouldBe true
  }
}
