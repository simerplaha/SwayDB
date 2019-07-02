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

package swaydb.core.util.cache

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import swaydb.data.IO

import scala.util.Random

class LazySpec extends WordSpec with Matchers with MockFactory {

  "LazyValue" when {
    "empty" should {
      "return undefined and not set value on getOrElse" in {
        val lazyValue = Lazy.value[Int](Random.nextBoolean())

        lazyValue.isDefined shouldBe false
        lazyValue getOrElse 10 shouldBe 10
        lazyValue.isDefined shouldBe false

        lazyValue.getOrElse(20) shouldBe 20
        lazyValue.isDefined shouldBe false
        lazyValue.getOrElse(100) shouldBe 100
        lazyValue.isDefined shouldBe false
      }
    }

    "set" should {
      "return value and not set new value" in {
        val lazyValue = Lazy.value[Int](Random.nextBoolean())

        lazyValue.getOrSet(20) shouldBe 20
        lazyValue getOrElse 10 shouldBe 20
        lazyValue.isDefined shouldBe true

        lazyValue.set(100)
        lazyValue.getOrSet(39393) shouldBe 100
        lazyValue getOrElse 10 shouldBe 100
      }
    }
  }
}
