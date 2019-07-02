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

  "Lazy.value" when {
    "empty" should {
      "return undefined and not set value on getOrElse" in {
        val lazyValue = Lazy.value[Int](Random.nextBoolean(), Random.nextBoolean())

        lazyValue.isDefined shouldBe false
        lazyValue getOrElse 10 shouldBe 10
        lazyValue.isDefined shouldBe false

        lazyValue.getOrElse(20) shouldBe 20
        lazyValue.isDefined shouldBe false
        lazyValue.getOrElse(100) shouldBe 100
        lazyValue.isDefined shouldBe false

        lazyValue map (_ => 20101) shouldBe empty
        lazyValue flatMap (_ => Some(32323)) shouldBe empty
      }
    }

    "value is already set" should {
      "not update value on getOrSet but update on set" in {
        val lazyValue = Lazy.value[Int](Random.nextBoolean(), true)

        lazyValue.getOrSet(20) shouldBe 20
        lazyValue getOrElse 10 shouldBe 20
        lazyValue.isDefined shouldBe true

        //but overwrites when set is invoked
        lazyValue.set(100)
        lazyValue.getOrSet(39393) shouldBe 100
        lazyValue getOrElse 10 shouldBe 100

        lazyValue map (_ => 20101) should contain(20101)
        lazyValue flatMap (_ => Some(32323)) should contain(32323)
      }
    }

    "synchronised and stored" should {
      "not allow concurrent modifications of value" in {
        val value = Random.nextInt()

        val mockValueFunction = mockFunction[Int]
        mockValueFunction expects() returning value //this function is only invoked once.

        val lazyValue = Lazy.value[Int](synchronised = true, stored = true)

        (1 to 10000).par foreach {
          _ =>
            lazyValue getOrSet mockValueFunction.apply()
        }

        lazyValue.get() should contain(value)

        lazyValue map (_ => 20101) should contain(20101)
        lazyValue flatMap (_ => Some(32323)) should contain(32323)
      }
    }

    "synchronised is false" should {
      "allow concurrent modifications of value" in {
        val value = Random.nextInt()

        val mockValueFunction = mockFunction[Int]
        mockValueFunction expects() returning value repeat (2 to 100) //this function is invoked more than once.

        val lazyValue = Lazy.value[Int](synchronised = false, stored = true)

        (1 to 10000).par foreach {
          _ =>
            lazyValue getOrSet mockValueFunction.apply()
        }

        lazyValue.get() should contain(value)

        lazyValue map (_ => 20101) should contain(20101)
        lazyValue flatMap (_ => Some(32323)) should contain(32323)
      }
    }
  }

  "Lazy.io" when {
    "empty" should {
      "return undefined and not set value on getOrElse" in {
        val lazyValue = Lazy.io[Int](Random.nextBoolean(), Random.nextBoolean())

        lazyValue.isDefined shouldBe false
        lazyValue getOrElse 10 shouldBe 10
        lazyValue.isDefined shouldBe false

        lazyValue getOrElse 20 shouldBe 20
        lazyValue.isDefined shouldBe false
        lazyValue getOrElse 100 shouldBe 100
        lazyValue.isDefined shouldBe false
      }
    }

    "value is already set" should {
      "not update value on getOrSet but update on set" in {
        val lazyValue = Lazy.io[Int](Random.nextBoolean(), true)

        lazyValue getOrSet IO(20) shouldBe IO.Success(20)
        lazyValue getOrElse 10 shouldBe IO.Success(20)
        lazyValue.isDefined shouldBe true

        //but overwrites when set is invoked
        lazyValue set IO.Success(100)
        lazyValue getOrSet IO.Success(4234242) shouldBe IO.Success(100)
        lazyValue getOrElse 10 shouldBe IO.Success(100)
      }
    }

    "synchronised and stored" should {
      "not allow concurrent modifications of value" in {
        val value = Random.nextInt()

        val mockValueFunction = mockFunction[Int]
        mockValueFunction expects() returning value //this function is only invoked once.

        val lazyValue = Lazy.io[Int](synchronised = true, stored = true)

        (1 to 10000).par foreach {
          _ =>
            lazyValue getOrSet IO(mockValueFunction.apply())
        }

        lazyValue.get() should contain(IO.Success(value))

        lazyValue map (_ => 20101) shouldBe IO.Success(Some(20101))
        lazyValue flatMap (_ => IO.Success(32323)) shouldBe IO.Success(Some(32323))
      }
    }

    "synchronised is false" should {
      "allow concurrent modifications of value" in {
        val value = Random.nextInt()

        val mockValueFunction = mockFunction[Int]
        mockValueFunction expects() returning value repeat (2 to 100) //this function is invoked more than once.

        val lazyValue = Lazy.io[Int](synchronised = false, stored = true)

        (1 to 10000).par foreach {
          _ =>
            lazyValue getOrSet IO(mockValueFunction.apply())
        }

        lazyValue.get() should contain(IO.Success(value))

        lazyValue map (_ => 20101) shouldBe IO.Success(Some(20101))
        lazyValue flatMap (_ => IO.Success(32323)) shouldBe IO.Success(Some(32323))
      }
    }
  }
}
