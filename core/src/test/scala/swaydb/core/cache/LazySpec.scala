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

package swaydb.core.cache

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._

import scala.concurrent.duration._
import scala.util.Random
import scala.collection.parallel.CollectionConverters._

class LazySpec extends AnyWordSpec with Matchers with MockFactory {

  "Lazy.value" when {
    "empty" should {
      "return undefined & empty and not set value on getOrElse" in {
        def doTest(isSynchronised: Boolean, stored: Boolean) = {
          val lazyValue = Lazy.value[Int](isSynchronised, stored, None)

          lazyValue.isDefined shouldBe false
          lazyValue.isEmpty shouldBe true
          lazyValue.getOrElse(10) shouldBe 10
          lazyValue.isDefined shouldBe false
          lazyValue.isEmpty shouldBe true

          lazyValue.getOrElse(20) shouldBe 20
          lazyValue.isDefined shouldBe false
          lazyValue.isEmpty shouldBe true
          lazyValue.getOrElse(100) shouldBe 100
          lazyValue.isDefined shouldBe false
          lazyValue.isEmpty shouldBe true

          //map and flatMap should return empty since value is not set
          lazyValue map (_ => randomInt()) shouldBe empty
          lazyValue flatMap (_ => Some(randomInt())) shouldBe empty
          lazyValue.isDefined shouldBe false
          lazyValue.isEmpty shouldBe true
        }

        doTest(isSynchronised = true, stored = true)
        doTest(isSynchronised = true, stored = false)
        doTest(isSynchronised = false, stored = false)
        doTest(isSynchronised = false, stored = true)
      }
    }

    "value is already set" should {
      "not update value on getOrSet but update on set" in {
        def doTest(isSynchronised: Boolean) = {
          val lazyValue = Lazy.value[Int](isSynchronised, true, None)

          lazyValue.getOrSet(20) shouldBe 20
          lazyValue.getOrElse(fail()) shouldBe 20
          lazyValue.isDefined shouldBe true
          lazyValue.isEmpty shouldBe false

          //but overwrites when set is invoked
          lazyValue.set(100)
          lazyValue.getOrSet(fail()) shouldBe 100
          lazyValue.getOrElse(fail()) shouldBe 100

          //map and flatMap should return value
          lazyValue.map(previous => previous + 1) should contain(101)
          lazyValue.flatMap(previous => Some(previous + 2)) should contain(102)
        }

        doTest(isSynchronised = true)
        doTest(isSynchronised = false)
      }
    }

    "synchronised and stored" should {
      "not allow concurrent modifications of value" in {
        val value = Random.nextInt()

        val mockValueFunction = mockFunction[Int]
        mockValueFunction.expects() returning value //this function is only invoked once.

        val lazyValue = Lazy.value[Int](synchronised = true, stored = true, None)

        (1 to 10000).par foreach {
          _ =>
            lazyValue getOrSet {
              sleep(1.millisecond)
              mockValueFunction.apply()
            }
        }

        lazyValue.get() should contain(value)

        //map and flatMap should return value
        lazyValue map (value => value + 1) should contain(value + 1)
        lazyValue flatMap (value => Some(value + 2)) should contain(value + 2)
      }
    }

    "synchronised is false" should {
      "allow concurrent modifications of value" in {
        val value = Random.nextInt()

        val mockValueFunction = mockFunction[Int]
        mockValueFunction.expects() returning value repeat (2 to 100) //this function is invoked more than once.

        val lazyValue = Lazy.value[Int](synchronised = false, stored = true, None)

        (1 to 10000).par foreach {
          _ =>
            lazyValue getOrSet {
              sleep(1.millisecond)
              mockValueFunction.apply()
            }
        }

        lazyValue.get() should contain(value)

        //map and flatMap should return value
        lazyValue map (_ => 20101) should contain(20101)
        lazyValue flatMap (_ => Some(32323)) should contain(32323)
      }
    }
  }

  "Lazy.io" when {
    "empty" in {
      def doTest(isSynchronised: Boolean, stored: Boolean) = {
        val initialValue = eitherOne(None, Some(randomIntMax()))

        val lazyValue = Lazy.io[Throwable, Int](isSynchronised, stored, initialValue)

        if (stored)
          initialValue foreach {
            initialValue =>
              lazyValue.isDefined shouldBe true
              lazyValue.get().get.get shouldBe initialValue
              lazyValue.getOrElse(fail()) shouldBe IO.Right(initialValue)
              lazyValue.clear()
          }

        lazyValue.isDefined shouldBe false
        lazyValue getOrElse 10 shouldBe 10
        lazyValue.isDefined shouldBe false

        lazyValue getOrElse 20 shouldBe 20
        lazyValue.isDefined shouldBe false
        lazyValue getOrElse 100 shouldBe 100
        lazyValue.isDefined shouldBe false

        lazyValue.clear()
        lazyValue.isDefined shouldBe false
      }

      doTest(isSynchronised = true, stored = true)
      doTest(isSynchronised = true, stored = false)
      doTest(isSynchronised = false, stored = false)
      doTest(isSynchronised = false, stored = true)
    }

    "nonEmpty" in {
      def doTest(isSynchronised: Boolean) = {
        val initialValue = eitherOne(None, Some(randomIntMax()))

        val lazyValue = Lazy.io[Throwable, Int](isSynchronised, true, initialValue)

        initialValue foreach {
          initialValue =>
            lazyValue.isDefined shouldBe true
            lazyValue.get().get.get shouldBe initialValue
            lazyValue.getOrElse(fail()) shouldBe IO.Right(initialValue)
            lazyValue.clear()
        }

        lazyValue.getOrSet(IO(20)) shouldBe IO.Right(20)
        lazyValue.getOrElse(fail()) shouldBe IO.Right(20)
        lazyValue.isDefined shouldBe true

        //but overwrites when set is invoked
        lazyValue set IO.Right(100)
        lazyValue.getOrSet(fail()) shouldBe IO.Right(100)
        lazyValue.getOrElse(fail()) shouldBe IO.Right(100)

        lazyValue.clear()
        lazyValue.isDefined shouldBe false
      }

      doTest(isSynchronised = true)
      doTest(isSynchronised = false)
    }

    "synchronised and stored" should {
      "not allow concurrent modifications" in {
        val value = randomInt()
        implicit val exception = swaydb.IO.ExceptionHandler.Throwable

        val mockValueFunction = mockFunction[Int]
        mockValueFunction.expects() returning value //this function is only invoked once.

        val initialValue = eitherOne(None, Some(randomIntMax()))

        val lazyValue = Lazy.io[Throwable, Int](synchronised = true, stored = true, initialValue)

        initialValue foreach {
          initialValue =>
            lazyValue.isDefined shouldBe true
            lazyValue.get().get.get shouldBe initialValue
            lazyValue.getOrElse(fail()) shouldBe IO.Right(initialValue)
            lazyValue.clear()
        }

        (1 to 10000).par foreach {
          _ =>
            lazyValue getOrSet IO(mockValueFunction.apply())
        }

        lazyValue.get() should contain(IO.Right(value))

        //map and flatMap should return value
        lazyValue map (value => value + 1) shouldBe IO.Right(Some(value + 1))
        lazyValue flatMap (value => IO.Right(value + 2)) shouldBe IO.Right(Some(value + 2))

        lazyValue.clear()
        lazyValue.isDefined shouldBe false
        lazyValue.isEmpty shouldBe true
      }
    }

    "synchronised is false" should {
      "allow concurrent modifications" in {
        val value = Random.nextInt()
        implicit val exception = swaydb.IO.ExceptionHandler.Throwable

        val mockValueFunction = mockFunction[Int]
        mockValueFunction.expects() returning value repeat (2 to 100) //this function is invoked more than once.

        val initialValue = eitherOne(None, Some(randomIntMax()))

        val lazyValue = Lazy.io[Throwable, Int](synchronised = false, stored = true, initialValue)

        initialValue foreach {
          initialValue =>
            lazyValue.isDefined shouldBe true
            lazyValue.get().get.get shouldBe initialValue
            lazyValue.getOrElse(fail()) shouldBe IO.Right(initialValue)
            lazyValue.clear()
        }

        (1 to 10000).par foreach {
          _ =>
            lazyValue getOrSet IO {
              sleep(1.millisecond)
              mockValueFunction.apply()
            }
        }

        lazyValue.get() should contain(IO.Right(value))

        //map and flatMap should return value
        lazyValue map (_ + 1) shouldBe IO.Right(Some(value + 1))
        lazyValue flatMap (value => IO.Right(value + 1)) shouldBe IO.Right(Some(value + 1))

        lazyValue.clear()
        lazyValue.isDefined shouldBe false
      }
    }

    "recover from failure on set & getOrSet" in {
      def doTest(isSynchronised: Boolean, stored: Boolean) = {
        val lazyValue = Lazy.io[Throwable, Int](isSynchronised, stored, None)

        lazyValue.getOrSet(throw IO.throwable("failed")).left.get.getMessage shouldBe "failed"
        lazyValue.isEmpty shouldBe true
        lazyValue.isDefined shouldBe false
        lazyValue.get() shouldBe empty

        lazyValue.set(throw IO.throwable("failed")).left.get.getMessage shouldBe "failed"
        lazyValue.isEmpty shouldBe true
        lazyValue.isDefined shouldBe false
        lazyValue.get() shouldBe empty
      }

      doTest(isSynchronised = true, stored = true)
      doTest(isSynchronised = true, stored = false)
      doTest(isSynchronised = false, stored = false)
      doTest(isSynchronised = false, stored = true)
    }
  }
}
