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

package swaydb.core.cache

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IO
import swaydb.effect.Base._
import swaydb.testkit.RunThis._

import scala.concurrent.duration._
import scala.util.Random

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
          lazyValue map (_ => Random.nextInt()) shouldBe empty
          lazyValue flatMap (_ => Some(Random.nextInt())) shouldBe empty
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
        val initialValue = eitherOne(None, Some(Random.nextInt()))

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
        val initialValue = eitherOne(None, Some(Random.nextInt()))

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
        val value = Random.nextInt()
        implicit val exception = swaydb.IO.ExceptionHandler.Throwable

        val mockValueFunction = mockFunction[Int]
        mockValueFunction.expects() returning value //this function is only invoked once.

        val initialValue = eitherOne(None, Some(Random.nextInt()))

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

        val initialValue = eitherOne(None, Some(Random.nextInt()))

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

        lazyValue.getOrSet(throw new Exception("failed")).left.get.getMessage shouldBe "failed"
        lazyValue.isEmpty shouldBe true
        lazyValue.isDefined shouldBe false
        lazyValue.get() shouldBe empty

        lazyValue.set(throw new Exception("failed")).left.get.getMessage shouldBe "failed"
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
