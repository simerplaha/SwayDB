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
import swaydb.core.RunThis._
import swaydb.data.IO

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class CacheSpec extends WordSpec with Matchers with MockFactory {

  def runTestForAllCombinations(test: (Boolean, Boolean, Boolean) => Unit) = {
    test(true, true, true)
    test(true, true, false)
    test(true, false, true)
    test(true, false, false)

    test(false, true, true)
    test(false, true, false)
    test(false, false, true)
    test(false, false, false)
  }

  "Cache.io" should {
    "fetch data only once on success" in {
      def doTest(delayedIO: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[Int]]
        val cache =
          if (delayedIO)
            Cache.delayedIO[Unit, Int](synchronised = (_: Unit) => isSynchronised, reserved = (_: Unit) => isReserved, stored = (_: Unit) => true)(_ => mock.apply())
          else
            Cache.io[Unit, Int](synchronised = isSynchronised, reserved = isReserved, stored = true)(_ => mock.apply())

        cache.isCached shouldBe false

        //getOrElse on un-cached should set the cache
        cache.getOrElse(IO(111)) shouldBe IO(111)
        cache.isCached shouldBe false // still not cached
        mock.expects() returning IO(123)

        cache.value() shouldBe IO.Success(123)
        cache.isCached shouldBe true
        cache.value() shouldBe IO.Success(123) //value again mock function is not invoked again

        cache.foreach()(int => int shouldBe 123)
        cache.map()(int => int) shouldBe IO.Success(123)
        cache.flatMap()(int => IO.Success(int + 1)) shouldBe IO.Success(124)

        //getOrElse on cached is not invoked on new value
        cache.getOrElse(???) shouldBe IO(123)

        cache.clear()
        cache.isCached shouldBe false
      }

      runTestForAllCombinations(doTest)
    }

    "not cache on failure" in {
      def doTest(delayedIO: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[Int]]

        val cache =
          if (delayedIO)
            Cache.delayedIO[Unit, Int](synchronised = (_: Unit) => isSynchronised, reserved = (_: Unit) => isReserved, stored = (_: Unit) => true)(_ => mock.apply())
          else
            Cache.io[Unit, Int](synchronised = isSynchronised, reserved = isReserved, stored = true)(_ => mock.apply())

        cache.isCached shouldBe false
        mock.expects() returning IO.Failure("Kaboom!")
        cache.getOrElse(IO(233)) shouldBe IO(233)
        cache.isCached shouldBe false

        //failure
        cache.value().failed.get.exception.getMessage shouldBe "Kaboom!"
        cache.isCached shouldBe false

        //success
        mock.expects() returning IO(123)
        cache.value() shouldBe IO.Success(123) //value again mock function is not invoked again
        cache.isCached shouldBe true
        cache.isCached shouldBe true
        cache.clear()
        cache.isCached shouldBe false
        cache.getOrElse(IO(233)) shouldBe IO(233)
        cache.isCached shouldBe false
      }

      runTestForAllCombinations(doTest)
    }

    "cache on successful map and flatMap" in {
      def doTest(delayedIO: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[Int]]

        val cache =
          if (delayedIO)
            Cache.delayedIO[Unit, Int](synchronised = (_: Unit) => isSynchronised, reserved = (_: Unit) => isReserved, stored = (_: Unit) => true)(_ => mock.apply())
          else
            Cache.io[Unit, Int](synchronised = isSynchronised, reserved = isReserved, stored = true)(_ => mock.apply())

        cache.isCached shouldBe false

        mock.expects() returning IO(111)
        cache.foreach()(int => int shouldBe 111)
        cache.map()(int => int) shouldBe IO(111)
        cache.flatMap()(int => IO(int + 1)) shouldBe IO(112)

        cache.clear()
        cache.isCached shouldBe false
        mock.expects() returning IO(222)
        cache.flatMap()(int => IO(int + 1)) shouldBe IO(223)

        //on cached value ??? is not invoked.
        cache.getOrElse(???) shouldBe IO(222)
      }

      runTestForAllCombinations(doTest)
    }

    "not cache on unsuccessful map and flatMap" in {
      def doTest(delayedIO: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[Int]]

        val cache =
          if (delayedIO)
            Cache.delayedIO[Unit, Int](synchronised = (_: Unit) => isSynchronised, reserved = (_: Unit) => isReserved, stored = (_: Unit) => true)(_ => mock.apply())
          else
            Cache.io[Unit, Int](synchronised = isSynchronised, reserved = isReserved, stored = true)(_ => mock.apply())

        cache.isCached shouldBe false

        mock.expects() returning IO.Failure("Kaboom!") repeat 3.times
        cache.foreach()(_ => fail("error was expected. This should not be executed"))
        cache.map()(int => int).failed.get.exception.getMessage shouldBe "Kaboom!"
        cache.isCached shouldBe false
        cache.flatMap()(int => IO.Success(int + 1)).failed.get.exception.getMessage shouldBe "Kaboom!"
        cache.isCached shouldBe false

        mock.expects() returning IO(222)
        cache.flatMap()(int => IO.Success(int + 1)) shouldBe IO.Success(223)
        cache.isCached shouldBe true
      }

      runTestForAllCombinations(doTest)
    }

    "concurrent access to reserved io" should {
      "not be allowed" in {
        def doTest(delayedIO: Boolean) = {

          @volatile var invokeCount = 0

          val cache =
            if (delayedIO)
              Cache.delayedIO[Unit, Int](synchronised = _ => false, reserved = _ => true, stored = _ => true) {
                _ =>
                  invokeCount += 1
                  sleep(5.millisecond) //delay access
                  IO.Success(10)
              }
            else
              Cache.io[Unit, Int](synchronised = false, reserved = true, stored = true) {
                _ =>
                  invokeCount += 1
                  sleep(5.millisecond) //delay access
                  IO.Success(10)
              }

          if (delayedIO) {
            cache.value()
            cache.clear()
          }

          val futures =
          //concurrently do requests
            Future.sequence {
              (1 to 100) map {
                _ =>
                  Future().flatMap(_ => cache.value().toFuture)
              }
            }

          //results in failure since some thread has reserved.
          val failure = futures.failed.await
          failure shouldBe a[IO.Exception.ReservedValue]

          //eventually it's freed
          eventual {
            failure.asInstanceOf[IO.Exception.ReservedValue].busy.isBusy shouldBe false
          }

          if (delayedIO)
            invokeCount shouldBe 2 //since it's cleared above.
          else
            invokeCount shouldBe 1
        }

        doTest(delayedIO = true)
        doTest(delayedIO = false)
      }
    }
  }

  "Cache.value" should {
    "cache function's output" in {
      def doTest(isSynchronised: Boolean) = {
        @volatile var got1 = false
        val cache =
          Cache.unsafe[Unit, Int](Random.nextBoolean(), true) {
            case _: Unit =>
              if (!got1) {
                got1 = true
                1
              } else {
                Random.nextInt()
              }
          }

        cache.isCached shouldBe false

        cache.value() shouldBe 1

        (1 to 1000).par foreach {
          _ =>
            cache.value() shouldBe 1
        }

        cache.isCached shouldBe true

        cache.clear()
        cache.isCached shouldBe false

        cache.value() should not be 1
        cache.isCached shouldBe true
      }

      doTest(isSynchronised = true)
      doTest(isSynchronised = false)
    }
  }
}
