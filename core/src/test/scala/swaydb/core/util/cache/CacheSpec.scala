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

  "Cache.io" should {
    "invoke the init function only once on success" in {

      def doTest(delayedIO: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[Int]]
        val cache =
          if (delayedIO)
            Cache.io[Unit, Int](synchronised = (_: Unit) => isSynchronised, reserved = (_: Unit) => isReserved, stored = (_: Unit) => true)(_ => mock.apply())
          else
            Cache.io[Unit, Int](synchronised = isSynchronised, reserved = isReserved, stored = true)(_ => mock.apply())

        cache.isCached shouldBe false

        //getOrElse on un-cached should set the cache
        mock.expects() returning IO(123)

        cache.value() shouldBe IO.Success(123)
        cache.isCached shouldBe true
        cache.value() shouldBe IO.Success(123) //value again mock function is not invoked again

        cache.map()(int => int) shouldBe IO.Success(123)
        cache.flatMap()(int => IO.Success(int + 1)) shouldBe IO.Success(124)

        cache.clear()
        cache.isCached shouldBe false
      }

      doTest(delayedIO = true, isSynchronised = true, isReserved = true)
      doTest(delayedIO = true, isSynchronised = true, isReserved = false)
      doTest(delayedIO = true, isSynchronised = false, isReserved = true)
      doTest(delayedIO = true, isSynchronised = false, isReserved = false)

      doTest(delayedIO = false, isSynchronised = true, isReserved = true)
      doTest(delayedIO = false, isSynchronised = true, isReserved = false)
      doTest(delayedIO = false, isSynchronised = false, isReserved = true)
      doTest(delayedIO = false, isSynchronised = false, isReserved = false)
    }

    "not cache on failure" in {
      def doTest(delayedIO: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[Int]]

        val cache =
          if (delayedIO)
            Cache.io[Unit, Int](synchronised = (_: Unit) => isSynchronised, reserved = (_: Unit) => isReserved, stored = (_: Unit) => true)(_ => mock.apply())
          else
            Cache.io[Unit, Int](synchronised = isSynchronised, reserved = isReserved, stored = true)(_ => mock.apply())

        cache.isCached shouldBe false
        mock.expects() returning IO.Failure("Kaboom!")

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
      }

      doTest(delayedIO = true, isSynchronised = true, isReserved = true)
      doTest(delayedIO = true, isSynchronised = true, isReserved = false)
      doTest(delayedIO = true, isSynchronised = false, isReserved = true)
      doTest(delayedIO = true, isSynchronised = false, isReserved = false)

      doTest(delayedIO = false, isSynchronised = true, isReserved = true)
      doTest(delayedIO = false, isSynchronised = true, isReserved = false)
      doTest(delayedIO = false, isSynchronised = false, isReserved = true)
      doTest(delayedIO = false, isSynchronised = false, isReserved = false)
    }

    "cache on successful map and flatMap" in {
      def doTest(delayedIO: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[Int]]

        val cache =
          if (delayedIO)
            Cache.io[Unit, Int](synchronised = (_: Unit) => isSynchronised, reserved = (_: Unit) => isReserved, stored = (_: Unit) => true)(_ => mock.apply())
          else
            Cache.io[Unit, Int](synchronised = isSynchronised, reserved = isReserved, stored = true)(_ => mock.apply())

        cache.isCached shouldBe false

        mock.expects() returning IO(111)
        cache.map()(int => int) shouldBe IO.Success(111)
        cache.flatMap()(int => IO.Success(int + 1)) shouldBe IO.Success(112)

        cache.clear()
        cache.isCached shouldBe false
        mock.expects() returning IO(222)
        cache.flatMap()(int => IO.Success(int + 1)) shouldBe IO.Success(223)
      }

      doTest(delayedIO = true, isSynchronised = true, isReserved = true)
      doTest(delayedIO = true, isSynchronised = true, isReserved = false)
      doTest(delayedIO = true, isSynchronised = false, isReserved = true)
      doTest(delayedIO = true, isSynchronised = false, isReserved = false)

      doTest(delayedIO = false, isSynchronised = true, isReserved = true)
      doTest(delayedIO = false, isSynchronised = true, isReserved = false)
      doTest(delayedIO = false, isSynchronised = false, isReserved = true)
      doTest(delayedIO = false, isSynchronised = false, isReserved = false)
    }

    "not cache on unsuccessful map and flatMap" in {
      def doTest(delayedIO: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[Int]]

        val cache =
          if (delayedIO)
            Cache.io[Unit, Int](synchronised = (_: Unit) => isSynchronised, reserved = (_: Unit) => isReserved, stored = (_: Unit) => true)(_ => mock.apply())
          else
            Cache.io[Unit, Int](synchronised = isSynchronised, reserved = isReserved, stored = true)(_ => mock.apply())

        cache.isCached shouldBe false

        mock.expects() returning IO.Failure("Kaboom!") repeat 2.times
        cache.map()(int => int).failed.get.exception.getMessage shouldBe "Kaboom!"
        cache.isCached shouldBe false
        cache.flatMap()(int => IO.Success(int + 1)).failed.get.exception.getMessage shouldBe "Kaboom!"
        cache.isCached shouldBe false

        mock.expects() returning IO(222)
        cache.flatMap()(int => IO.Success(int + 1)) shouldBe IO.Success(223)
        cache.isCached shouldBe true
      }

      doTest(delayedIO = true, isSynchronised = true, isReserved = true)
      doTest(delayedIO = true, isSynchronised = true, isReserved = false)
      doTest(delayedIO = true, isSynchronised = false, isReserved = true)
      doTest(delayedIO = true, isSynchronised = false, isReserved = false)

      doTest(delayedIO = false, isSynchronised = true, isReserved = true)
      doTest(delayedIO = false, isSynchronised = true, isReserved = false)
      doTest(delayedIO = false, isSynchronised = false, isReserved = true)
      doTest(delayedIO = false, isSynchronised = false, isReserved = false)
    }

    "concurrent access to reserved io" should {
      "not be allowed" in {
        val cache =
          Cache.io[Unit, Int](synchronised = false, reserved = true, stored = true) {
            _ =>
              sleep(1.millisecond) //delay access
              IO.Success(10)
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
