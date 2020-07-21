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
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.data.Reserve
import swaydb.data.config.IOStrategy

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.parallel.CollectionConverters._

class CacheSpec extends AnyWordSpec with Matchers with MockFactory {

  //run all possible combinations of Cache tests on different cache types.
  def runTestForAllCombinations(test: (Boolean, Boolean, Boolean, Boolean) => Unit) = {

    def run(test: (Boolean, Boolean, Boolean) => Unit) = {
      test(true, true, true)
      test(true, true, false)
      test(true, false, true)
      test(true, false, false)

      test(false, true, true)
      test(false, true, false)
      test(false, false, true)
      test(false, false, false)
    }

    run(test(true, _, _, _))
    run(test(false, _, _, _))
  }

  def getIOStrategy(isConcurrent: Boolean, isSynchronised: Boolean, isReserved: Boolean, stored: Boolean)(unit: Unit): IOStrategy =
    if (isConcurrent)
      IOStrategy.ConcurrentIO(cacheOnAccess = stored)
    else if (isSynchronised)
      IOStrategy.SynchronisedIO(cacheOnAccess = stored)
    else if (isReserved)
      IOStrategy.AsyncIO(cacheOnAccess = stored)
    else
      IOStrategy.ConcurrentIO(cacheOnAccess = stored) //then it's concurrent

  /**
   * Return a partial cache with applied configuration which requires the cache body.
   */
  def getTestCache(isDeferred: Boolean,
                   isConcurrent: Boolean,
                   isSynchronised: Boolean,
                   isReserved: Boolean,
                   stored: Boolean,
                   initialValue: Option[Int]): ((Unit, Cache[swaydb.Error.Segment, Unit, Int]) => IO[swaydb.Error.Segment, Int]) => Cache[swaydb.Error.Segment, Unit, Int] =
    if (isDeferred)
      Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Unit, Int](
        initial = initialValue,
        strategy = getIOStrategy(isConcurrent, isSynchronised, isReserved, stored),
        reserveError = swaydb.Error.ReservedResource(Reserve.free(name = "test"))
      )()
    else if (isConcurrent)
      Cache.concurrentIO(
        synchronised = false,
        stored = stored,
        initial = initialValue
      )
    else if (isSynchronised)
      Cache.concurrentIO(
        synchronised = true,
        stored = stored,
        initial = initialValue
      )
    else if (isReserved)
      Cache.reservedIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Unit, Int](
        stored = stored,
        swaydb.Error.ReservedResource(Reserve.free(name = "test")),
        initial = initialValue
      )
    else
      Cache.io(
        strategy = getIOStrategy(isConcurrent, isSynchronised, isReserved, stored)(()),
        reserveError = swaydb.Error.ReservedResource(Reserve.free(name = "test")),
        initial = initialValue
      ) //then it's strategy IO

  "valueIO" should {
    "always return initial value" in {
      val cache: Cache[swaydb.Error.Segment, Unit, Int] = Cache.valueIO(10)
      cache.isCached shouldBe true
      cache.getIO should contain(IO.Right(10))
      runThisParallel(100.times) {
        cache.value(()).get shouldBe 10
      }
    }
  }

  "it" should {
    "fetch data only once on success" in {
      def doTest(isDeferredIO: Boolean, isConcurrent: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[swaydb.Error.Segment, Int]]

        val initialValue = eitherOne(Some(1), None)

        val cache = getTestCache(isDeferredIO, isConcurrent, isSynchronised, isReserved, stored = true, initialValue = initialValue)((_, _) => mock.apply())

        initialValue foreach {
          initialValue =>
            cache.isCached shouldBe true
            cache.getIO().get.get shouldBe initialValue
            cache.clear()
        }

        cache.isCached shouldBe false

        cache.getIO() shouldBe empty

        //getOrElse on un-cached should set the cache
        cache.getOrElse(IO(111)) shouldBe IO(111)
        cache.isCached shouldBe false // still not cached
        cache.getIO() shouldBe empty //still not cached
        mock.expects() returning IO(123)

        cache.value(()) shouldBe IO.Right(123)
        cache.isCached shouldBe true
        cache.getIO() shouldBe Some(IO.Right(123))
        cache.value(()) shouldBe IO.Right(123) //value again mock function is not invoked again
        cache.getOrElse(fail()) shouldBe IO.Right(123)

        val mapNotStoredCache = cache.map(int => IO(int + 1))
        mapNotStoredCache.getIO() shouldBe Some(IO.Right(124))
        mapNotStoredCache.value(fail()) shouldBe IO.Right(124)
        mapNotStoredCache.value(fail()) shouldBe IO.Right(124)
        mapNotStoredCache.isCached shouldBe cache.isCached

        //        val mapStoredCache = cache.mapConcurrentStored(int => IO(int + 5))
        //        mapStoredCache.getIO() shouldBe Some(IO.Right(128))
        //        mapStoredCache.value(fail()) shouldBe IO.Right(128)
        //        mapStoredCache.value(fail()) shouldBe IO.Right(128)
        //        mapStoredCache.isCached shouldBe cache.isCached

        val flatMapStoredCache = cache.flatMap(Cache.concurrentIO(randomBoolean(), stored = true, None)((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO(int + 2)))
        flatMapStoredCache.value(()) shouldBe IO.Right(125)
        flatMapStoredCache.value(fail()) shouldBe IO.Right(125)
        flatMapStoredCache.getIO() shouldBe Some(IO.Right(125))

        val flatMapNotStoredCache =
          cache flatMap {
            Cache.concurrentIO(
              synchronised = randomBoolean(),
              stored = false,
              initial = orNone(Some(randomInt()))) {
              (int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) =>
                IO(int + 3)
            }
          }
        flatMapNotStoredCache.value(()) shouldBe IO.Right(126)
        flatMapNotStoredCache.value(fail()) shouldBe IO.Right(126)
        //stored is false but get() will apply the value function fetching the value from parent cache.
        flatMapNotStoredCache.getIO() shouldBe Some(IO.Right(126))

        //getOrElse on cached is not invoked on new value
        cache.getOrElse(fail()) shouldBe IO(123)

        cache.clear()
        cache.isCached shouldBe false
        mapNotStoredCache.isCached shouldBe false

        //        mapStoredCache.isCached shouldBe true
        flatMapStoredCache.isCached shouldBe true

        //        mapStoredCache.clear()
        //        mapStoredCache.isCached shouldBe false
        //        mapStoredCache.getIO() shouldBe empty

        flatMapStoredCache.clear()
        flatMapStoredCache.isCached shouldBe false
        flatMapStoredCache.getIO() shouldBe empty
      }

      runTestForAllCombinations(doTest)
    }

    "not cache on failure" in {
      def doTest(isDeferredIO: Boolean, isConcurrent: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[swaydb.Error.Segment, Int]]

        val cache = getTestCache(isDeferredIO, isConcurrent, isSynchronised, isReserved, stored = true, None)((_, _: Cache[swaydb.Error.Segment, Unit, Int]) => mock.apply())

        val kaboom = IO.failed("Kaboom!").exception

        cache.isCached shouldBe false
        mock.expects() returning IO.failed(kaboom) repeat 5.times
        cache.getOrElse(IO(233)) shouldBe IO(233)
        cache.isCached shouldBe false

        //failure
        cache.value(()).left.get shouldBe swaydb.Error.Fatal(kaboom)
        cache.isCached shouldBe false

        val mapCache = cache.map(int => IO(int))
        mapCache.value(()).left.get shouldBe swaydb.Error.Fatal(kaboom)
        mapCache.value(()).left.get shouldBe swaydb.Error.Fatal(kaboom)

        val flatMapCache = cache.flatMap(Cache.concurrentIO(randomBoolean(), randomBoolean(), None)((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO(int + 1)))
        flatMapCache.value(()).left.get shouldBe swaydb.Error.Fatal(kaboom)
        flatMapCache.value(()).left.get shouldBe swaydb.Error.Fatal(kaboom)

        //success
        mock.expects() returning IO(123)
        cache.value(()) shouldBe IO.Right(123) //value again mock function is not invoked again
        mapCache.value(()) shouldBe IO.Right(123)
        flatMapCache.value(()) shouldBe IO.Right(124)
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
      def doTest(isDeferredIO: Boolean, isConcurrent: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[swaydb.Error.Segment, Int]]

        val cache =
          getTestCache(
            isDeferred = isDeferredIO,
            isConcurrent = isConcurrent,
            isSynchronised = isSynchronised,
            isReserved = isReserved,
            stored = true,
            initialValue = None
          ) {
            (_, _: Cache[swaydb.Error.Segment, Unit, Int]) =>
              mock.apply()
          }

        cache.isCached shouldBe false

        mock.expects() returning IO(111)
        cache.map(int => IO(int)).value(()) shouldBe IO(111)

        cache.isCached shouldBe true

        val flatMapCache1 =
          cache.flatMap(
            Cache.concurrentIO(
              synchronised = randomBoolean(),
              stored = true,
              initial = None
            )((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO(int + 1))
          )

        //since cache is populated value will not be fetched
        flatMapCache1.value(fail()) shouldBe IO(112) //value is not invoked since value is already set.
        //since cache is populated value will not be fetched
        flatMapCache1.getIO().get.get shouldBe 112

        cache.clear()
        cache.isCached shouldBe false
        mock.expects() returning IO(222)

        val flatMapCache2 =
          cache flatMap {
            Cache.concurrentIO(
              synchronised = randomBoolean(),
              stored = true,
              initial = None
            )((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO(int + 2))
          }

        flatMapCache2.value(()) shouldBe IO(224)
        flatMapCache2.value(fail()) shouldBe IO(224)

        cache.isCached shouldBe true
        //on cached value fail() is not invoked.
        cache.getOrElse(fail()) shouldBe IO(222)

        cache.clear()
        cache.isCached shouldBe false

        flatMapCache1.value(fail()) shouldBe IO(112)
        flatMapCache2.value(fail()) shouldBe IO(224)

        //child caches are fetched above but since they are already populated
        //root cache's value should still remaing cleared
        cache.isCached shouldBe false
        cache.getIO() shouldBe empty

        flatMapCache1.getIO().get.get shouldBe 112
        flatMapCache2.getIO().get.get shouldBe 224

        cache.isCached shouldBe false
      }

      runTestForAllCombinations(doTest)
    }

    "not cache on unsuccessful map and flatMap" in {
      def doTest(isDeferredIO: Boolean, isConcurrent: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
        val mock = mockFunction[IO[swaydb.Error.Segment, Int]]

        val cache =
          getTestCache(
            isDeferred = isDeferredIO,
            isConcurrent = isConcurrent,
            isSynchronised = isSynchronised,
            isReserved = isReserved,
            stored = true,
            initialValue = None
          )((_, _: Cache[swaydb.Error.Segment, Unit, Int]) => mock.apply())

        cache.isCached shouldBe false

        val exception = IO.failed("Kaboom!").exception

        mock.expects() returning IO.failed(exception) repeat 2.times
        cache.map(IO(_)).value(()).left.get shouldBe swaydb.Error.Fatal(exception)
        cache.isCached shouldBe false
        cache.flatMap {
          Cache.reservedIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Int, Int](true, swaydb.Error.ReservedResource(Reserve.free(name = "test")), None) {
            (int, _: Cache[swaydb.Error.Segment, Int, Int]) =>
              IO.Right(int + 1)
          }
        }.value(()).left.get shouldBe swaydb.Error.Fatal(exception)
        cache.isCached shouldBe false

        mock.expects() returning IO(222)
        cache.flatMap(Cache.concurrentIO(randomBoolean(), true, None)((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO.Right(int + 1))).value(()) shouldBe IO.Right(223)
        cache.isCached shouldBe true
      }

      runTestForAllCombinations(doTest)
    }

    "clear all flatMapped caches" in {
      val cache = Cache.concurrentIO[swaydb.Error.Segment, Unit, Int](randomBoolean(), true, None)((_, self) => IO(1))
      cache.value(()) shouldBe IO.Right(1)
      cache.isCached shouldBe true

      val nestedCache = Cache.concurrentIO[swaydb.Error.Segment, Int, Int](randomBoolean(), true, None)((int, self) => IO(int + 1))

      val flatMapCache = cache.flatMap(nestedCache)
      flatMapCache.value(()) shouldBe IO.Right(2)
      flatMapCache.isCached shouldBe true
      nestedCache.isCached shouldBe true

      val mapCache = flatMapCache.map(int => IO(int + 1))
      mapCache.value(()) shouldBe IO.Right(3)
      mapCache.isCached shouldBe true

      mapCache.clear()

      cache.isCached shouldBe false
      flatMapCache.isCached shouldBe false
      nestedCache.isCached shouldBe false
      mapCache.isCached shouldBe false
    }

    //    "store cache value on mapStored" in {
    //      def doTest(isDeferredIO: Boolean, isConcurrent: Boolean, isSynchronised: Boolean, isReserved: Boolean) = {
    //        val mock = mockFunction[IO[swaydb.Error.Segment, Int]]
    //        val rootCache = getTestCache(isDeferredIO, isConcurrent, isSynchronised, isReserved, stored = false, None)((_, self) => mock.apply())
    //        //ensure rootCache is not stored
    //        mock.expects() returning IO(1)
    //        mock.expects() returning IO(2)
    //
    //        rootCache.isCached shouldBe false
    //        rootCache.value(()) shouldBe IO(1)
    //        rootCache.isCached shouldBe false
    //        rootCache.value(()) shouldBe IO(2)
    //        rootCache.isCached shouldBe false
    //
    //        mock.expects() returning IO(3)
    //        //run stored
    //        val storedCache = rootCache.mapConcurrentStored {
    //          previous =>
    //            previous shouldBe 3
    //            IO(100)
    //        }
    //        storedCache.value(()) shouldBe IO(100)
    //        storedCache.isCached shouldBe true
    //        //rootCache not value is not read
    //        storedCache.value(fail()) shouldBe IO(100)
    //        //original rootCache is still empty
    //        rootCache.isCached shouldBe false
    //
    //        //run stored
    //        val storedCache2 = storedCache.mapConcurrentStored {
    //          previous =>
    //            //stored rootCache's value is received
    //            previous shouldBe 100
    //            IO(200)
    //        }
    //        storedCache2.value(()) shouldBe IO(200)
    //        storedCache2.isCached shouldBe true
    //        //rootCache not value is not read
    //        storedCache2.value(fail()) shouldBe IO(200)
    //        storedCache.value(fail()) shouldBe IO(100)
    //        //original rootCache is still empty
    //        rootCache.isCached shouldBe false
    //        storedCache2.isCached shouldBe true
    //        storedCache.isCached shouldBe true
    //
    //        //clearing child cache, clears it all.
    //        storedCache2.clear()
    //        storedCache2.isCached shouldBe false
    //        storedCache.isCached shouldBe false
    //        rootCache.isCached shouldBe false
    //      }
    //
    //      runTestForAllCombinations(doTest)
    //    }

    "concurrent access to reserved io" should {
      "not be allowed" in {
        def doTest(blockIO: Boolean) = {

          @volatile var invokeCount = 0

          val simpleCache =
            if (blockIO)
              Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Unit, Int](None, strategy = _ => IOStrategy.AsyncIO(true), swaydb.Error.ReservedResource(Reserve.free(name = "test")))() {
                (_, _) =>
                  invokeCount += 1
                  sleep(1.millisecond) //delay access
                  IO.Right(10)
              }
            else
              Cache.reservedIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Unit, Int](stored = true, swaydb.Error.ReservedResource(Reserve.free(name = "test")), None) {
                (_, _) =>
                  invokeCount += 1
                  sleep(1.millisecond) //delay access
                  IO.Right(10)
              }

          val cache =
            if (randomBoolean())
              simpleCache.map(IO(_))
            else if (randomBoolean())
              simpleCache.flatMap {
                Cache.concurrentIO[swaydb.Error.Segment, Int, Int](synchronised = false, stored = false, None)((int: Int, _) => IO(int))
              }
            else
              simpleCache

          if (blockIO) {
            cache.value(())
            cache.clear()
          }

          val futures =
          //concurrently do requests
            Future.sequence {
              (1 to 100) map {
                _ =>
                  Future().flatMap(_ => cache.value(()).toFuture)
              }
            }

          //results in failure since some thread has reserved.
          val failure = futures.failed.await
          failure shouldBe a[swaydb.Exception.ReservedResource]

          //eventually it's freed
          eventual {
            failure.asInstanceOf[swaydb.Exception.ReservedResource].reserve.isBusy shouldBe false
          }

          if (blockIO)
            invokeCount shouldBe 2 //2 because it's cleared above.
          else
            invokeCount shouldBe 1
        }

        doTest(blockIO = true)
        doTest(blockIO = false)
      }
    }
  }

  "value" should {
    "cache function's output" in {
      def doTest(isSynchronised: Boolean) = {
        @volatile var got1 = false
        val cache =
          Cache.noIO[Unit, Int](synchronised = Random.nextBoolean(), stored = true, None) {
            (_, self) =>
              if (!got1) {
                got1 = true
                1
              } else {
                Random.nextInt()
              }
          }

        cache.isCached shouldBe false

        cache.value(()) shouldBe 1

        (1 to 1000).par foreach {
          _ =>
            cache.value(()) shouldBe 1
        }

        cache.isCached shouldBe true

        cache.clear()
        cache.isCached shouldBe false

        cache.value(()) should not be 1
        cache.isCached shouldBe true
      }

      doTest(isSynchronised = true)
      doTest(isSynchronised = false)
    }
  }
}
