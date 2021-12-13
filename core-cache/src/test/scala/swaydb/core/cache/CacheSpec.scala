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
import swaydb.Error.Segment.ExceptionHandler
import swaydb.effect.{IOStrategy, Reserve}
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._
import swaydb.{Error, IO}

import scala.annotation.tailrec
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

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
      cache.state() should contain(IO.Right(10))
      runThisParallel(100.times) {
        cache.getOrFetch(()).get shouldBe 10
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
            cache.state().get.get shouldBe initialValue
            cache.clear()
        }

        cache.isCached shouldBe false

        cache.state() shouldBe empty

        //getOrElse on un-cached should set the cache
        cache.getOrElse(IO(111)) shouldBe IO(111)
        cache.isCached shouldBe false // still not cached
        cache.state() shouldBe empty //still not cached
        mock.expects() returning IO(123)

        cache.getOrFetch(()) shouldBe IO.Right(123)
        cache.isCached shouldBe true
        cache.state() shouldBe Some(IO.Right(123))
        cache.getOrFetch(()) shouldBe IO.Right(123) //value again mock function is not invoked again
        cache.getOrElse(fail()) shouldBe IO.Right(123)

        val mapNotStoredCache = cache.map(int => IO(int + 1))
        mapNotStoredCache.state() shouldBe Some(IO.Right(124))
        mapNotStoredCache.getOrFetch(fail()) shouldBe IO.Right(124)
        mapNotStoredCache.getOrFetch(fail()) shouldBe IO.Right(124)
        mapNotStoredCache.isCached shouldBe cache.isCached

        //        val mapStoredCache = cache.mapConcurrentStored(int => IO(int + 5))
        //        mapStoredCache.getIO() shouldBe Some(IO.Right(128))
        //        mapStoredCache.value(fail()) shouldBe IO.Right(128)
        //        mapStoredCache.value(fail()) shouldBe IO.Right(128)
        //        mapStoredCache.isCached shouldBe cache.isCached

        val flatMapStoredCache = cache.flatMap(Cache.concurrentIO(Random.nextBoolean(), stored = true, None)((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO(int + 2)))
        flatMapStoredCache.getOrFetch(()) shouldBe IO.Right(125)
        flatMapStoredCache.getOrFetch(fail()) shouldBe IO.Right(125)
        flatMapStoredCache.state() shouldBe Some(IO.Right(125))

        val flatMapNotStoredCache =
          cache flatMap {
            Cache.concurrentIO(
              synchronised = Random.nextBoolean(),
              stored = false,
              initial = orNone(Some(Random.nextInt()))) {
              (int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) =>
                IO(int + 3)
            }
          }
        flatMapNotStoredCache.getOrFetch(()) shouldBe IO.Right(126)
        flatMapNotStoredCache.getOrFetch(fail()) shouldBe IO.Right(126)
        //stored is false but get() will apply the value function fetching the value from parent cache.
        flatMapNotStoredCache.state() shouldBe Some(IO.Right(126))

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
        flatMapStoredCache.state() shouldBe empty
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
        cache.getOrFetch(()).left.get shouldBe swaydb.Error.Fatal(kaboom)
        cache.isCached shouldBe false

        val mapCache = cache.map(int => IO(int))
        mapCache.getOrFetch(()).left.get shouldBe swaydb.Error.Fatal(kaboom)
        mapCache.getOrFetch(()).left.get shouldBe swaydb.Error.Fatal(kaboom)

        val flatMapCache = cache.flatMap(Cache.concurrentIO(Random.nextBoolean(), Random.nextBoolean(), None)((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO(int + 1)))
        flatMapCache.getOrFetch(()).left.get shouldBe swaydb.Error.Fatal(kaboom)
        flatMapCache.getOrFetch(()).left.get shouldBe swaydb.Error.Fatal(kaboom)

        //success
        mock.expects() returning IO(123)
        cache.getOrFetch(()) shouldBe IO.Right(123) //value again mock function is not invoked again
        mapCache.getOrFetch(()) shouldBe IO.Right(123)
        flatMapCache.getOrFetch(()) shouldBe IO.Right(124)
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
        cache.map(int => IO(int)).getOrFetch(()) shouldBe IO(111)

        cache.isCached shouldBe true

        val flatMapCache1 =
          cache.flatMap(
            Cache.concurrentIO(
              synchronised = Random.nextBoolean(),
              stored = true,
              initial = None
            )((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO(int + 1))
          )

        //since cache is populated value will not be fetched
        flatMapCache1.getOrFetch(fail()) shouldBe IO(112) //value is not invoked since value is already set.
        //since cache is populated value will not be fetched
        flatMapCache1.state().get.get shouldBe 112

        cache.clear()
        cache.isCached shouldBe false
        mock.expects() returning IO(222)

        val flatMapCache2 =
          cache flatMap {
            Cache.concurrentIO(
              synchronised = Random.nextBoolean(),
              stored = true,
              initial = None
            )((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO(int + 2))
          }

        flatMapCache2.getOrFetch(()) shouldBe IO(224)
        flatMapCache2.getOrFetch(fail()) shouldBe IO(224)

        cache.isCached shouldBe true
        //on cached value fail() is not invoked.
        cache.getOrElse(fail()) shouldBe IO(222)

        cache.clear()
        cache.isCached shouldBe false

        flatMapCache1.getOrFetch(fail()) shouldBe IO(112)
        flatMapCache2.getOrFetch(fail()) shouldBe IO(224)

        //child caches are fetched above but since they are already populated
        //root cache's value should still remaing cleared
        cache.isCached shouldBe false
        cache.state() shouldBe empty

        flatMapCache1.state().get.get shouldBe 112
        flatMapCache2.state().get.get shouldBe 224

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
        cache.map(IO(_)).getOrFetch(()).left.get shouldBe swaydb.Error.Fatal(exception)
        cache.isCached shouldBe false
        cache.flatMap {
          Cache.reservedIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Int, Int](true, swaydb.Error.ReservedResource(Reserve.free(name = "test")), None) {
            (int, _: Cache[swaydb.Error.Segment, Int, Int]) =>
              IO.Right(int + 1)
          }
        }.getOrFetch(()).left.get shouldBe swaydb.Error.Fatal(exception)
        cache.isCached shouldBe false

        mock.expects() returning IO(222)
        cache.flatMap(Cache.concurrentIO(Random.nextBoolean(), true, None)((int: Int, _: Cache[swaydb.Error.Segment, Int, Int]) => IO.Right(int + 1))).getOrFetch(()) shouldBe IO.Right(223)
        cache.isCached shouldBe true
      }

      runTestForAllCombinations(doTest)
    }

    "clear all flatMapped caches" in {
      val cache = Cache.concurrentIO[swaydb.Error.Segment, Unit, Int](Random.nextBoolean(), true, None)((_, self) => IO(1))
      cache.getOrFetch(()) shouldBe IO.Right(1)
      cache.isCached shouldBe true

      val nestedCache = Cache.concurrentIO[swaydb.Error.Segment, Int, Int](Random.nextBoolean(), true, None)((int, self) => IO(int + 1))

      val flatMapCache = cache.flatMap(nestedCache)
      flatMapCache.getOrFetch(()) shouldBe IO.Right(2)
      flatMapCache.isCached shouldBe true
      nestedCache.isCached shouldBe true

      val mapCache = flatMapCache.map(int => IO(int + 1))
      mapCache.getOrFetch(()) shouldBe IO.Right(3)
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
        import scala.concurrent.ExecutionContext.Implicits.global

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
            if (Random.nextBoolean())
              simpleCache.map(IO(_))
            else if (Random.nextBoolean())
              simpleCache.flatMap {
                Cache.concurrentIO[swaydb.Error.Segment, Int, Int](synchronised = false, stored = false, None)((int: Int, _) => IO(int))
              }
            else
              simpleCache

          if (blockIO) {
            cache.getOrFetch(())
            cache.clear()
          }

          val futures =
          //concurrently do requests
            Future.sequence {
              (1 to 100) map {
                _ =>
                  Future().flatMap(_ => cache.getOrFetch(()).toFuture)
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

        cache.getOrFetch(()) shouldBe 1

        (1 to 1000).par foreach {
          _ =>
            cache.getOrFetch(()) shouldBe 1
        }

        cache.isCached shouldBe true

        cache.clear()
        cache.isCached shouldBe false

        cache.getOrFetch(()) should not be 1
        cache.isCached shouldBe true
      }

      doTest(isSynchronised = true)
      doTest(isSynchronised = false)
    }
  }


  "clear and apply" should {
    def createCache(): Cache[Error.Segment, Unit, Int] =
      Cache.io[swaydb.Error.Segment, Error.ReservedResource, Unit, Int](
        strategy = eitherOne(IOStrategy.SynchronisedIO(true), IOStrategy.AsyncIO(true)),
        reserveError = swaydb.Error.ReservedResource(Reserve.free(name = "test")),
        initial = None
      ) {
        case (_, _) =>
          IO.Right(1)
      }

    "apply before clearing atomically" in {
      runThis(10.times, log = true) {
        @volatile var count = 0

        val cache = createCache()

        val maxIterations = 1000000

        @tailrec
        def doApply(tried: Int): Unit =
          if (tried > 1000000)
            fail("Unable to apply update")
          else
            cache.clearApply(_ => IO(count += 1)) match {
              case IO.Right(_) =>
                ()

              case IO.Left(_: Error.ReservedResource) =>
                doApply(tried + 1)
            }

        runThisParallel(times = maxIterations)(doApply(0))

        count shouldBe maxIterations

        cache.isCached shouldBe false
        cache.isStored shouldBe true
      }
    }

    "return failure on clean" when {
      "cache is populated" in {
        runThis(10.times, log = true) {
          val cache = createCache()
          cache.getOrFetch(()) shouldBe IO.Right(1)

          //fails and does not clear the cache
          cache.clearApply(_ => IO(throw new Exception("kaboom!"))).left.get.exception.getMessage shouldBe "kaboom!"

          cache.isCached shouldBe true
          cache.isStored shouldBe true
        }
      }

      "cache is not populated" in {
        runThis(10.times, log = true) {
          val cache = createCache()
          //fails and does not clear the cache
          cache.clearApply(_ => IO(throw new Exception("kaboom!"))).left.get.exception.getMessage shouldBe "kaboom!"

          cache.isCached shouldBe false
          cache.isStored shouldBe true

          cache.getOrFetch(()) shouldBe IO.Right(1)
          cache.isCached shouldBe true
        }
      }
    }
  }
}
