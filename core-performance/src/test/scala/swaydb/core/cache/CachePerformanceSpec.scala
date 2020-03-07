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

import org.scalatest.{Matchers, WordSpec}
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.util.Benchmark
import swaydb.data.Reserve

import scala.collection.parallel.CollectionConverters._

class CachePerformanceSpec extends WordSpec with Matchers {

  val range = 1 to 1000000

  "initialising caches" in {
    Benchmark.time("initialising caches") {
      runThis(range.size.times) {
        Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Int, Int](None, _ => randomIOStrategy(), swaydb.Error.ReservedResource(Reserve.free(name = "test")))() {
          (int, self) =>
            IO.Right(int)
        }
      }
    } should be < 0.13
  }

  "reading concurrentIO" when {
    "stored & concurrent" in {
      val cache = Cache.concurrentIO[swaydb.Error.Segment, Int, Int](false, true, None)((int, self) => IO.Right(int))

      Benchmark("reading stored") {
        range foreach {
          i =>
            cache.value(i) shouldBe IO.Right(1)
        }
      }

      Benchmark("reading stored concurrently") {
        range.par foreach {
          i =>
            cache.value(i) shouldBe IO.Right(1)
        }
      }
    }

    "stored & synchronised" in {
      val cache = Cache.concurrentIO[swaydb.Error.Segment, Int, Int](true, true, None)((int, self) => IO.Right(int))

      Benchmark("reading stored") {
        range foreach {
          i =>
            cache.value(i) shouldBe IO.Right(1)
        }
      }

      Benchmark("reading stored concurrently") {
        range.par foreach {
          i =>
            cache.value(i) shouldBe IO.Right(1)
        }
      }
    }

    "not stored" in {
      val cache = Cache.concurrentIO[swaydb.Error.Segment, Int, Int](false, false, None)((int, self) => IO.Right(int))

      Benchmark("reading not stored") {
        range foreach {
          i =>
            cache.value(i) shouldBe IO.Right(i)
        }
      }

      Benchmark("reading not stored concurrently") {
        range.par foreach {
          i =>
            cache.value(i) shouldBe IO.Right(i)
        }
      }
    }
  }
}
