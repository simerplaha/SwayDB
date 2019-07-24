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

import org.scalatest.{Matchers, WordSpec}
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.util.Benchmark
import swaydb.data.Reserve
import swaydb.data.io.Core
import swaydb.data.io.Core.IO.Error.ErrorHandler

class CachePerformanceSpec extends WordSpec with Matchers {

  val range = 1 to 1000000

  "initialising caches" in {
    Benchmark("initialising 1 million concurrent caches") {
      runThis(range.size.times) {
        Cache.blockIO[Int, Int](_ => randomIOStrategy(), Core.IO.Error.BusyFuture(Reserve()))(int => IO.Success(int))
      }
    }
  }

  "reading concurrentIO" when {
    "stored & concurrent" in {
      val cache = Cache.concurrentIO[Core.IO.Error, Int, Int](false, true)(int => IO.Success(int))

      Benchmark("reading stored") {
        range foreach {
          i =>
            cache.value(i) shouldBe IO.Success(1)
        }
      }

      Benchmark("reading stored concurrently") {
        range.par foreach {
          i =>
            cache.value(i) shouldBe IO.Success(1)
        }
      }
    }

    "stored & synchronised" in {
      val cache = Cache.concurrentIO[Core.IO.Error, Int, Int](true, true)(int => IO.Success(int))

      Benchmark("reading stored") {
        range foreach {
          i =>
            cache.value(i) shouldBe IO.Success(1)
        }
      }

      Benchmark("reading stored concurrently") {
        range.par foreach {
          i =>
            cache.value(i) shouldBe IO.Success(1)
        }
      }
    }

    "not stored" in {
      val cache = Cache.concurrentIO[Core.IO.Error, Int, Int](false, false)(int => IO.Success(int))

      Benchmark("reading not stored") {
        range foreach {
          i =>
            cache.value(i) shouldBe IO.Success(i)
        }
      }

      Benchmark("reading not stored concurrently") {
        range.par foreach {
          i =>
            cache.value(i) shouldBe IO.Success(i)
        }
      }
    }
  }
}
