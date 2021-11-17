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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Error.Segment.ExceptionHandler
import swaydb.{Benchmark, IO}
import swaydb.core.CommonAssertions._
import swaydb.effect.Reserve
import swaydb.testkit.RunThis._

class CachePerformanceSpec extends AnyWordSpec with Matchers {

  val range = 1 to 1000000

  "initialising caches" in {
    Benchmark.time("initialising caches") {
      runThis(range.size.times) {
        Cache.deferredIO[swaydb.Error.Segment, swaydb.Error.ReservedResource, Int, Int](None, _ => randomIOStrategy(), swaydb.Error.ReservedResource(Reserve.free(name = "test")))() {
          (int, self) =>
            IO.Right(int)
        }
      }
    }.toDouble should be < 0.13
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
