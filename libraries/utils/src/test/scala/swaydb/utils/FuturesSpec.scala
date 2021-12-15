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

package swaydb.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.testkit.RunThis._

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.concurrent.Future
import scala.util.Random

class FuturesSpec extends AnyWordSpec with Matchers {

  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  "bounded traverse" should {
    def executeTest(parallelism: Int,
                    input: Seq[Int],
                    delay: Long): Unit = {
      val callTimes = new AtomicInteger(0) //how many times has the traversable been called
      val lastCall = new AtomicLong(0) //last called timestamp

      val result =
        Futures.traverseBounded(parallelism, input) {
          item =>
            Future {
              callTimes.incrementAndGet() //increment the call time
              println(s"parallelism: $parallelism, delay: $delay, item: $item. callTimes: $callTimes. lastCall: $lastCall. head: ${input.head}. last: ${input.last}")

              val called = callTimes.get() //check if this is interval call so it check if the delay occurred
              if (called > parallelism && (called - 1) % parallelism == 0) {
                try {
                  val expectedCallTime = System.currentTimeMillis() - lastCall.get()
                  expectedCallTime should be >= (delay - 20)
                } catch {
                  case exception: Exception =>
                    throw exception //for debugging
                }
              }

              //always set the last max time of invocation
              lastCall.getAndUpdate(
                (currentLastCall: Long) =>
                  System.currentTimeMillis() max currentLastCall
              )

              Thread.sleep(delay)
              item
            }
        }

      result.awaitInf shouldBe input
    }

    "execute tasks in regular interval" when {
      "random" in {
        runThis(200.times, log = true) {
          executeTest(
            parallelism = Random.nextInt(Runtime.getRuntime.availableProcessors()) max 1,
            input = Seq.range(0, Random.nextInt(20) max 1),
            delay = Random.nextInt(40)
          )
        }
      }

      "parallelism == Int.MaxValue" in {
        runThis(200.times, log = true) {
          executeTest(
            parallelism = Int.MaxValue,
            input = Seq.range(0, Random.nextInt(20) max 1),
            delay = Random.nextInt(40)
          )
        }
      }
    }
  }

  "random" in {
    runThis(100.times, log = true) {
      val parallelism = Random.nextInt(20) max 1
      val ranges = Seq.range(1, Random.nextInt(1000) max 1)

      val result =
        Futures.traverseBounded(parallelism = parallelism, input = ranges) {
          item =>
            Future {
              Thread.sleep(Random.nextInt(10))
              item
            }
        }

      result.awaitInf shouldBe ranges
    }
  }
}
