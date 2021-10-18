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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import swaydb.utils.FiniteDurations._

import java.util.{Timer, TimerTask}
import scala.concurrent.duration._

class FiniteDurationsSpec extends AnyFlatSpec with Matchers {

  it should "return duration as string" in {
    (1 to 100) foreach {
      i =>
        i.second.asString shouldBe s"$i.000000 seconds"
    }
  }

  it should "convert TimeTask to deadline" in {
    val timer = new Timer(true)
    (10 to 20) foreach {
      i =>
        val task =
          new TimerTask {
            def run() =
              ()
          }
        val delay = i.seconds
        timer.schedule(task, delay.toMillis)

        val timeLeft = task.timeLeft()
        //        println(timeLeft.asString)

        //timeLeft should be nearly the same
        timeLeft should be <= delay
        timeLeft should be > delay - 15.millisecond

        //also fetch the deadline
        val deadlineDiff = delay.fromNow - task.deadline()
        deadlineDiff should be < 100.millisecond
    }

    timer.cancel()
  }

  it should "return None for empty deadlines" in {
    FiniteDurations.getNearestDeadline(None, None) shouldBe empty
  }

  it should "return earliest deadline" in {
    val deadline1 = 10.seconds.fromNow
    val deadline2 = 20.seconds.fromNow

    FiniteDurations.getNearestDeadline(Some(deadline1), None) should contain(deadline1)
    FiniteDurations.getNearestDeadline(Some(deadline2), None) should contain(deadline2)

    FiniteDurations.getNearestDeadline(None, Some(deadline1)) should contain(deadline1)
    FiniteDurations.getNearestDeadline(None, Some(deadline2)) should contain(deadline2)

    FiniteDurations.getNearestDeadline(Some(deadline1), Some(deadline1)) should contain(deadline1)
    FiniteDurations.getNearestDeadline(Some(deadline2), Some(deadline2)) should contain(deadline2)

    FiniteDurations.getNearestDeadline(Some(deadline1), Some(deadline2)) should contain(deadline1)
    FiniteDurations.getNearestDeadline(Some(deadline2), Some(deadline1)) should contain(deadline1)
  }

  it should "return furthest deadline" in {
    val deadline1 = 10.seconds.fromNow
    val deadline2 = 20.seconds.fromNow

    FiniteDurations.getFurthestDeadline(Some(deadline1), None) should contain(deadline1)
    FiniteDurations.getFurthestDeadline(Some(deadline2), None) should contain(deadline2)

    FiniteDurations.getFurthestDeadline(None, Some(deadline1)) should contain(deadline1)
    FiniteDurations.getFurthestDeadline(None, Some(deadline2)) should contain(deadline2)

    FiniteDurations.getFurthestDeadline(Some(deadline1), Some(deadline1)) should contain(deadline1)
    FiniteDurations.getFurthestDeadline(Some(deadline2), Some(deadline2)) should contain(deadline2)

    FiniteDurations.getFurthestDeadline(Some(deadline1), Some(deadline2)) should contain(deadline2)
    FiniteDurations.getFurthestDeadline(Some(deadline2), Some(deadline1)) should contain(deadline2)
  }
}
