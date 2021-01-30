/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.data.util

import java.util.{Timer, TimerTask}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import swaydb.data.util.FiniteDurations._

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
