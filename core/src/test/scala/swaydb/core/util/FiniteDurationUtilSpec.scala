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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import org.scalatest.{FlatSpec, Matchers}
import scala.concurrent.duration._
import FiniteDurationUtil._
import java.util.{Timer, TimerTask}

class FiniteDurationUtilSpec extends FlatSpec with Matchers {

  it should "return duration as string" in {
    (1 to 100) foreach {
      i =>
        i.second.asString shouldBe s"$i.0 seconds"
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
        println(timeLeft.asString)

        //timeLeft should be nearly the same
        timeLeft should be <= delay
        timeLeft should be > delay - 10.millisecond

        //also fetch the deadline
        val deadlineDiff = delay.fromNow - task.deadline()
        deadlineDiff should be < 100.millisecond
    }

    timer.cancel()
  }
}
