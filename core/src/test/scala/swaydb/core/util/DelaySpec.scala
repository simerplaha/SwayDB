/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class DelaySpec extends WordSpec with Matchers {

  "Delay.cancelTimer" should {
    "cancel all existing scheduled tasks" in {
      @volatile var tasksExecuted = 0

      //create 5 tasks
      Delay.future(1.second)(tasksExecuted += 1)
      Delay.future(1.second)(tasksExecuted += 1)
      Delay.future(1.second)(tasksExecuted += 1)
      Delay.future(2.second)(tasksExecuted += 1)
      Delay.future(2.second)(tasksExecuted += 1)

      //after 1.5 seconds cancel timer
      Thread.sleep(1.5.seconds.toMillis)
      Delay.cancelTimer()

      //the remaining two tasks did not get executed.
      tasksExecuted shouldBe 3

      //after 2.seconds the remaining two tasks are still not executed.
      Thread.sleep(2.seconds.toMillis)
      tasksExecuted shouldBe 3
    }
  }
}