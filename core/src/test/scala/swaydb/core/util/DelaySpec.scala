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

package swaydb.core.util

import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import swaydb.IO
import swaydb.Error.Segment.ErrorHandler

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DelaySpec extends WordSpec with Matchers with Eventually {

  //  "Delay.cancelTimer" should {
  //    "cancel all existing scheduled tasks" in {
  //      @volatile var tasksExecuted = 0
  //
  //      //create 5 tasks
  //      Delay.future(1.second)(tasksExecuted += 1)
  //      Delay.future(1.second)(tasksExecuted += 1)
  //      Delay.future(1.second)(tasksExecuted += 1)
  //      Delay.future(2.second)(tasksExecuted += 1)
  //      Delay.future(2.second)(tasksExecuted += 1)
  //
  //      //after 1.5 seconds cancel timer
  //      Thread.sleep(1.5.seconds.toMillis)
  ////      Delay.cancelTimer()
  //
  //      //the remaining two tasks did not value executed.
  //      tasksExecuted shouldBe 3
  //
  //      //after 2.seconds the remaining two tasks are still not executed.
  //      Thread.sleep(2.seconds.toMillis)
  //      tasksExecuted shouldBe 3
  //    }
  //  }

  "Delay.task" should {
    "run tasks and cancel tasks" in {
      @volatile var tasksExecuted = 0

      Delay.task(1.seconds)(tasksExecuted += 1)
      Delay.task(2.seconds)(tasksExecuted += 1)
      Delay.task(3.seconds)(tasksExecuted += 1)
      Delay.task(4.seconds)(tasksExecuted += 1)
      Delay.task(5.seconds)(tasksExecuted += 1)

      eventually(timeout(8.seconds)) {
        tasksExecuted shouldBe 5
      }

      Delay.task(1.seconds)(tasksExecuted += 1).cancel()
      Delay.task(1.seconds)(tasksExecuted += 1)

      Thread.sleep(5.seconds.toMillis)

      tasksExecuted shouldBe 6
    }
  }

  "futureFromIO" should {
    "run in future and return result" in {
      @volatile var tryThread = ""

      Delay.futureFromIO(100.millisecond)(IO(tryThread = Thread.currentThread().getName))

      val currentThread = Thread.currentThread().getName

      eventually(timeout(2.seconds)) {
        tryThread should not be empty
        tryThread should not be currentThread
      }
    }
  }

  "future" should {
    "run in future" in {
      @volatile var futureThread = ""

      Delay.future(100.millisecond)(futureThread = Thread.currentThread().getName)

      val currentThread = Thread.currentThread().getName

      eventually(timeout(2.seconds)) {
        futureThread should not be empty
        futureThread should not be currentThread
      }
    }
  }
}
