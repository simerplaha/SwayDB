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

package swaydb.core.util

import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.{TestCaseSweeper, TestExecutionContext}
import swaydb.{IO, Scheduler}

import scala.concurrent.duration._
import TestCaseSweeper._

class SchedulerSpec extends AnyWordSpec with Matchers with Eventually {

  implicit val ec = TestExecutionContext.executionContext

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
      TestCaseSweeper {
        implicit sweeper =>

          @volatile var tasksExecuted = 0

          val scheduler = Scheduler().sweep()

          scheduler.task(1.seconds)(tasksExecuted += 1)
          scheduler.task(2.seconds)(tasksExecuted += 1)
          scheduler.task(3.seconds)(tasksExecuted += 1)
          scheduler.task(4.seconds)(tasksExecuted += 1)
          scheduler.task(5.seconds)(tasksExecuted += 1)

          eventually(timeout(8.seconds)) {
            tasksExecuted shouldBe 5
          }

          scheduler.task(1.seconds)(tasksExecuted += 1).cancel()
          scheduler.task(1.seconds)(tasksExecuted += 1)

          Thread.sleep(5.seconds.toMillis)

          tasksExecuted shouldBe 6
      }
    }
  }

  "futureFromIO" should {
    "run in future and return result" in {
      TestCaseSweeper {
        implicit sweeper =>
          @volatile var tryThread = ""

          val scheduler = Scheduler().sweep()

          scheduler.futureFromIO(100.millisecond) {
            IO {
              tryThread = Thread.currentThread().getName
            }
          }

          val currentThread = Thread.currentThread().getName

          eventually(timeout(2.seconds)) {
            tryThread should not be empty
            tryThread should not be currentThread
          }
      }
    }
  }

  "future" should {
    "run in future" in {
      TestCaseSweeper {
        implicit sweeper =>
          @volatile var futureThread = ""

          val scheduler = Scheduler().sweep()

          scheduler.future(100.millisecond) {
            futureThread = Thread.currentThread().getName
          }

          val currentThread = Thread.currentThread().getName

          eventually(timeout(2.seconds)) {
            futureThread should not be empty
            futureThread should not be currentThread
          }
      }
    }
  }
}
