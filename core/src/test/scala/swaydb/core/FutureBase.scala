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

package swaydb.core

import org.scalatest.concurrent.Eventually

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}
import swaydb.core.util.FiniteDurationUtil._

trait FutureBase extends Eventually {

  implicit val level0PushDownPool = TestExecutionContext.executionContext

  def sleep(time: FiniteDuration): Unit = {
    println(s"Sleeping for: ${time.asString}")
    Thread.sleep(time.toMillis)
    println(s"Up from sleep: ${time.asString}")
  }

  def sleep(time: Deadline): Unit =
    if (time.hasTimeLeft()) sleep(time.timeLeft)

  def eventual[A](code: => A): Unit =
    eventual()(code)

  def eventual[A](after: FiniteDuration = 1.second)(code: => A): Unit =
    eventually(timeout(after))(code)

  //TODO duplicate with TestBase. Extract these to a common class
  implicit class WithTimesTimesImplicits(int: Int) {
    def times = int

    def time = int
  }

  implicit class FutureAwait[T](f: => Future[T]) {
    def await(timeout: FiniteDuration): T =
      Await.result(f, timeout)

    def await: T =
      Await.result(f, 1.second)

    def runThis(times: Int): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures = Range.inclusive(1, times) map (_ => f)
      Future.sequence(futures)
    }
  }

  implicit class FutureAwait2[T](f: => T) {
    def runThisInFuture(times: Int): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures = Range.inclusive(1, times) map { _ => Future(f) }
      Future.sequence(futures)
    }
  }

}
