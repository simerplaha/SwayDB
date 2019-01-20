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

package swaydb.core

import org.scalatest.concurrent.Eventually
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import swaydb.core.util.FiniteDurationUtil._

object RunThis extends Eventually {

  implicit class RunThisImplicits[R](f: => R) {
    def runThis(times: Int): Unit =
      for (i <- 1 to times) f
  }

  implicit class FutureImplicits[T](f: => Future[T])(implicit ec: ExecutionContext) {
    def runThis(times: Int): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures =
        Range.inclusive(1, times).map {
          _ =>
            f
        }
      Future.sequence(futures)
    }

    def await(timeout: FiniteDuration): T =
      Await.result(f, timeout)

    def await: T =
      Await.result(f, 1.second)
  }

  val once = 1

  implicit class TimesImplicits(int: Int) {
    def times = int

    def time = int
  }

  def runThis(times: Int)(f: => Unit): Unit =
    (1 to times) foreach {
      i =>
//        println(s"Iteration number: $i")
        f
    }

  def runThisParallel(times: Int)(f: => Unit): Unit =
    (1 to times).par foreach {
      i =>
//        println(s"Iteration number: $i")
        f
//        println(s"Iteration done  : $i")
    }

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

  implicit class FutureAwait2[T](f: => T) {
    def runThisInFuture(times: Int): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures = Range.inclusive(1, times) map { _ => Future(f) }
      Future.sequence(futures)
    }
  }

}
