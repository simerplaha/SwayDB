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

package swaydb.testkit

import org.scalatest.concurrent.Eventually

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

object RunThis extends Eventually {

  @inline def round(double: Double, scale: Int = 6): BigDecimal =
    BigDecimal(double).setScale(scale, BigDecimal.RoundingMode.HALF_UP)

  implicit class RunThisImplicits[R](f: => R) {
    def runThis(times: Int): Unit =
      for (i <- 1 to times) f
  }

  implicit class RunParallelSeq[T](input: Iterable[() => T]) {
    def runThisRandomlyInParallel =
      Random.shuffle(input).par.foreach(_ ())

    def runThisRandomly =
      Random.shuffle(input).foreach(_ ())

    def runThisRandomlyValue: Iterable[T] =
      Random.shuffle(input).map(_ ())
  }

  implicit class FiniteDurationImplicits(duration: Duration) {

    @inline final def asString: String =
      asString(scale = 6)

    @inline final def asString(scale: Int): String = {
      val seconds: Double = duration.toMillis / 1000D
      val scaledSeconds = RunThis.round(seconds, scale)
      s"$scaledSeconds seconds"
    }
  }

  implicit class FutureImplicits[T](f: => Future[T]) {
    def runThis(times: Int)(implicit ec: ExecutionContext): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures =
        Range.inclusive(1, times) map {
          _ =>
            f
        }
      Future.sequence(futures)
    }

    def await(timeout: FiniteDuration): T =
      Await.result(f, timeout)

    def await: T =
      Await.result(f, 1.second)

    def awaitInf: T =
      Await.result(f, Duration.Inf)

    def awaitFailureInf: Throwable =
      try {
        val result = Await.result(f, Duration.Inf)
        throw new Exception(s"Expected failure. Actual success: $result")
      } catch {
        case throwable: Throwable =>
          throwable
      }
  }

  implicit class FutureAwait2[T](f: => T)(implicit ec: ExecutionContext) {
    def runThisInFuture(times: Int): Future[Seq[T]] = {
      println(s"runThis $times times")
      val futures = Range.inclusive(1, times) map { _ => Future(f) }
      Future.sequence(futures)
    }
  }

  val once = 1

  implicit class TimesImplicits(int: Int) {
    def times = int

    def time = int
  }

  def runThis(times: Int, log: Boolean = false, otherInfo: String = "")(f: => Unit): Unit =
    (1 to times) foreach {
      i =>
        if (log) println(s"Iteration: $i${if (otherInfo.nonEmpty) s": $otherInfo" else ""}/$times")
        f
    }

  def runThisParallel(times: Int, log: Boolean = false, otherInfo: String = "")(f: => Unit)(implicit ec: ExecutionContext): Unit = {
    val futures =
      (1 to times) map {
        i =>
          Future {
            if (log) println(s"Iteration: $i${if (otherInfo.nonEmpty) s": $otherInfo" else ""}/$times")
            f
          }
      }

    Future.sequence(futures).await(10.seconds)
  }

  def sleep(time: FiniteDuration): Unit = {
    println(s"Sleeping for: ${time.asString}")
    if (time.fromNow.hasTimeLeft()) Thread.sleep(time.toMillis)
    println(s"Up from sleep: ${time.asString}")
  }

  def sleep(time: Deadline): Unit =
    if (time.hasTimeLeft()) sleep(time.timeLeft)

  def eventual[A](code: => A): Unit =
    eventual()(code)

  def eventual[A](after: FiniteDuration = 1.second)(code: => A): Unit =
    eventually(timeout(after))(code)
}
