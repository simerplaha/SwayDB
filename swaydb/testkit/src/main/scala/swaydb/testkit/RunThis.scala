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
    def runThisRandomlyInParallel() =
      Random.shuffle(input).par.foreach(_ ())

    def runThisRandomly() =
      Random.shuffle(input).foreach(_ ())

    def runThisRandomlyValue(): Iterable[T] =
      Random.shuffle(input).map(_ ())
  }

  implicit class RunThisFiniteDurationImplicits(duration: Duration) {

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
      val futures = Range.inclusive(1, times).map(_ => Future(f))
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

  def runThisNumbered(times: Int, log: Boolean = false, otherInfo: String = "")(f: Int => Unit): Unit =
    (1 to times) foreach {
      i =>
        if (log) println(s"Iteration: $i${if (otherInfo.nonEmpty) s": $otherInfo" else ""}/$times")
        f(i)
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
