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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util

import java.util.concurrent.atomic.AtomicInteger

import swaydb.IO
import swaydb.core.util.series.SeriesVolatile

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag

object ParallelCollection {

  implicit class ParallelCollectionImplicits[A: ClassTag, E: IO.ExceptionHandler](iterable: Iterable[A]) {

    @inline def mapParallel[B >: Null](parallelism: Int,
                                       timeout: FiniteDuration)(block: A => IO[E, B],
                                                                recover: (Iterable[B], IO.Left[E, Iterable[B]]) => Unit = (_: Iterable[B], _: IO.Left[E, Iterable[B]]) => (),
                                                                failFast: Boolean = true)(implicit ec: ExecutionContext): IO[E, Iterable[B]] =
      ParallelCollection.mapParallel[A, B, E](
        iterable = iterable,
        parallelism = parallelism,
        timeout = timeout
      )(block = block, recover = recover, failFast = failFast)

  }

  /**
   * Null is used internally. Outcome [[B]] cannot be null.
   */
  def mapParallel[A: ClassTag, B >: Null, E: IO.ExceptionHandler](iterable: Iterable[A],
                                                                  parallelism: Int,
                                                                  timeout: FiniteDuration)(block: A => IO[E, B],
                                                                                           recover: (Iterable[B], IO.Left[E, Iterable[B]]) => Unit = (_: Iterable[B], _: IO.Left[E, Iterable[B]]) => (),
                                                                                           failFast: Boolean = true)(implicit ec: ExecutionContext): IO[E, Iterable[B]] = {
    val jobsCount = iterable.size

    if (jobsCount <= 1 || parallelism <= 1) {
      val series = SeriesVolatile[B](jobsCount)

      iterable.foldLeft(0) {
        case (index, job) =>
          block(job) match {
            case IO.Right(outcome) =>
              series.set(index, outcome)
              index + 1

            case IO.Left(error) =>
              val left = IO.Left[E, SeriesVolatile[B]](error)
              recover(series.filter(_ != null), left)
              return left
          }
      }

      IO.Right(series.filter(_ != null))

    } else {
      @volatile var failure: IO.Left[E, SeriesVolatile[B]] = null

      val jobs: Array[A] = new Array[A](jobsCount)

      iterable.foldLeft(0) {
        case (index, item) =>
          jobs(index) = item
          index + 1
      }

      val jobIndex = new AtomicInteger(0)

      val series = SeriesVolatile[B](jobsCount)

      @tailrec
      def processJobs(index: Int): Unit =
        if (index < jobs.length)
          block(jobs(index)) match {
            case IO.Right(jobResult) =>
              series.set(index, jobResult)

              val stop = failFast && failure != null

              if (!stop) {
                val nextJob = jobIndex.getAndIncrement()

                if (nextJob < jobs.length)
                  processJobs(nextJob)
              }

            case IO.Left(value) =>
              failure = IO.Left(value)
          }

      @tailrec
      def runParallel(futures: ListBuffer[Future[Unit]], remaining: Int): ListBuffer[Future[Unit]] =
        if (remaining == 0) {
          futures
        } else {
          val nextJob = jobIndex.getAndIncrement()
          if (nextJob >= jobs.length)
            futures
          else
            runParallel(
              futures = futures += Future(processJobs(nextJob)),
              remaining = remaining - 1
            )
        }

      //leave enough work for current thread to process.
      val parallelRuns = parallelism min (jobsCount - 1)
      //process jobs concurrent
      val parallelJobs = runParallel(futures = ListBuffer.empty, remaining = parallelRuns)
      //also keep this thread busy while there are still jobs remaining.
      processJobs(jobIndex.getAndIncrement())

      try
        Await.result(Future.sequence(parallelJobs), timeout)
      catch {
        case throwable: Throwable =>
          failure = IO.failed(throwable)
      }

      val nonNullSeries = series.filter(_ != null)

      if (failure != null) {
        recover(nonNullSeries, failure)
        failure
      } else {
        IO.Right(nonNullSeries)
      }
    }
  }
}
