/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.utils

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.compat.BuildFrom
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

private[swaydb] object Futures {

  val none = Future.successful(None)
  val unit: Future[Unit] = Future.successful(())
  val `true` = Future.successful(true)
  val rightUnit: Right[Nothing, Future[Unit]] = Right(unit)
  val rightUnitFuture: Future[Right[Nothing, Future[Unit]]] = Future.successful(rightUnit)
  val `false` = Future.successful(false)
  val emptyIterable = Future.successful(Iterable.empty)

  implicit class FutureImplicits[A](future1: Future[A]) {
    @inline def and[B](future2: => Future[B])(implicit executionContext: ExecutionContext): Future[B] =
      future1.flatMap(_ => future2)

    @inline def join[B](future2: => Future[B])(implicit executionContext: ExecutionContext): Future[(A, B)] =
      future1 flatMap {
        a =>
          future2 map {
            b =>
              (a, b)
          }
      }

    @inline def onError[B >: A](result: => B)(implicit executionContext: ExecutionContext): Future[B] =
      future1.recover {
        case _ =>
          result
      }

    @inline def unitCallback(onComplete: => Unit)(implicit executionContext: ExecutionContext): Future[A] = {
      future1.onComplete(_ => onComplete)
      future1
    }

    @inline def flatMapCallback(future2: => Future[Unit])(implicit executionContext: ExecutionContext): Future[A] =
      future1 flatMap {
        onesResult =>
          future2.map(_ => onesResult)
      }
  }

  implicit class FutureUnitImplicits(future1: Future[Unit]) {
    @inline def flatMapUnit[A](future2: => Future[A])(implicit executionContext: ExecutionContext): Future[A] =
      future1.flatMap(_ => future2)

    @inline def mapUnit[A](future2: => A)(implicit executionContext: ExecutionContext): Future[A] =
      future1.map(_ => future2)
  }

  def traverseBounded[A, B, C[X] <: Iterable[X]](parallelism: Int,
                                                 input: C[A])(f: A => Future[B])(implicit bf: BuildFrom[C[A], B, C[B]], ec: ExecutionContext): Future[C[B]] =
    if (parallelism <= 0) {
      Future.failed(new Exception(s"Invalid parallelism $parallelism. Should be >= 1."))
    } else {
      val items = new ConcurrentLinkedQueue[A](input.asJavaCollection) //queue all the items
      val resultQueue = new ConcurrentLinkedQueue[Future[B]]() //final result
      val remaining = new AtomicInteger(parallelism) //remaining parallelism left
      @volatile var failed: Throwable = null //indicates if the traverse has failed

      def run(previousFuture: Future[Unit]): Future[Unit] =
        if (failed != null) {
          Future.failed(failed)
        } else if (remaining.decrementAndGet() < 0) { //attempt at reserving execution
          remaining.incrementAndGet() //if it goes negative then fix this error by reverting the decrement
          previousFuture
        } else {
          val nextItem = items.poll()
          if (nextItem != null) {
            val executedFuture = f(nextItem)
            resultQueue add executedFuture

            val nextFuture =
              executedFuture
                .flatMap {
                  _ =>
                    //return this parallelism before continuing
                    remaining.incrementAndGet()
                    run(Future.unit)
                }
                .recoverWith {
                  case throwable =>
                    failed = throwable
                    Future.failed(throwable)
                }

            //call run again and chain tail previous and nextFuture
            run(previousFuture.and(nextFuture))
          } else if (failed != null) {
            Future.failed(failed)
          } else {
            previousFuture
          }
        }

      run(Future.unit)
        .and {
          resultQueue
            .asScala
            .foldLeft(Future.successful(bf.newBuilder(input))) {
              case (builder, future) =>
                builder.zipWith(future)(_ += _)
            }.map(_.result())
        }
    }
}
