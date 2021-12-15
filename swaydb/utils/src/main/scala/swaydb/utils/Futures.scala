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

package swaydb.utils

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.compat._
import scala.concurrent.{ExecutionContext, Future}

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
      val items: ConcurrentLinkedQueue[(A, Int)] = toZipWithIndexQueue(input) //queue all the items
      val resultQueue = new ConcurrentLinkedQueue[(Future[B], Int)]() //final result
      val remaining = new AtomicInteger(parallelism) //remaining parallelism left
      @volatile var failed: Throwable = null //indicates if the traverse has failed

      def run(previousFuture: Future[Unit]): Future[Unit] =
        if (failed != null) {
          Future.failed(failed)
        } else if (remaining.decrementAndGet() < 0) { //attempt at reserving execution
          remaining.incrementAndGet() //if it goes negative then fix this error by reverting the decrement
          previousFuture
        } else {
          val next = items.poll()
          if (next != null) {
            val (nextItem, nextItemIndex) = next
            val executedFuture = f(nextItem)
            resultQueue.add((executedFuture, nextItemIndex))

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
          toSortedValues(resultQueue)
            .foldLeft(Future.successful(bf.newBuilder(input))) {
              case (builder, future) =>
                builder.zipWith(future)(_ += _)
            }.map(_.result())
        }
    }

  /**
   * Creates zipWithIndex queue form iterables.
   */
  def toZipWithIndexQueue[A](iterable: IterableOnce[A]): ConcurrentLinkedQueue[(A, Int)] = {
    val queue = new ConcurrentLinkedQueue[(A, Int)]()
    val iterator = iterable.iterator
    var index = 0
    while (iterator.hasNext) {
      queue.add((iterator.next(), index))
      index += 1
    }

    queue
  }

  /**
   * Given the above [[toZipWithIndexQueue]] this returns the values of type
   * [[A]] in the order of the index.
   */
  def toSortedValues[A](queue: ConcurrentLinkedQueue[(A, Int)]): Iterable[A] = {
    val sortedMap = scala.collection.mutable.SortedMap[Int, A]()

    queue forEach {
      case (item, index) =>
        sortedMap.put(index, item)
    }

    sortedMap.values
  }
}
