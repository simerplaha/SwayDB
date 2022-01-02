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

package swaydb

import swaydb.Bag.AsyncRetryable

import scala.concurrent.{ExecutionContext, Future, Promise}

case class BagFuture()(implicit ec: ExecutionContext) extends AsyncRetryable[Future] { self =>
  @inline override def executionContext: ExecutionContext =
    ec

  override val unit: Future[Unit] =
    Future.unit

  @inline override def none[A]: Future[Option[A]] =
    Future.successful(None)

  @inline override def apply[A](a: => A): Future[A] =
    Future(a)

  @inline override def map[A, B](a: Future[A])(f: A => B): Future[B] =
    a.map(f)

  @inline override def transform[A, B](a: Future[A])(f: A => B): Future[B] =
    a.transform(f, throwable => throwable)

  @inline override def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] =
    fa.flatMap(f)

  @inline override def success[A](value: A): Future[A] =
    Future.successful(value)

  @inline override def failure[A](exception: Throwable): Future[A] =
    Future.failed(exception)

  @inline override def foreach[A](a: Future[A])(f: A => Unit): Unit =
    a.foreach(f)

  @inline def fromPromise[A](a: Promise[A]): Future[A] =
    a.future

  @inline override def complete[A](promise: Promise[A], a: Future[A]): Unit =
    promise tryCompleteWith a

  @inline def isComplete[A](a: Future[A]): Boolean =
    a.isCompleted

  @inline override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): Future[A] =
    a.toFuture

  @inline override def fromFuture[A](a: Future[A]): Future[A] =
    a

  @inline override def suspend[B](f: => Future[B]): Future[B] =
    f

  @inline override def flatten[A](fa: Future[Future[A]]): Future[A] =
    fa.flatten
}
