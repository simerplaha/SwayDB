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

package swaydb.monix

import monix.eval._
import swaydb.Bag.Async
import swaydb.IO

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

object Bag {

  implicit def apply(implicit scheduler: monix.execution.Scheduler): swaydb.Bag.Async[Task] =
    new Async[Task] { self =>

      override def executionContext: ExecutionContext =
        scheduler

      override val unit: Task[Unit] =
        Task.unit

      override def none[A]: Task[Option[A]] =
        Task.now(Option.empty)

      override def apply[A](a: => A): Task[A] =
        Task(a)

      override def map[A, B](a: Task[A])(f: A => B): Task[B] =
        a.map(f)

      override def transform[A, B](a: Task[A])(f: A => B): Task[B] =
        a.map(f)

      override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] =
        fa.flatMap(f)

      override def success[A](value: A): Task[A] =
        Task.now(value)

      override def failure[A](exception: Throwable): Task[A] =
        Task.fromTry(Failure(exception))

      override def foreach[A](a: Task[A])(f: A => Unit): Unit =
        a.foreach(f)

      def fromPromise[A](a: Promise[A]): Task[A] =
        Task.fromFuture(a.future)

      override def complete[A](promise: Promise[A], a: Task[A]): Unit =
        promise tryCompleteWith a.runToFuture

      override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): Task[A] =
        Task.fromTry(a.toTry)

      override def fromFuture[A](a: Future[A]): Task[A] =
        Task.fromFuture(a)

      override def suspend[B](f: => Task[B]): Task[B] =
        Task.suspend(f)

      override def flatten[A](fa: Task[Task[A]]): Task[A] =
        fa.flatten
    }
}
