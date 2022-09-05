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

package swaydb.zio

import swaydb.Bag.Async
import zio.{Task, Unsafe, ZIO}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

object Bag {

  implicit def apply[T](implicit runTime: zio.Runtime[T],
                        ec: ExecutionContext): swaydb.Bag.Async[Task] =
    new Async[Task] { self =>

      override def executionContext: ExecutionContext =
        ec

      override val unit: Task[Unit] =
        ZIO.unit

      override def none[A]: Task[Option[A]] =
        ZIO.none

      override def apply[A](a: => A): Task[A] =
        ZIO.attempt(a)

      override def map[A, B](a: Task[A])(f: A => B): Task[B] =
        a.map(f)

      override def transform[A, B](a: Task[A])(f: A => B): Task[B] =
        a.map(f)

      override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] =
        fa.flatMap(f)

      override def success[A](value: A): Task[A] =
        ZIO.succeed(value)

      override def failure[A](exception: Throwable): Task[A] =
        ZIO.fromTry(Failure(exception))

      override def foreach[A](a: Task[A])(f: A => Unit): Unit =
        Unsafe.unsafe {
          implicit unsafe =>
            f(runTime.unsafe.run(a).getOrThrowFiberFailure())
        }

      def fromPromise[A](a: Promise[A]): Task[A] =
        ZIO.fromFuture(_ => a.future)

      override def complete[A](promise: Promise[A], task: Task[A]): Unit =
        Unsafe.unsafe {
          implicit unsafe =>
            promise.tryCompleteWith(runTime.unsafe.runToFuture(task))
        }

      override def fromIO[E: swaydb.IO.ExceptionHandler, A](a: swaydb.IO[E, A]): Task[A] =
        ZIO.fromTry(a.toTry)

      override def fromFuture[A](a: Future[A]): Task[A] =
        ZIO.fromFuture(_ => a)

      override def suspend[B](f: => Task[B]): Task[B] =
        ZIO.suspend(f)

      override def flatten[A](fa: Task[Task[A]]): Task[A] =
        fa.flatten
    }
}
