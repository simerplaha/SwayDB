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

package swaydb.cats.effect

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import swaydb.Bag.Async
import swaydb.{IO => SwayIO}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

object Bag {

  /**
   * Cats-effect 3 async bag implementation
   */
  implicit def apply(implicit runtime: IORuntime): swaydb.Bag.Async[IO] =
    new Async[IO] { self =>

      override def executionContext: ExecutionContext =
        runtime.compute

      override val unit: IO[Unit] =
        IO.unit

      override def none[A]: IO[Option[A]] =
        IO.pure(Option.empty)

      override def apply[A](a: => A): IO[A] =
        IO(a)

      override def map[A, B](a: IO[A])(f: A => B): IO[B] =
        a.map(f)

      override def transform[A, B](a: IO[A])(f: A => B): IO[B] =
        a.map(f)

      override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] =
        fa.flatMap(f)

      override def success[A](value: A): IO[A] =
        IO.pure(value)

      override def failure[A](exception: Throwable): IO[A] =
        IO.fromTry(Failure(exception))

      override def foreach[A](a: IO[A])(f: A => Unit): Unit =
        f(a.unsafeRunSync())

      def fromPromise[A](a: Promise[A]): IO[A] =
        IO.fromFuture(IO(a.future))

      override def complete[A](promise: Promise[A], a: IO[A]): Unit =
        promise tryCompleteWith a.unsafeToFuture()

      override def fromIO[E: SwayIO.ExceptionHandler, A](a: SwayIO[E, A]): IO[A] =
        IO.fromTry(a.toTry)

      override def fromFuture[A](a: Future[A]): IO[A] =
        IO.fromFuture(IO(a))

      override def suspend[B](f: => IO[B]): IO[B] =
        IO.defer(f)

      override def flatten[A](fa: IO[IO[A]]): IO[A] =
        fa.flatMap(io => io)
    }
}
