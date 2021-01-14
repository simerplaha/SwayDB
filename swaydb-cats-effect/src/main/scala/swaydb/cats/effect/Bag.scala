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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.cats.effect

import cats.effect.{ContextShift, IO}
import swaydb.Bag.Async
import swaydb.{IO => SwayIO}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure

object Bag {

  /**
   * Async tag for Cats-effect's IO.
   */
  implicit def apply(implicit contextShift: ContextShift[IO],
                     ec: ExecutionContext): swaydb.Bag.Async[IO] =
    new Async[IO] { self =>

      override def executionContext: ExecutionContext =
        ec

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
        IO.suspend(f)

      override def flatten[A](fa: IO[IO[A]]): IO[A] =
        fa.flatMap(io => io)

    }
}
