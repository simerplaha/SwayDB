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
 */

package swaydb.cats.effect

import swaydb.Tag.Async
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.{Actor, Serial}
import swaydb.{IO => SwayIO}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Try}
import cats.effect.{ContextShift, IO}

object Tag {

  implicit object CatsEffectIOMonad extends swaydb.Monad[IO] {
    override def map[A, B](a: A, f: A => B): IO[B] =
      IO(f(a))

    override def flatMap[A, B](a: IO[A], f: A => IO[B]): IO[B] =
      a.flatMap(f)

    override def success[A](a: A): IO[A] =
      IO.pure(a)

    override def failed[A](a: Throwable): IO[A] =
      IO.fromTry(scala.util.Failure(a))
  }

  /**
   * Async tag for Cats-effect's IO.
   */
  implicit def apply(implicit contextShift: ContextShift[IO],
                     ec: ExecutionContext): swaydb.Tag.Async[IO] =
    new Async[IO] {

      override def executionContext: ExecutionContext =
        ec

      override def createSerial(): Serial[IO] =
        new Serial[IO] {
          val actor = Actor[() => Any] {
            (run, _) =>
              run()
          }(ec, QueueOrder.FIFO)

          override def execute[F](f: => F): IO[F] = {
            val promise = Promise[F]
            actor.send(() => promise.tryComplete(Try(f)))
            IO.fromFuture(IO(promise.future))
          }
        }

      override val unit: IO[Unit] =
        IO.unit

      override def none[A]: IO[Option[A]] =
        IO.pure(Option.empty)

      override def apply[A](a: => A): IO[A] =
        IO(a)

      override def map[A, B](a: IO[A])(f: A => B): IO[B] =
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

      override def foldLeft[A, U](initial: U, after: Option[A], stream: swaydb.Stream[A, IO], drop: Int, take: Option[Int])(operation: (U, A) => U): IO[U] =
        swaydb.Tag.Async.foldLeft(
          initial = initial,
          after = after,
          stream = stream,
          drop = drop,
          take = take,
          operation = operation
        )

      override def collectFirst[A](previous: A, stream: swaydb.Stream[A, IO])(condition: A => Boolean): IO[Option[A]] =
        swaydb.Tag.Async.collectFirst(
          previous = previous,
          stream = stream,
          condition = condition
        )

      override def fromIO[E: SwayIO.ExceptionHandler, A](a: SwayIO[E, A]): IO[A] =
        IO.fromTry(a.toTry)

      override def fromFuture[A](a: Future[A]): IO[A] =
        IO.fromFuture(IO(a))
    }
}
