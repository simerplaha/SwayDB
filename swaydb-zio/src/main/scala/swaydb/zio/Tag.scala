/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.zio

import swaydb.Tag.Async
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.{Actor, Serial}
import zio.Task

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Try}

object Tag {

  implicit object ZIOTaskMonad extends swaydb.Monad[Task] {
    override def map[A, B](a: A, f: A => B): Task[B] =
      Task(f(a))

    override def flatMap[A, B](a: Task[A], f: A => Task[B]): Task[B] =
      a.flatMap(f)

    override def success[A](a: A): Task[A] =
      Task.succeed(a)

    override def failed[A](a: Throwable): Task[A] =
      Task.fromTry(scala.util.Failure(a))
  }

  implicit def apply[T](implicit runTime: zio.Runtime[T]): swaydb.Tag.Async[Task] =
    new Async[Task] {

      override def executionContext: ExecutionContext =
        runTime.platform.executor.asEC

      override def createSerial(): Serial[Task] =
        new Serial[Task] {

          /**
           * May be use zio.Actor instead since fromFuture is ignoring the ec.
           */
          val actor = Actor[() => Any] {
            (run, _) =>
              run()
          }(executionContext, QueueOrder.FIFO)

          override def execute[F](f: => F): Task[F] = {
            val promise = Promise[F]
            actor.send(() => promise.tryComplete(Try(f)))
            //ok may be zio.Actor is required here.
            Task.fromFuture(_ => promise.future)
          }
        }

      override val unit: Task[Unit] =
        Task.unit

      override def none[A]: Task[Option[A]] =
        Task.none

      override def apply[A](a: => A): Task[A] =
        Task(a)

      override def map[A, B](a: Task[A])(f: A => B): Task[B] =
        a.map(f)

      override def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] =
        fa.flatMap(f)

      override def success[A](value: A): Task[A] =
        Task.succeed(value)

      override def failure[A](exception: Throwable): Task[A] =
        Task.fromTry(Failure(exception))

      override def foreach[A](a: Task[A])(f: A => Unit): Unit =
        f(runTime.unsafeRun(a))

      def fromPromise[A](a: Promise[A]): Task[A] =
        Task.fromFuture(_ => a.future)

      override def complete[A](promise: Promise[A], task: Task[A]): Unit =
        promise.tryCompleteWith(runTime.unsafeRunToFuture(task))

      override def foldLeft[A, U](initial: U, after: Option[A], stream: swaydb.Stream[A, Task], drop: Int, take: Option[Int])(operation: (U, A) => U): Task[U] =
        swaydb.Tag.Async.foldLeft(
          initial = initial,
          after = after,
          stream = stream,
          drop = drop,
          take = take,
          operation = operation
        )

      override def collectFirst[A](previous: A, stream: swaydb.Stream[A, Task])(condition: A => Boolean): Task[Option[A]] =
        swaydb.Tag.Async.collectFirst(
          previous = previous,
          stream = stream,
          condition = condition
        )

      override def fromIO[E: swaydb.IO.ExceptionHandler, A](a: swaydb.IO[E, A]): Task[A] =
        Task.fromTry(a.toTry)

      override def fromFuture[A](a: Future[A]): Task[A] =
        Task.fromFuture(_ => a)
    }
}
