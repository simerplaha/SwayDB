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

package swaydb.monix

import monix.eval._
import swaydb.Bag.Async
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.{Actor, IO, Serial}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Try}

object Bag {

  implicit object MonixTaskMonad extends swaydb.Monad[Task] {
    override def map[A, B](a: A, f: A => B): Task[B] =
      Task(f(a))

    override def flatMap[A, B](a: Task[A], f: A => Task[B]): Task[B] =
      a.flatMap(f)

    override def success[A](a: A): Task[A] =
      Task.now(a)

    override def failed[A](a: Throwable): Task[A] =
      Task.fromTry(scala.util.Failure(a))
  }

  implicit def apply(implicit scheduler: monix.execution.Scheduler): swaydb.Bag.Async[Task] =
    new Async[Task] {

      implicit val self: swaydb.Bag.Async[Task] = this

      override def executionContext: ExecutionContext =
        scheduler

      override def createSerial(): Serial[Task] =
        new Serial[Task] {

          /**
           * If there another preferred way to execute tasks serially in Monix instead of using an Actor
           * that should be used here.
           */
          val actor = Actor[() => Any] {
            (run, _) =>
              run()
          }(scheduler, QueueOrder.FIFO)

          override def execute[F](f: => F): Task[F] = {
            val promise = Promise[F]
            actor.send(() => promise.tryComplete(Try(f)))
            Task.fromFuture(promise.future)
          }
        }

      override val unit: Task[Unit] =
        Task.unit

      override def none[A]: Task[Option[A]] =
        Task.now(Option.empty)

      override def apply[A](a: => A): Task[A] =
        Task(a)

      override def map[A, B](a: Task[A])(f: A => B): Task[B] =
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

      override def foldLeft[A, U](initial: U, after: Option[A], stream: swaydb.Stream[A], drop: Int, take: Option[Int])(operation: (U, A) => U): Task[U] =
        swaydb.Bag.Async.foldLeft(
          initial = initial,
          after = after,
          stream = stream,
          drop = drop,
          take = take,
          operation = operation
        )

      override def collectFirst[A](previous: A, stream: swaydb.Stream[A])(condition: A => Boolean): Task[Option[A]] =
        swaydb.Bag.Async.collectFirst(
          previous = previous,
          stream = stream,
          condition = condition
        )

      override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): Task[A] =
        Task.fromTry(a.toTry)

      override def fromFuture[A](a: Future[A]): Task[A] =
        Task.fromFuture(a)
    }
}
