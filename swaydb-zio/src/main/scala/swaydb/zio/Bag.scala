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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.zio

import swaydb.Bag.Async
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.{Actor, Bag, Serial}
import zio.Task

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Try}

object Bag {

  implicit def apply[T](implicit runTime: zio.Runtime[T]): swaydb.Bag.Async[Task] =
    new Async[Task] { self =>

      override def executionContext: ExecutionContext =
        runTime.platform.executor.asEC

      override def createSerial(): Serial[Task] =
        new Serial[Task] {

          /**
           * May be use zio.Actor instead since fromFuture is ignoring the ec.
           */
          val actor = Actor[() => Unit]("ZIO Serial Actor") {
            (run, _) =>
              run()
          }(executionContext, QueueOrder.FIFO)

          override def execute[F](f: => F): Task[F] = {
            val promise = Promise[F]
            actor.send(() => promise.tryComplete(Try(f)))
            //ok may be zio.Actor is required here.
            Task.fromFuture(_ => promise.future)
          }

          override def terminate(): Task[Unit] =
            actor.terminateAndClear[Task]()(self)

          override def terminateBag[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
            actor.terminateAndClear[BAG]()(bag)
        }

      override val unit: Task[Unit] =
        Task.unit

      override def none[A]: Task[Option[A]] =
        Task.none

      override def apply[A](a: => A): Task[A] =
        Task(a)

      override def map[A, B](a: Task[A])(f: A => B): Task[B] =
        a.map(f)

      override def transform[A, B](a: Task[A])(f: A => B): Task[B] =
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

      override def fromIO[E: swaydb.IO.ExceptionHandler, A](a: swaydb.IO[E, A]): Task[A] =
        Task.fromTry(a.toTry)

      override def fromFuture[A](a: Future[A]): Task[A] =
        Task.fromFuture(_ => a)

      override def suspend[B](f: => Task[B]): Task[B] =
        Task.effectSuspendTotal(f)
    }
}
