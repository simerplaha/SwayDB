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

package swaydb

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO.{ApiIO, ThrowableIO}
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.data.util.Futures

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/**
 * [[Bag]]s implement functions for managing side-effect. You can use any external effect type like Try, Future etc.
 *
 * Popular effect libraries in Scala like Cats, ZIO and Monix are supported internally. See examples repo for samples.
 *
 * [[Bag.Less]] can be used to disable effect types.
 */
sealed trait Bag[BAG[_]] {
  def unit: BAG[Unit]
  def none[A]: BAG[Option[A]]
  def apply[A](a: => A): BAG[A]
  def createSerial(): Serial[BAG]
  def foreach[A](a: BAG[A])(f: A => Unit): Unit
  def map[A, B](a: BAG[A])(f: A => B): BAG[B]
  def transform[A, B](a: BAG[A])(f: A => B): BAG[B]
  def flatMap[A, B](fa: BAG[A])(f: A => BAG[B]): BAG[B]
  def success[A](value: A): BAG[A]
  def failure[A](exception: Throwable): BAG[A]
  def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): BAG[A]

  /**
   * For Async [[Bag]]s [[apply]] will always run asynchronously but to cover
   * cases where the operation might already be executed [[suspend]] is used.
   *
   * @example All SwayDB writes occur synchronously using [[IO]]. Running completed [[IO]] in a [[Future]]
   *          will have a performance cost. [[suspend]] is used to cover these cases and [[IO]]
   *          types that are complete are directly converted to Future in current thread.
   */
  @inline def suspend[B](f: => BAG[B]): BAG[B]

  def safe[B](f: => BAG[B]): BAG[B] =
    flatMap(unit) {
      _ =>
        f
    }
}

object Bag extends LazyLogging {

  /**
   * Converts containers. More tags can be created from existing Bags with this trait using [[Bag.toBag]]
   */
  trait Transfer[A[_], B[_]] {
    def to[T](a: A[T]): B[T]
    def from[T](a: B[T]): A[T]
  }

  object Transfer {

    implicit def sameBag[A[_], B[_]](implicit evd: A[_] =:= B[_]) =
      new Transfer[A, B] {
        override def to[T](a: A[T]): B[T] =
          a.asInstanceOf[B[T]]

        override def from[T](b: B[T]): A[T] =
          b.asInstanceOf[A[T]]
      }

    implicit val optionToTry = new Transfer[Option, Try] {
      override def to[T](a: Option[T]): Try[T] =
        tryBag(a.get)

      override def from[T](a: Try[T]): Option[T] =
        a.toOption
    }

    implicit val tryToOption = new Transfer[Try, Option] {
      override def to[T](a: Try[T]): Option[T] =
        a.toOption

      override def from[T](a: Option[T]): Try[T] =
        tryBag(a.get)
    }

    implicit val tryToIO = new Transfer[Try, IO.ApiIO] {
      override def to[T](a: Try[T]): ApiIO[T] =
        IO.fromTry[Error.API, T](a)

      override def from[T](a: ApiIO[T]): Try[T] =
        a.toTry
    }

    implicit val ioToTry = new Transfer[IO.ApiIO, Try] {
      override def to[T](a: ApiIO[T]): Try[T] =
        a.toTry

      override def from[T](a: Try[T]): ApiIO[T] =
        IO.fromTry[Error.API, T](a)
    }

    implicit val ioToOption = new Transfer[IO.ApiIO, Option] {
      override def to[T](a: ApiIO[T]): Option[T] =
        a.toOption

      override def from[T](a: Option[T]): ApiIO[T] =
        IO[Error.API, T](a.get)
    }

    implicit val throwableToApiIO = new Bag.Transfer[IO.ThrowableIO, IO.ApiIO] {
      override def to[T](a: IO.ThrowableIO[T]): IO.ApiIO[T] =
        IO[swaydb.Error.API, T](a.get)

      override def from[T](a: IO.ApiIO[T]): IO.ThrowableIO[T] =
        IO(a.get)
    }

    implicit val throwableToTry = new Bag.Transfer[IO.ThrowableIO, Try] {
      override def to[T](a: IO.ThrowableIO[T]): Try[T] =
        a.toTry

      override def from[T](a: Try[T]): IO.ThrowableIO[T] =
        IO.fromTry(a)
    }

    implicit val throwableToOption = new Bag.Transfer[IO.ThrowableIO, Option] {
      override def to[T](a: IO.ThrowableIO[T]): Option[T] =
        a.toOption

      override def from[T](a: Option[T]): IO.ThrowableIO[T] =
        IO(a.get)
    }

    implicit val throwableToUnit = new Bag.Transfer[IO.ThrowableIO, IO.UnitIO] {
      override def to[T](a: IO.ThrowableIO[T]): IO.UnitIO[T] =
        IO[Unit, T](a.get)(IO.ExceptionHandler.Unit)

      override def from[T](a: IO.UnitIO[T]): IO.ThrowableIO[T] =
        IO(a.get)
    }

    implicit val throwableToNothing = new Bag.Transfer[IO.ThrowableIO, IO.NothingIO] {
      override def to[T](a: IO.ThrowableIO[T]): IO.NothingIO[T] =
        IO[Nothing, T](a.get)(IO.ExceptionHandler.Nothing)

      override def from[T](a: IO.NothingIO[T]): IO.ThrowableIO[T] =
        IO(a.get)
    }
  }

  private sealed trait ToBagBase[T[_], X[_]] {
    def base: Bag[T]

    def baseConverter: Bag.Transfer[T, X]

    def unit: X[Unit] =
      baseConverter.to(base.unit)

    def none[A]: X[Option[A]] =
      baseConverter.to(base.none)

    def apply[A](a: => A): X[A] =
      baseConverter.to(base.apply(a))

    def foreach[A](a: X[A])(f: A => Unit): Unit =
      base.foreach(baseConverter.from(a))(f)

    def map[A, B](a: X[A])(f: A => B): X[B] =
      baseConverter.to(base.map(baseConverter.from(a))(f))

    def transform[A, B](a: X[A])(f: A => B): X[B] =
      baseConverter.to(base.transform(baseConverter.from(a))(f))

    def flatMap[A, B](fa: X[A])(f: A => X[B]): X[B] =
      baseConverter.to {
        base.flatMap(baseConverter.from(fa)) {
          a =>
            baseConverter.from(f(a))
        }
      }

    def success[A](value: A): X[A] =
      baseConverter.to(base.success(value))

    def failure[A](exception: Throwable): X[A] =
      baseConverter.to(base.failure(exception))

    def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): X[A] =
      baseConverter.to(base.fromIO(a))
  }

  trait Sync[T[_]] extends Bag[T] { self =>
    def isSuccess[A](a: T[A]): Boolean
    def isFailure[A](a: T[A]): Boolean
    def exception[A](a: T[A]): Option[Throwable]
    def getOrElse[A, B >: A](a: T[A])(b: => B): B
    def getUnsafe[A](a: T[A]): A
    def orElse[A, B >: A](a: T[A])(b: T[B]): T[B]

    def toBag[X[_]](implicit transfer: Bag.Transfer[T, X]): Bag.Sync[X] =
      new Bag.Sync[X] with ToBagBase[T, X] {
        override val base: Bag[T] = self

        override val baseConverter: Transfer[T, X] = transfer

        override def createSerial(): Serial[X] =
          new Serial[X] {
            val selfSerial = base.createSerial()

            override def execute[F](f: => F): X[F] =
              transfer.to(selfSerial.execute(f))
          }

        override def exception[A](a: X[A]): Option[Throwable] =
          self.exception(transfer.from(a))

        override def isSuccess[A](a: X[A]): Boolean =
          self.isSuccess(transfer.from(a))

        override def isFailure[A](a: X[A]): Boolean =
          self.isFailure(transfer.from(a))

        override def getOrElse[A, B >: A](a: X[A])(b: => B): B =
          self.getOrElse[A, B](transfer.from(a))(b)

        override def getUnsafe[A](a: X[A]): A =
          self.getUnsafe(transfer.from(a))

        override def orElse[A, B >: A](a: X[A])(b: X[B]): X[B] =
          transfer.to(self.orElse[A, B](transfer.from(a))(transfer.from(b)))

        override def suspend[B](f: => X[B]): X[B] = f
      }
  }

  trait Async[T[_]] extends Bag[T] { self =>
    def fromPromise[A](a: Promise[A]): T[A]
    def complete[A](promise: Promise[A], a: T[A]): Unit
    def executionContext: ExecutionContext
    def fromFuture[A](a: Future[A]): T[A]

    def toBag[X[_]](implicit transfer: Bag.Transfer[T, X]): Bag.Async[X] =
      new Bag.Async[X] with ToBagBase[T, X] {
        override val base: Bag.Async[T] = self

        override val baseConverter: Transfer[T, X] = transfer

        override def createSerial(): Serial[X] =
          new Serial[X] {
            val selfSerial = base.createSerial()

            override def execute[F](f: => F): X[F] =
              transfer.to(selfSerial.execute(f))
          }

        override def fromPromise[A](a: Promise[A]): X[A] =
          transfer.to(self.fromPromise(a))

        override def complete[A](promise: Promise[A], a: X[A]): Unit =
          self.complete(promise, transfer.from(a))

        override def executionContext: ExecutionContext =
          self.executionContext

        override def fromFuture[A](a: Future[A]): X[A] =
          transfer.to(self.fromFuture(a))

        override def suspend[B](f: => X[B]): X[B] =
          flatMap[Unit, B](unit)(_ => f)
      }
  }

  object Async {

    /**
     * Reserved for Bags that have the ability to check is T.isComplete or not.
     *
     * zio.Task and monix.Task do not have this ability but scala.Future does.
     *
     * isComplete is required to add stack-safe read retries if there were failures like
     * async closed files during reads etc.
     */
    trait Retryable[T[_]] extends Bag.Async[T] { self =>
      def isComplete[A](a: T[A]): Boolean
      def isIncomplete[A](a: T[A]): Boolean =
        !isComplete(a)
    }
  }

  implicit val throwableIO: Bag.Sync[IO.ThrowableIO] =
    new Bag.Sync[IO.ThrowableIO] {
      override val unit: IO.ThrowableIO[Unit] =
        IO.unit

      override def none[A]: IO.ThrowableIO[Option[A]] =
        IO.none

      override def createSerial(): Serial[ThrowableIO] =
        new Serial[ThrowableIO] {
          override def execute[F](f: => F): ThrowableIO[F] =
            IO(f)
        }

      override def apply[A](a: => A): IO.ThrowableIO[A] =
        IO(a)

      def isSuccess[A](a: IO.ThrowableIO[A]): Boolean =
        a.isRight

      def isFailure[A](a: IO.ThrowableIO[A]): Boolean =
        a.isLeft

      override def map[A, B](a: IO.ThrowableIO[A])(f: A => B): IO.ThrowableIO[B] =
        a.map(f)

      override def transform[A, B](a: ThrowableIO[A])(f: A => B): ThrowableIO[B] =
        a.transform(f)

      override def foreach[A](a: IO.ThrowableIO[A])(f: A => Unit): Unit =
        a.foreach(f)

      override def flatMap[A, B](fa: IO.ThrowableIO[A])(f: A => IO.ThrowableIO[B]): IO.ThrowableIO[B] =
        fa.flatMap(f)

      override def success[A](value: A): IO.ThrowableIO[A] =
        IO.Right(value)

      override def failure[A](exception: Throwable): IO.ThrowableIO[A] =
        IO.failed(exception)

      override def exception[A](a: IO.ThrowableIO[A]): Option[Throwable] =
        a.left.toOption

      override def getOrElse[A, B >: A](a: IO.ThrowableIO[A])(b: => B): B =
        a.getOrElse(b)

      override def getUnsafe[A](a: ThrowableIO[A]): A =
        a.get

      override def orElse[A, B >: A](a: IO.ThrowableIO[A])(b: IO.ThrowableIO[B]): IO.ThrowableIO[B] =
        a.orElse(b)

      override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): IO.ThrowableIO[A] =
        IO[Throwable, A](a.get)

      override def suspend[B](f: => ThrowableIO[B]): ThrowableIO[B] =
        f
    }

  implicit val apiIO: Bag.Sync[IO.ApiIO] =
    new Sync[IO.ApiIO] {

      implicit val exceptionHandler = swaydb.Error.API.ExceptionHandler

      override def isSuccess[A](a: ApiIO[A]): Boolean =
        a.isRight

      override def isFailure[A](a: ApiIO[A]): Boolean =
        a.isLeft

      override def exception[A](a: ApiIO[A]): Option[Throwable] =
        a.left.toOption.map(_.exception)

      override def getOrElse[A, B >: A](a: ApiIO[A])(b: => B): B =
        a.getOrElse(b)

      override def getUnsafe[A](a: ApiIO[A]): A =
        a.get

      override def orElse[A, B >: A](a: ApiIO[A])(b: ApiIO[B]): ApiIO[B] =
        a.orElse(b)

      override def unit: ApiIO[Unit] =
        IO.unit

      override def none[A]: ApiIO[Option[A]] =
        IO.none

      override def apply[A](a: => A): ApiIO[A] =
        IO(a)

      override def createSerial(): Serial[ApiIO] =
        new Serial[ApiIO] {
          override def execute[F](f: => F): ApiIO[F] =
            IO(f)
        }

      override def foreach[A](a: ApiIO[A])(f: A => Unit): Unit =
        a.foreach(f)

      override def map[A, B](a: ApiIO[A])(f: A => B): ApiIO[B] =
        a.map(f)

      override def transform[A, B](a: ApiIO[A])(f: A => B): ApiIO[B] =
        a.transform(f)

      override def flatMap[A, B](fa: ApiIO[A])(f: A => ApiIO[B]): ApiIO[B] =
        fa.flatMap(f)

      override def success[A](value: A): ApiIO[A] =
        IO.Right(value)

      override def failure[A](exception: Throwable): ApiIO[A] =
        IO.failed(exception)

      override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): ApiIO[A] =
        IO[swaydb.Error.API, A](a.get)

      override def suspend[B](f: => ApiIO[B]): ApiIO[B] =
        f
    }

  implicit val tryBag: Bag.Sync[Try] =
    new Bag.Sync[Try] {
      val unit: scala.util.Success[Unit] = scala.util.Success(())

      val none: scala.util.Success[Option[Nothing]] = scala.util.Success(None)

      override def isSuccess[A](a: Try[A]): Boolean =
        a.isSuccess

      override def isFailure[A](a: Try[A]): Boolean =
        a.isFailure

      override def exception[A](a: Try[A]): Option[Throwable] =
        a.failed.toOption

      override def getOrElse[A, B >: A](a: Try[A])(b: => B): B =
        a.getOrElse(b)

      override def getUnsafe[A](a: Try[A]): A =
        a.get

      override def orElse[A, B >: A](a: Try[A])(b: Try[B]): Try[B] =
        a.orElse(b)

      override def none[A]: Try[Option[A]] =
        none

      override def apply[A](a: => A): Try[A] =
        Try(a)

      override def createSerial(): Serial[Try] =
        new Serial[Try] {
          override def execute[F](f: => F): Try[F] =
            Try(f)
        }

      override def foreach[A](a: Try[A])(f: A => Unit): Unit =
        a.foreach(f)

      override def map[A, B](a: Try[A])(f: A => B): Try[B] =
        a.map(f)

      override def transform[A, B](a: Try[A])(f: A => B): Try[B] =
        a.transform(a => Try(f(a)), exception => scala.util.Failure(exception))

      override def flatMap[A, B](fa: Try[A])(f: A => Try[B]): Try[B] =
        fa.flatMap(f)

      override def success[A](value: A): Try[A] =
        scala.util.Success(value)

      override def failure[A](exception: Throwable): Try[A] =
        scala.util.Failure(exception)

      override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): Try[A] =
        a.toTry

      override def suspend[B](f: => Try[B]): Try[B] =
        f
    }

  type Less[A] = A

  implicit val less: Bag.Sync[Less] =
    new Bag.Sync[Less] {
      override def isSuccess[A](a: Less[A]): Boolean = true

      override def isFailure[A](a: Less[A]): Boolean = false

      override def exception[A](a: Less[A]): Option[Throwable] = None

      override def getOrElse[A, B >: A](a: Less[A])(b: => B): B = a

      override def getUnsafe[A](a: Less[A]): A = a

      override def orElse[A, B >: A](a: Less[A])(b: Less[B]): Less[B] = a

      override def unit: Less[Unit] = ()

      override def none[A]: Less[Option[A]] = Option.empty[A]

      override def apply[A](a: => A): Less[A] = a

      override def createSerial(): Serial[Less] =
        new Serial[Less] {
          override def execute[F](f: => F): Less[F] = f
        }

      override def foreach[A](a: Less[A])(f: A => Unit): Unit = f(a)

      override def map[A, B](a: Less[A])(f: A => B): Less[B] = f(a)

      override def transform[A, B](a: Less[A])(f: A => B): Less[B] = f(a)

      override def flatMap[A, B](fa: Less[A])(f: A => Less[B]): Less[B] = f(fa)

      override def success[A](value: A): Less[A] = value

      override def failure[A](exception: Throwable): Less[A] = throw exception

      override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): Less[A] = a.get

      override def suspend[B](f: => Less[B]): Less[B] = f

      override def safe[B](f: => Less[B]): Less[B] = f
    }

  implicit def future(implicit ec: ExecutionContext): Bag.Async.Retryable[Future] =
    new Async.Retryable[Future] {

      override def executionContext: ExecutionContext =
        ec

      override def createSerial(): Serial[Future] =
        new Serial[Future] {

          val actor = Actor[() => Unit] {
            (run, _) =>
              run()
          }(ec, QueueOrder.FIFO)

          override def execute[F](f: => F): Future[F] = {
            val promise = Promise[F]()
            actor.send(() => promise.tryComplete(Try(f)))
            promise.future
          }
        }

      override val unit: Future[Unit] =
        Futures.unit

      override def none[A]: Future[Option[A]] =
        Future.successful(None)

      override def apply[A](a: => A): Future[A] =
        Future(a)

      override def map[A, B](a: Future[A])(f: A => B): Future[B] =
        a.map(f)

      override def transform[A, B](a: Future[A])(f: A => B): Future[B] =
        a.transform(f, throwable => throwable)

      override def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] =
        fa.flatMap(f)

      override def success[A](value: A): Future[A] =
        Future.successful(value)

      override def failure[A](exception: Throwable): Future[A] =
        Future.failed(exception)

      override def foreach[A](a: Future[A])(f: A => Unit): Unit =
        a.foreach(f)

      def fromPromise[A](a: Promise[A]): Future[A] =
        a.future

      override def complete[A](promise: Promise[A], a: Future[A]): Unit =
        promise tryCompleteWith a

      def isComplete[A](a: Future[A]): Boolean =
        a.isCompleted

      override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): Future[A] =
        a.toFuture

      override def fromFuture[A](a: Future[A]): Future[A] =
        a

      override def suspend[B](f: => Future[B]): Future[B] =
        f
    }
}
