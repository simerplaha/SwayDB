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
sealed trait Bag[BAG[_]] { thisBag =>
  def unit: BAG[Unit]
  def none[A]: BAG[Option[A]]
  def apply[A](a: => A): BAG[A]
  def foreach[A](a: BAG[A])(f: A => Unit): Unit
  def map[A, B](a: BAG[A])(f: A => B): BAG[B]
  def transform[A, B](a: BAG[A])(f: A => B): BAG[B]
  def flatMap[A, B](fa: BAG[A])(f: A => BAG[B]): BAG[B]
  def flatten[A](fa: BAG[BAG[A]]): BAG[A]
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

  @inline def and[A, B](fa: BAG[A])(f: => BAG[B]): BAG[B] =
    flatMap(fa)(_ => f)

  @inline def andIO[E: IO.ExceptionHandler, A, B](fa: BAG[A])(f: => IO[E, B]): BAG[B] =
    and(fa)(fromIO(f))

  @inline def andTransform[A, B](fa: BAG[A])(f: => B): BAG[B] =
    transform(fa)(_ => f)

  @inline def andThen[A, B](fa: BAG[A])(f: => B): BAG[B] =
    flatMap(fa)(_ => apply(f))

  def safe[A](f: => BAG[A]): BAG[A] =
    flatMap(unit) {
      _ =>
        f
    }

  def getOrElseOption[A, B >: A](fa: BAG[Option[A]])(orElse: => B): BAG[B] =
    map(fa)(_.getOrElse(orElse))

  def getOrElseOptionFlatten[A, B >: A](fa: BAG[Option[A]])(orElse: => BAG[B]): BAG[B] =
    flatMap(fa) {
      case Some(value) =>
        success(value)

      case None =>
        orElse
    }

  def orElseOption[A, B >: A](fa: BAG[Option[A]])(orElse: => Option[B]): BAG[Option[B]] =
    map(fa)(_.orElse(orElse))

  def orElseOptionFlatten[A, B >: A](fa: BAG[Option[A]])(orElse: => BAG[Option[B]]): BAG[Option[B]] =
    flatMap(fa) {
      case some @ Some(_) =>
        success(some)

      case None =>
        orElse
    }

  def filter[A](a: BAG[A])(f: A => Boolean): BAG[A] =
    flatMap(a) {
      value =>
        if (f(value))
          a
        else
          failure(new NoSuchElementException("Predicate does not hold for " + value))
    }
}

object Bag extends LazyLogging {

  object Implicits {

    implicit class BagImplicits[A, BAG[_]](fa: BAG[A])(implicit bag: Bag[BAG]) {
      @inline def and[B](b: => BAG[B]): BAG[B] =
        bag.and(fa)(b)

      @inline def andIO[E: IO.ExceptionHandler, B](b: => IO[E, B]): BAG[B] =
        bag.andIO(fa)(b)

      @inline def andThen[B](b: => B): BAG[B] =
        bag.andThen(fa)(b)

      @inline def andTransform[B](b: => B): BAG[B] =
        bag.andTransform(fa)(b)

      @inline def unit: BAG[Unit] =
        bag.unit

      @inline def none: BAG[Option[A]] =
        bag.none

      @inline def apply(a: => A): BAG[A] =
        bag(a)

      @inline def foreach(f: A => Unit): Unit =
        bag.foreach(fa)(f)

      @inline def map[B](f: A => B): BAG[B] =
        bag.map(fa)(f)

      @inline def transform[B](f: A => B): BAG[B] =
        bag.transform(fa)(f)

      @inline def flatMap[B](f: A => BAG[B]): BAG[B] =
        bag.flatMap(fa)(f)

      @inline def flatten(fa: BAG[BAG[A]]): BAG[A] =
        bag.flatten(fa)

      @inline def success(value: A): BAG[A] =
        bag.success(value)

      @inline def failure(exception: Throwable): BAG[A] =
        bag.failure(exception)

      @inline def fromIO[E: IO.ExceptionHandler](a: IO[E, A]): BAG[A] =
        bag.fromIO(a)

      @inline def suspend(f: => BAG[A]): BAG[A] =
        bag.suspend(f)

      @inline def filter(f: A => Boolean): BAG[A] =
        bag.filter(fa)(f)

      @inline final def withFilter(p: A => Boolean): WithFilter = new WithFilter(fa, p)

      final class WithFilter(a: BAG[A], p: A => Boolean) {
        def map[U](f: A => U): BAG[U] =
          bag.map(bag.filter[A](a)(p))(f)

        def flatMap[U](f: A => BAG[U]): BAG[U] =
          bag.flatMap(bag.filter(a)(p))(f)

        def foreach(f: A => Unit): Unit =
          bag.foreach(bag.filter(a)(p))(f)

        def withFilter(b: BAG[A], q: A => Boolean): WithFilter =
          new WithFilter(b, x => p(x) && q(x))
      }
    }

    implicit class BagOptionImplicits[A, BAG[_]](fa: BAG[Option[A]])(implicit bag: Bag[BAG]) {

      @inline def getOrElseOption[B >: A](orElse: => B): BAG[B] =
        bag.getOrElseOption[A, B](fa)(orElse)

      @inline def getOrElseOptionFlatten[B >: A](orElse: => BAG[B]): BAG[B] =
        bag.getOrElseOptionFlatten[A, B](fa)(orElse)

      @inline def orElseOption[B >: A](orElse: => Option[B]): BAG[Option[B]] =
        bag.orElseOption[A, B](fa)(orElse)

      @inline def orElseOptionFlatten[B >: A](orElse: => BAG[Option[B]]): BAG[Option[B]] =
        bag.orElseOptionFlatten[A, B](fa)(orElse)

    }

    implicit class BagSyncImplicits[A, BAG[_]](a: BAG[A])(implicit bag: Bag.Sync[BAG]) {
      @inline def isSuccess: Boolean =
        bag.isSuccess(a)

      @inline def isFailure: Boolean =
        bag.isFailure(a)

      @inline def exception: Option[Throwable] =
        bag.exception(a)

      @inline def getOrElse[B >: A](b: => B): B =
        bag.getOrElse[A, B](a)(b)

      @inline def getUnsafe: A =
        bag.getUnsafe(a)

      @inline def orElse[B >: A](b: BAG[B]): BAG[B] =
        bag.orElse[A, B](a)(b)

      @inline def recover[B >: A](pf: PartialFunction[Throwable, B]): BAG[B] =
        bag.recover[A, B](a)(pf)

      @inline def recoverWith[B >: A](pf: PartialFunction[Throwable, BAG[B]]): BAG[B] =
        bag.recoverWith[A, B](a)(pf)
    }

    implicit class BagAsyncImplicits[A, BAG[_]](a: BAG[A])(implicit bag: Bag.Async[BAG]) {
      @inline def complete(promise: Promise[A]): Unit =
        bag.complete(promise, a)

      @inline def fromFuture(future: Future[A]): BAG[A] =
        bag.fromFuture(future)
    }
  }

  trait Sync[BAG[_]] extends Bag[BAG] { self =>
    def isSuccess[A](a: BAG[A]): Boolean
    def isFailure[A](a: BAG[A]): Boolean
    def exception[A](a: BAG[A]): Option[Throwable]
    def getOrElse[A, B >: A](a: BAG[A])(b: => B): B
    def getUnsafe[A](a: BAG[A]): A
    def orElse[A, B >: A](a: BAG[A])(b: BAG[B]): BAG[B]
    def recover[A, B >: A](fa: BAG[A])(pf: PartialFunction[Throwable, B]): BAG[B]
    def recoverWith[A, B >: A](fa: BAG[A])(pf: PartialFunction[Throwable, BAG[B]]): BAG[B]

  }

  trait Async[BAG[_]] extends Bag[BAG] { self =>
    def fromPromise[A](a: Promise[A]): BAG[A]
    def complete[A](promise: Promise[A], a: BAG[A]): Unit
    def executionContext: ExecutionContext
    def fromFuture[A](a: Future[A]): BAG[A]
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
    trait Retryable[BAG[_]] extends Bag.Async[BAG] { self =>
      def isComplete[A](a: BAG[A]): Boolean
      def isIncomplete[A](a: BAG[A]): Boolean =
        !isComplete(a)
    }
  }

  implicit val throwableIO: Bag.Sync[IO.ThrowableIO] =
    new Bag.Sync[IO.ThrowableIO] {
      override val unit: IO.ThrowableIO[Unit] =
        IO.unit

      override def none[A]: IO.ThrowableIO[Option[A]] =
        IO.none

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
      override def flatten[A](fa: ThrowableIO[ThrowableIO[A]]): ThrowableIO[A] =
        fa.flatten

      override def recover[A, B >: A](fa: ThrowableIO[A])(pf: PartialFunction[Throwable, B]): ThrowableIO[B] =
        fa.recover(pf)

      override def recoverWith[A, B >: A](fa: ThrowableIO[A])(pf: PartialFunction[Throwable, ThrowableIO[B]]): ThrowableIO[B] =
        fa.recoverWith(pf)
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
      override def flatten[A](fa: ApiIO[ApiIO[A]]): ApiIO[A] =
        fa.flatten

      override def recover[A, B >: A](fa: ApiIO[A])(pf: PartialFunction[Throwable, B]): ApiIO[B] =
        fa match {
          case right @ IO.Right(_) =>
            right

          case IO.Left(value) =>
            IO(pf.apply(value.exception))
        }

      override def recoverWith[A, B >: A](fa: ApiIO[A])(pf: PartialFunction[Throwable, ApiIO[B]]): ApiIO[B] =
        fa match {
          case right @ IO.Right(_) =>
            right

          case IO.Left(value) =>
            pf.apply(value.exception)
        }
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

      override def flatten[A](fa: Try[Try[A]]): Try[A] =
        fa.flatten

      override def recover[A, B >: A](fa: Try[A])(pf: PartialFunction[Throwable, B]): Try[B] =
        fa.recover(pf)

      override def recoverWith[A, B >: A](fa: Try[A])(pf: PartialFunction[Throwable, Try[B]]): Try[B] =
        fa.recoverWith(pf)
    }

  type Less[+A] = A

  implicit val less: Bag.Sync[Less] =
    new Bag.Sync[Less] {
      override def isSuccess[A](a: Less[A]): Boolean = true

      override def isFailure[A](a: Less[A]): Boolean = false

      override def exception[A](a: Less[A]): Option[Throwable] = None

      override def getOrElse[A, B >: A](a: Less[A])(b: => B): B = a

      override def getUnsafe[A](a: Less[A]): A = a

      override def orElse[A, B >: A](a: Less[A])(b: Less[B]): B = a

      override def unit: Unit = ()

      override def none[A]: Option[A] = Option.empty[A]

      override def apply[A](a: => A): A = a

      override def foreach[A](a: Less[A])(f: A => Unit): Unit = f(a)

      override def map[A, B](a: Less[A])(f: A => B): B = f(a)

      override def transform[A, B](a: Less[A])(f: A => B): B = f(a)

      override def flatMap[A, B](fa: Less[A])(f: A => Less[B]): B = f(fa)

      override def success[A](value: A): Less[A] = value

      override def failure[A](exception: Throwable): A = throw exception

      override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): A = a.get

      override def suspend[B](f: => Less[B]): B = f

      override def safe[B](f: => Less[B]): B = f

      override def flatten[A](fa: Less[A]): A = fa

      override def recover[A, B >: A](fa: Less[A])(pf: PartialFunction[Throwable, B]): B = fa

      override def recoverWith[A, B >: A](fa: Less[A])(pf: PartialFunction[Throwable, Less[B]]): Less[B] = fa
    }

  implicit def future(implicit ec: ExecutionContext): Bag.Async.Retryable[Future] =
    new Async.Retryable[Future] { self =>

      override def executionContext: ExecutionContext =
        ec

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

      override def flatten[A](fa: Future[Future[A]]): Future[A] =
        fa.flatten
    }
}
