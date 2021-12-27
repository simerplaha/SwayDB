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

package swaydb

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/**
 * [[Bag]]s implement functions for managing side-effect. You can use any external effect type like Try, Future etc.
 *
 * Popular effect libraries in Scala like Cats, ZIO and Monix are supported internally. See examples repo for samples.
 *
 * [[Glass]] can be used to disable effect types.
 */
sealed trait Bag[BAG[_]] { thisBag =>
  @inline def unit: BAG[Unit]
  @inline def none[A]: BAG[Option[A]]
  @inline def apply[A](a: => A): BAG[A]
  @inline def foreach[A](a: BAG[A])(f: A => Unit): Unit
  @inline def map[A, B](a: BAG[A])(f: A => B): BAG[B]
  @inline def transform[A, B](a: BAG[A])(f: A => B): BAG[B]
  @inline def flatMap[A, B](fa: BAG[A])(f: A => BAG[B]): BAG[B]
  @inline def flatten[A](fa: BAG[BAG[A]]): BAG[A]
  @inline def success[A](value: A): BAG[A]
  @inline def failure[A](exception: Throwable): BAG[A]
  @inline def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): BAG[A]

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

  @inline def safe[A](f: => BAG[A]): BAG[A] =
    flatMap(unit) {
      _ =>
        f
    }

  @inline def getOrElseOption[A, B >: A](fa: BAG[Option[A]])(orElse: => B): BAG[B] =
    map(fa)(_.getOrElse(orElse))

  @inline def getOrElseOptionFlatten[A, B >: A](fa: BAG[Option[A]])(orElse: => BAG[B]): BAG[B] =
    flatMap(fa) {
      case Some(value) =>
        success(value)

      case None =>
        orElse
    }

  @inline def orElseOption[A, B >: A](fa: BAG[Option[A]])(orElse: => Option[B]): BAG[Option[B]] =
    map(fa)(_.orElse(orElse))

  @inline def orElseOptionFlatten[A, B >: A](fa: BAG[Option[A]])(orElse: => BAG[Option[B]]): BAG[Option[B]] =
    flatMap(fa) {
      case some @ Some(_) =>
        success(some)

      case None =>
        orElse
    }

  @inline def filter[A](a: BAG[A])(f: A => Boolean): BAG[A] =
    flatMap(a) {
      value =>
        if (f(value))
          a
        else
          failure(new NoSuchElementException("Predicate does not hold for " + value))
    }
}

object Bag extends LazyLogging {

  trait Sync[BAG[_]] extends Bag[BAG] { self =>
    @inline def isSuccess[A](a: BAG[A]): Boolean
    @inline def isFailure[A](a: BAG[A]): Boolean
    @inline def exception[A](a: BAG[A]): Option[Throwable]
    @inline def getOrElse[A, B >: A](a: BAG[A])(b: => B): B
    @inline def getUnsafe[A](a: BAG[A]): A
    @inline def orElse[A, B >: A](a: BAG[A])(b: BAG[B]): BAG[B]
    @inline def recover[A, B >: A](fa: BAG[A])(pf: PartialFunction[Throwable, B]): BAG[B]
    @inline def recoverWith[A, B >: A](fa: BAG[A])(pf: PartialFunction[Throwable, BAG[B]]): BAG[B]

  }

  trait Async[BAG[_]] extends Bag[BAG] { self =>
    @inline def fromPromise[A](a: Promise[A]): BAG[A]
    @inline def complete[A](promise: Promise[A], a: BAG[A]): Unit
    @inline def executionContext: ExecutionContext
    @inline def fromFuture[A](a: Future[A]): BAG[A]
  }

  /**
   * Reserved for Bags that have the ability to check is T.isComplete or not.
   *
   * zio.Task and monix.Task do not have this ability but scala.Future does.
   *
   * isComplete is required to add stack-safe read retries if there were failures like
   * async closed files during reads etc.
   */
  trait AsyncRetryable[BAG[_]] extends Async[BAG] { self =>
    @inline def isComplete[A](a: BAG[A]): Boolean
    @inline def isIncomplete[A](a: BAG[A]): Boolean =
      !isComplete(a)
  }

  //default Bag implementation for known effect types
  implicit val throwableIO: Bag.Sync[IO.ThrowableIO] = BagThrowableIO
  implicit val apiIO: Bag.Sync[IO.ApiIO] = BagApiIO

  implicit val tryBag: Bag.Sync[Try] = BagTry
  implicit val glass: Bag.Sync[Glass] = BagGlass

  implicit def future(implicit ec: ExecutionContext): BagFuture = BagFuture()(ec)

  /**
   * Convenience implicits for working with [[Bag]]s
   */
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
        @inline def map[U](f: A => U): BAG[U] =
          bag.map(bag.filter[A](a)(p))(f)

        @inline def flatMap[U](f: A => BAG[U]): BAG[U] =
          bag.flatMap(bag.filter(a)(p))(f)

        @inline def foreach(f: A => Unit): Unit =
          bag.foreach(bag.filter(a)(p))(f)

        @inline def withFilter(b: BAG[A], q: A => Boolean): WithFilter =
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
}
