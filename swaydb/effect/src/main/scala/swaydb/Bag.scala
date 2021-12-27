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
  val implicits = BagImplicits
}
