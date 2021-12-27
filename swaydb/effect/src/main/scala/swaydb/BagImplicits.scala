/*
 * Copyright (c) 27/12/21, 2:15 pm Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package swaydb

import scala.concurrent.{Future, Promise}

object BagImplicits {

  implicit class AllBagsImplicits[A, BAG[_]](fa: BAG[A])(implicit bag: Bag[BAG]) {
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
