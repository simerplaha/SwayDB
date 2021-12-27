/*
 * Copyright (c) 27/12/21, 1:51 pm Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.IO.ApiIO

object BagApiIO extends Bag.Sync[ApiIO] {

  implicit val exceptionHandler = swaydb.Error.API.ExceptionHandler

  @inline override def isSuccess[A](a: ApiIO[A]): Boolean =
    a.isRight

  @inline override def isFailure[A](a: ApiIO[A]): Boolean =
    a.isLeft

  @inline override def exception[A](a: ApiIO[A]): Option[Throwable] =
    a.left.toOption.map(_.exception)

  @inline override def getOrElse[A, B >: A](a: ApiIO[A])(b: => B): B =
    a.getOrElse(b)

  @inline override def getUnsafe[A](a: ApiIO[A]): A =
    a.get

  @inline override def orElse[A, B >: A](a: ApiIO[A])(b: ApiIO[B]): ApiIO[B] =
    a.orElse(b)

  @inline override def unit: ApiIO[Unit] =
    IO.unit

  @inline override def none[A]: ApiIO[Option[A]] =
    IO.none

  @inline override def apply[A](a: => A): ApiIO[A] =
    IO(a)

  @inline override def foreach[A](a: ApiIO[A])(f: A => Unit): Unit =
    a.foreach(f)

  @inline override def map[A, B](a: ApiIO[A])(f: A => B): ApiIO[B] =
    a.map(f)

  @inline override def transform[A, B](a: ApiIO[A])(f: A => B): ApiIO[B] =
    a.transform(f)

  @inline override def flatMap[A, B](fa: ApiIO[A])(f: A => ApiIO[B]): ApiIO[B] =
    fa.flatMap(f)

  @inline override def success[A](value: A): ApiIO[A] =
    IO.Right(value)

  @inline override def failure[A](exception: Throwable): ApiIO[A] =
    IO.failed(exception)

  @inline override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): ApiIO[A] =
    IO[swaydb.Error.API, A](a.get)

  @inline override def suspend[B](f: => ApiIO[B]): ApiIO[B] =
    f
  @inline override def flatten[A](fa: ApiIO[ApiIO[A]]): ApiIO[A] =
    fa.flatten

  @inline override def recover[A, B >: A](fa: ApiIO[A])(pf: PartialFunction[Throwable, B]): ApiIO[B] =
    fa match {
      case right @ IO.Right(_) =>
        right

      case IO.Left(value) =>
        IO(pf.apply(value.exception))
    }

  @inline override def recoverWith[A, B >: A](fa: ApiIO[A])(pf: PartialFunction[Throwable, ApiIO[B]]): ApiIO[B] =
    fa match {
      case right @ IO.Right(_) =>
        right

      case IO.Left(value) =>
        pf.apply(value.exception)
    }
}
