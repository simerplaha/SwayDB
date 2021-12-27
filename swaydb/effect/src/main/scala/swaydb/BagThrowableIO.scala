/*
 * Copyright (c) 27/12/21, 1:52 pm Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.IO.ThrowableIO

object BagThrowableIO extends Bag.Sync[IO.ThrowableIO] {

  override val unit: IO.ThrowableIO[Unit] =
    IO.unit

  @inline override def none[A]: IO.ThrowableIO[Option[A]] =
    IO.none

  @inline override def apply[A](a: => A): IO.ThrowableIO[A] =
    IO(a)

  @inline def isSuccess[A](a: IO.ThrowableIO[A]): Boolean =
    a.isRight

  @inline def isFailure[A](a: IO.ThrowableIO[A]): Boolean =
    a.isLeft

  @inline override def map[A, B](a: IO.ThrowableIO[A])(f: A => B): IO.ThrowableIO[B] =
    a.map(f)

  @inline override def transform[A, B](a: ThrowableIO[A])(f: A => B): ThrowableIO[B] =
    a.transform(f)

  @inline override def foreach[A](a: IO.ThrowableIO[A])(f: A => Unit): Unit =
    a.foreach(f)

  @inline override def flatMap[A, B](fa: IO.ThrowableIO[A])(f: A => IO.ThrowableIO[B]): IO.ThrowableIO[B] =
    fa.flatMap(f)

  @inline override def success[A](value: A): IO.ThrowableIO[A] =
    IO.Right(value)

  @inline override def failure[A](exception: Throwable): IO.ThrowableIO[A] =
    IO.failed(exception)

  @inline override def exception[A](a: IO.ThrowableIO[A]): Option[Throwable] =
    a.left.toOption

  @inline override def getOrElse[A, B >: A](a: IO.ThrowableIO[A])(b: => B): B =
    a.getOrElse(b)

  @inline override def getUnsafe[A](a: ThrowableIO[A]): A =
    a.get

  @inline override def orElse[A, B >: A](a: IO.ThrowableIO[A])(b: IO.ThrowableIO[B]): IO.ThrowableIO[B] =
    a.orElse(b)

  @inline override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): IO.ThrowableIO[A] =
    IO[Throwable, A](a.get)

  @inline override def suspend[B](f: => ThrowableIO[B]): ThrowableIO[B] =
    f
  @inline override def flatten[A](fa: ThrowableIO[ThrowableIO[A]]): ThrowableIO[A] =
    fa.flatten

  @inline override def recover[A, B >: A](fa: ThrowableIO[A])(pf: PartialFunction[Throwable, B]): ThrowableIO[B] =
    fa.recover(pf)

  @inline override def recoverWith[A, B >: A](fa: ThrowableIO[A])(pf: PartialFunction[Throwable, ThrowableIO[B]]): ThrowableIO[B] =
    fa.recoverWith(pf)

}
