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

import scala.util.Try

case object BagTry extends Bag.Sync[Try] {
  val unit: scala.util.Success[Unit] = scala.util.Success(())

  val none: scala.util.Success[Option[Nothing]] = scala.util.Success(None)

  @inline override def isSuccess[A](a: Try[A]): Boolean =
    a.isSuccess

  @inline override def isFailure[A](a: Try[A]): Boolean =
    a.isFailure

  @inline override def exception[A](a: Try[A]): Option[Throwable] =
    a.failed.toOption

  @inline override def getOrElse[A, B >: A](a: Try[A])(b: => B): B =
    a.getOrElse(b)

  @inline override def getUnsafe[A](a: Try[A]): A =
    a.get

  @inline override def orElse[A, B >: A](a: Try[A])(b: Try[B]): Try[B] =
    a.orElse(b)

  @inline override def none[A]: Try[Option[A]] =
    none

  @inline override def apply[A](a: => A): Try[A] =
    Try(a)

  @inline override def foreach[A](a: Try[A])(f: A => Unit): Unit =
    a.foreach(f)

  @inline override def map[A, B](a: Try[A])(f: A => B): Try[B] =
    a.map(f)

  @inline override def transform[A, B](a: Try[A])(f: A => B): Try[B] =
    a.transform(a => Try(f(a)), exception => scala.util.Failure(exception))

  @inline override def flatMap[A, B](fa: Try[A])(f: A => Try[B]): Try[B] =
    fa.flatMap(f)

  @inline override def success[A](value: A): Try[A] =
    scala.util.Success(value)

  @inline override def failure[A](exception: Throwable): Try[A] =
    scala.util.Failure(exception)

  @inline override def fromIO[E: IO.ExceptionHandler, A](a: IO[E, A]): Try[A] =
    a.toTry

  @inline override def suspend[B](f: => Try[B]): Try[B] =
    f

  @inline override def flatten[A](fa: Try[Try[A]]): Try[A] =
    fa.flatten

  @inline override def recover[A, B >: A](fa: Try[A])(pf: PartialFunction[Throwable, B]): Try[B] =
    fa.recover(pf)

  @inline override def recoverWith[A, B >: A](fa: Try[A])(pf: PartialFunction[Throwable, Try[B]]): Try[B] =
    fa.recoverWith(pf)

}
