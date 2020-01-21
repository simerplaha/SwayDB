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

package swaydb

import scala.concurrent.{ExecutionContext, Future}

trait Monad[T[_]] {
  def map[A, B](a: A, f: A => B): T[B]
  def flatMap[A, B](a: T[A], f: A => T[B]): T[B]
  def success[A](a: A): T[A]
  def failed[A](a: Throwable): T[A]
}

object Monad {
  implicit class Map[A](value: A) {
    @inline final def map[B, T[_]](f: A => B)(implicit monad: Monad[T]): T[B] =
      monad.map(value, f)
  }

  implicit class FlatMap[A, T[_]](value: T[A])(implicit monad: Monad[T]) {
    @inline final def flatMap[B](f: A => T[B]): T[B] =
      monad.flatMap(value, f)
  }

  implicit def futureMonad(implicit ec: ExecutionContext): Monad[Future] =
    new Monad[Future] {
      override def map[A, B](a: A, f: A => B): Future[B] =
        Future(f(a))

      override def flatMap[A, B](a: Future[A], f: A => Future[B]): Future[B] =
        a.flatMap(f)

      override def success[A](a: A): Future[A] =
        Future.successful(a)

      override def failed[A](a: Throwable): Future[A] =
        Future.failed(a)
    }
}
