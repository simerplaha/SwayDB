/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import swaydb.data.IO
import scala.concurrent.duration._
import swaydb.core.util.Delay

/**
  * New Wrappers can be implemented by extending this trait.
  */
trait Wrap[W[_]] {
  def apply[A](a: => A): W[A]
  def foreach[A, B](a: A)(f: A => B): Unit
  def map[A, B](a: A)(f: A => B): W[B]
  def flatMap[A, B](fa: W[A])(f: A => W[B]): W[B]
  def unsafeGet[A](b: W[A]): A
  def success[A](value: A): W[A]
  def none[A]: W[Option[A]]
  def foreachStream[A, U](stream: Stream[A, W])(f: A => U): Unit
  private[swaydb] def terminate[A]: W[A] = none.asInstanceOf[W[A]]
}

object Wrap {
  implicit val tryWrap = new Wrap[Try] {
    override def apply[A](a: => A): Try[A] = Try(a)
    override def map[A, B](a: A)(f: A => B): Try[B] = Try(f(a))
    override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
    override def flatMap[A, B](fa: Try[A])(f: A => Try[B]): Try[B] = fa.flatMap(f)
    override def unsafeGet[A](b: Try[A]): A = b.get
    override def success[A](value: A): Try[A] = scala.util.Success(value)
    override def none[A]: Try[Option[A]] = scala.util.Success(None)
    override def foreachStream[A, U](stream: Stream[A, Try])(f: A => U): Unit = {
      @tailrec
      def doForeach(previous: A): Unit =
        stream.next(previous) match {
          case Success(Some(next)) =>
            f(next)
            doForeach(next)

          case Success(None) =>
            ()

          case Failure(exception) =>
            throw exception
        }

      stream.first() match {
        case Success(Some(first)) =>
          f(first)
          doForeach(first)

        case Success(None) =>
          ()

        case Failure(exception) =>
          throw exception
      }
    }
  }

  implicit val ioWrap = new Wrap[IO] {
    override def apply[A](a: => A): IO[A] = IO(a)
    override def map[A, B](a: A)(f: A => B): IO[B] = IO(f(a))
    override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
    override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
    override def success[A](value: A): IO[A] = IO.Success(value)
    override def unsafeGet[A](b: IO[A]): A = b.get
    override def none[A]: IO[Option[A]] = IO.none
    override def foreachStream[A, U](stream: Stream[A, IO])(f: A => U): Unit = {
      @tailrec
      def doForeach(previous: A): Unit =
        stream.next(previous) match {
          case IO.Success(Some(next)) =>
            f(next)
            doForeach(next)

          case IO.Success(None) =>
            ()

          case IO.Failure(error) =>
            throw error.exception
        }

      stream.first() match {
        case IO.Success(Some(first)) =>
          f(first)
          doForeach(first)

        case IO.Success(None) =>
          ()

        case IO.Failure(error) =>
          throw error.exception
      }
    }
  }

  implicit def futureWrap(implicit ec: ExecutionContext): Wrap[Future] =
    futureWrap()

  implicit def futureWrap(timeout: FiniteDuration = 10.seconds)(implicit ec: ExecutionContext) = new Wrap[Future] {
    override def apply[A](a: => A): Future[A] = Future(a)
    override def map[A, B](a: A)(f: A => B): Future[B] = Future(f(a))
    override def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
    override def unsafeGet[A](b: Future[A]): A = Await.result(b, timeout)
    override def success[A](value: A): Future[A] = Future.successful(value)
    override def none[A]: Future[Option[A]] = Future.successful(None)
    override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
    override def foreachStream[A, U](stream: Stream[A, Future])(f: A => U): Unit = {
      def doForeach(previous: A): Future[Option[A]] =
        stream
          .next(previous)
          .flatMap {
            case Some(next) =>
              f(next)
              doForeach(next)

            case None =>
              Future.successful(None)
          }

      stream.first() flatMap {
        case Some(first) =>
          f(first)
          doForeach(first)

        case None =>
          Delay.futureUnit

      } recoverWith {
        case exception =>
          //This is not very nice but TraversableLike requires a foreach as well so
          //this needs to be caught at a higher place
          throw exception
      }
    }
  }

  implicit class WrapImplicits[A, W[_] : Wrap](a: W[A])(implicit wrap: Wrap[W]) {
    @inline def map[B](f: A => B): W[B] =
      wrap.flatMap(a) {
        a =>
          wrap.map[A, B](a)(f)
      }

    @inline def flatMap[B](f: A => W[B]): W[B] =
      wrap.flatMap(a)(f)

    @inline def get: A =
      wrap.unsafeGet(a)
  }
}