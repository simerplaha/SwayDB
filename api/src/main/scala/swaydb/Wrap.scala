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
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import swaydb.core.util.Delay
import swaydb.data.IO

/**
  * New Wrappers can be implemented by extending this trait.
  */
trait Wrap[W[_]] {
  def apply[A](a: => A): W[A]
  def foreach[A, B](a: A)(f: A => B): Unit
  def map[A, B](a: A)(f: A => B): W[B]
  def flatMap[A, B](fa: W[A])(f: A => W[B]): W[B]
  def success[A](value: A): W[A]
  def none[A]: W[Option[A]]
  def foldLeft[A, U](initial: U, stream: Stream[A, W], skip: Int, size: Option[Int])(operation: (U, A) => U): W[U]
  private[swaydb] def terminate[A]: W[A] = none.asInstanceOf[W[A]]
}

object Wrap {

  implicit val tryWrap = new Wrap[Try] {
    private val unit = Success(())

    override def apply[A](a: => A): Try[A] = Try(a)
    override def map[A, B](a: A)(f: A => B): Try[B] = Try(f(a))
    override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
    override def flatMap[A, B](fa: Try[A])(f: A => Try[B]): Try[B] = fa.flatMap(f)
    override def success[A](value: A): Try[A] = scala.util.Success(value)
    override def none[A]: Try[Option[A]] = scala.util.Success(None)

    override def foldLeft[A, U](initial: U, stream: Stream[A, Try], skip: Int, size: Option[Int])(operation: (U, A) => U): Try[U] = {
      @tailrec
      def doForeach(previous: A, skip: Int, currentSize: Int, previousResult: U): Try[U] =
        if (size.contains(currentSize))
          Success(previousResult)
        else
          stream.next(previous) match {
            case Success(Some(next)) =>
              if (skip >= 1) {
                doForeach(next, skip - 1, currentSize, previousResult)
              } else {
                val nextInput =
                  try {
                    operation(previousResult, next)
                  } catch {
                    case exception: Throwable =>
                      return Failure(exception)
                  }
                doForeach(next, skip, currentSize + 1, nextInput)
              }

            case Success(None) =>
              Success(previousResult)

            case Failure(exception) =>
              Failure(exception)
          }

      if (size.contains(0))
        Success(initial)
      else
        stream.headOption match {
          case Success(Some(first)) =>
            if (skip >= 1)
              doForeach(first, skip - 1, 0, initial)
            else {
              val next =
                try {
                  operation(initial, first)
                } catch {
                  case throwable: Throwable =>
                    return Failure(throwable)
                }
              doForeach(first, skip, 1, next)
            }

          case Success(None) =>
            Success(initial)

          case Failure(exception) =>
            Failure(exception)
        }
    }
  }

  implicit val ioWrap = new Wrap[IO] {
    override def apply[A](a: => A): IO[A] = IO(a)
    override def map[A, B](a: A)(f: A => B): IO[B] = IO(f(a))
    override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
    override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
    override def success[A](value: A): IO[A] = IO.Success(value)
    override def none[A]: IO[Option[A]] = IO.none
    override def foldLeft[A, U](initial: U, stream: Stream[A, IO], skip: Int, size: Option[Int])(operation: (U, A) => U): IO[U] = {
      @tailrec
      def doForeach(previous: A, skip: Int, currentSize: Int, previousResult: U): IO[U] =
        if (size.contains(currentSize))
          IO.Success(previousResult)
        else
          stream.next(previous) match {
            case IO.Success(Some(next)) =>
              if (skip >= 1) {
                doForeach(next, skip - 1, currentSize, previousResult)
              } else {
                val nextInput =
                  try {
                    operation(previousResult, next)
                  } catch {
                    case exception: Throwable =>
                      return IO.Failure(exception)
                  }
                doForeach(next, skip, currentSize + 1, nextInput)
              }

            case IO.Success(None) =>
              IO.Success(previousResult)

            case IO.Failure(exception) =>
              IO.Failure(exception)
          }

      if (size.contains(0))
        IO.Success(initial)
      else
        stream.headOption match {
          case IO.Success(Some(first)) =>
            if (skip >= 1)
              doForeach(first, skip - 1, 0, initial)
            else {
              val next =
                try {
                  operation(initial, first)
                } catch {
                  case throwable: Throwable =>
                    return IO.Failure(throwable)
                }
              doForeach(first, skip, 1, next)
            }

          case IO.Success(None) =>
            IO.Success(initial)

          case IO.Failure(exception) =>
            IO.Failure(exception)
        }
    }
  }

  implicit def futureWrap(implicit ec: ExecutionContext): Wrap[Future] =
    futureWrap()

  implicit def futureWrap(timeout: FiniteDuration = 10.seconds)(implicit ec: ExecutionContext) = new Wrap[Future] {
    override def apply[A](a: => A): Future[A] = Future(a)
    override def map[A, B](a: A)(f: A => B): Future[B] = Future(f(a))
    override def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
    override def success[A](value: A): Future[A] = Future.successful(value)
    override def none[A]: Future[Option[A]] = Future.successful(None)
    override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
    override def foldLeft[A, U](initial: U, stream: Stream[A, Future], skip: Int, size: Option[Int])(operation: (U, A) => U): Future[U] = {
      def doForeach(previous: A, skip: Int, currentSize: Int, previousResult: U): Future[U] =
        if (size.contains(currentSize))
          Future.successful(previousResult)
        else
          stream
            .next(previous)
            .flatMap {
              case Some(next) =>
                if (skip >= 1) {
                  doForeach(next, skip - 1, currentSize, previousResult)
                } else {
                  try {
                    val newResult = operation(previousResult, next)
                    doForeach(next, skip, currentSize + 1, newResult)
                  } catch {
                    case throwable: Throwable =>
                      Future.failed(throwable)
                  }
                }

              case None =>
                Future.successful(previousResult)
            }

      if (size.contains(0))
        Future.successful(initial)
      else
        stream.headOption flatMap {
          case Some(first) =>
            if (skip >= 1) {
              doForeach(first, skip - 1, 0, initial)
            } else {
              try {
                val nextResult = operation(initial, first)
                doForeach(first, skip, 1, nextResult)
              } catch {
                case throwable: Throwable =>
                  Future.failed(throwable)
              }
            }

          case None =>
            Future.successful(initial)
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
  }
}