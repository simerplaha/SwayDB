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
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import swaydb.data.IO
import swaydb.data.io.converter.{AsyncIOConverter, BlockingIOConverter}

/**
  * [[Wrap]]s are used to wrap databases operations (side-effects) into types that can be
  * used to build more declarative APIs.
  */
sealed trait Wrap[W[_]] {
  def apply[A](a: => A): W[A]
  def foreach[A, B](a: A)(f: A => B): Unit
  def map[A, B](a: A)(f: A => B): W[B]
  def flatMap[A, B](fa: W[A])(f: A => W[B]): W[B]
  def success[A](value: A): W[A]
  def none[A]: W[Option[A]]
  def foldLeft[A, U](initial: U, stream: Stream[A, W], skip: Int, size: Option[Int])(operation: (U, A) => U): W[U]
  def toFuture[A](a: W[A]): Future[A]
  def toIO[A](a: W[A]): IO[A]
}

object Wrap {

  def async[O[_]](converter: AsyncIOConverter[O], timeout: FiniteDuration)(implicit ec: ExecutionContext): Wrap[O] =
    new Wrap[O] {
      private val futureWrapper = futureWrap(timeout)

      override def apply[A](a: => A): O[A] = converter(futureWrapper.apply(a))
      override def foreach[A, B](a: A)(f: A => B): Unit = futureWrapper.foreach(a)(f)
      override def map[A, B](a: A)(f: A => B): O[B] = converter(futureWrapper.map(a)(f))
      override def flatMap[A, B](fa: O[A])(f: A => O[B]): O[B] = converter(futureWrapper.flatMap(converter.toFuture(fa))(a => converter.toFuture(f(a))))
      override def success[A](value: A): O[A] = converter(futureWrapper.success(value))
      override def none[A]: O[Option[A]] = converter.none
      override def foldLeft[A, U](initial: U, stream: Stream[A, O], skip: Int, size: Option[Int])(operation: (U, A) => U): O[U] =
        converter(futureWrapper.foldLeft(initial, stream.asFuture(futureWrapper), skip, size)(operation))
      override def toFuture[A](a: O[A]): Future[A] = converter.toFuture(a)
      override def toIO[A](a: O[A]): IO[A] = futureWrapper.toIO(converter.toFuture(a))
    }

  def sync[O[_]](converter: BlockingIOConverter[O]): Wrap[O] =
    new Wrap[O] {
      override def apply[A](a: => A): O[A] = converter(ioWrap.apply(a))
      override def foreach[A, B](a: A)(f: A => B): Unit = ioWrap.foreach(a)(f)
      override def map[A, B](a: A)(f: A => B): O[B] = converter(ioWrap.map(a)(f))
      override def flatMap[A, B](fa: O[A])(f: A => O[B]): O[B] = converter(ioWrap.flatMap(converter.toIO(fa))(a => converter.toIO(f(a))))
      override def success[A](value: A): O[A] = converter(ioWrap.success(value))
      override def none[A]: O[Option[A]] = converter(ioWrap.none)
      override def foldLeft[A, U](initial: U, stream: Stream[A, O], skip: Int, size: Option[Int])(operation: (U, A) => U): O[U] =
        converter(ioWrap.foldLeft(initial, stream.asIO(ioWrap), skip, size)(operation))
      override def toFuture[A](a: O[A]): Future[A] = converter.toIO(a).toFuture
      override def toIO[A](a: O[A]): IO[A] = converter.toIO(a)
    }

  implicit val tryWrap = new Wrap[Try] {
    override def apply[A](a: => A): Try[A] = Try(a)
    override def map[A, B](a: A)(f: A => B): Try[B] = Try(f(a))
    override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
    override def flatMap[A, B](fa: Try[A])(f: A => Try[B]): Try[B] = fa.flatMap(f)
    override def success[A](value: A): Try[A] = scala.util.Success(value)
    override def none[A]: Try[Option[A]] = scala.util.Success(None)
    override def foldLeft[A, U](initial: U, stream: Stream[A, Try], skip: Int, size: Option[Int])(operation: (U, A) => U): Try[U] =
      ioWrap.foldLeft(initial, stream.asIO(ioWrap), skip, size)(operation).toTry //use ioWrap and convert that result to try.
    override def toFuture[A](a: Try[A]): Future[A] = Future.fromTry(a)
    override def toIO[A](a: Try[A]): IO[A] = IO.fromTry(a)
  }

  implicit val ioWrap = new Wrap[IO] {
    override def apply[A](a: => A): IO[A] = IO(a)
    override def map[A, B](a: A)(f: A => B): IO[B] = IO(f(a))
    override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
    override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
    override def success[A](value: A): IO[A] = IO.Success(value)
    override def none[A]: IO[Option[A]] = IO.none
    override def toFuture[A](a: IO[A]): Future[A] = a.toFuture
    override def toIO[A](a: IO[A]): IO[A] = a
    override def foldLeft[A, U](initial: U, stream: Stream[A, IO], skip: Int, size: Option[Int])(operation: (U, A) => U): IO[U] = {
      @tailrec
      def fold(previous: A, skip: Int, currentSize: Int, previousResult: U): IO[U] =
        if (size.contains(currentSize))
          IO.Success(previousResult)
        else
          stream.next(previous) match {
            case IO.Success(Some(next)) =>
              if (skip >= 1) {
                fold(next, skip - 1, currentSize, previousResult)
              } else {
                val nextResult =
                  try {
                    operation(previousResult, next)
                  } catch {
                    case exception: Throwable =>
                      return IO.Failure(exception)
                  }
                fold(next, skip, currentSize + 1, nextResult)
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
              fold(first, skip - 1, 0, initial)
            else {
              val next =
                try {
                  operation(initial, first)
                } catch {
                  case throwable: Throwable =>
                    return IO.Failure(throwable)
                }
              fold(first, skip, 1, next)
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

  implicit def futureWrap(timeout: FiniteDuration = 60.seconds)(implicit ec: ExecutionContext) =
    new Wrap[Future] {
      override def apply[A](a: => A): Future[A] = Future(a)
      override def map[A, B](a: A)(f: A => B): Future[B] = Future(f(a))
      override def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
      override def success[A](value: A): Future[A] = Future.successful(value)
      override def none[A]: Future[Option[A]] = Future.successful(None)
      override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
      override def toFuture[A](a: Future[A]): Future[A] = a
      override def toIO[A](a: Future[A]): IO[A] = IO(Await.result(a, timeout)) //blocking await should be configurable
      override def foldLeft[A, U](initial: U, stream: Stream[A, Future], skip: Int, size: Option[Int])(operation: (U, A) => U): Future[U] = {
        def fold(previous: A, skip: Int, currentSize: Int, previousResult: U): Future[U] =
          if (size.contains(currentSize))
            Future.successful(previousResult)
          else
            stream
              .next(previous)
              .flatMap {
                case Some(next) =>
                  if (skip >= 1) {
                    fold(next, skip - 1, currentSize, previousResult)
                  } else {
                    try {
                      val newResult = operation(previousResult, next)
                      fold(next, skip, currentSize + 1, newResult)
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
                fold(first, skip - 1, 0, initial)
              } else {
                try {
                  val nextResult = operation(initial, first)
                  fold(first, skip, 1, nextResult)
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