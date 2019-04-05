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

package swaydb.data.io

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import swaydb.data.{IO, Stream}

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
  def failure[A](exception: Throwable): W[A]
  def none[A]: W[Option[A]]
  def foldLeft[A, U](initial: U, after: Option[A], stream: Stream[A, W], drop: Int, take: Option[Int])(operation: (U, A) => U): W[U]
  def collectFirst[A](previous: A, stream: Stream[A, W])(condition: A => Boolean): W[Option[A]]
  def toFuture[A](a: W[A]): Future[A]
  def toIO[A](a: W[A]): IO[A]
}

object Wrap {

  def buildAsyncWrap[O[_]](transform: AsyncIOTransformer[O], timeout: FiniteDuration)(implicit ec: ExecutionContext): Wrap[O] =
    new Wrap[O] {
      private val futureWrapper = futureWrap(timeout)

      override def apply[A](a: => A): O[A] = transform.toOther(futureWrapper.apply(a))
      override def foreach[A, B](a: A)(f: A => B): Unit = futureWrapper.foreach(a)(f)
      override def map[A, B](a: A)(f: A => B): O[B] = transform.toOther(futureWrapper.map(a)(f))
      override def flatMap[A, B](fa: O[A])(f: A => O[B]): O[B] = transform.toOther(futureWrapper.flatMap(transform.toFuture(fa))(a => transform.toFuture(f(a))))
      override def success[A](value: A): O[A] = transform.toOther(futureWrapper.success(value))
      override def failure[A](exception: Throwable): O[A] = transform.toOther(futureWrapper.failure(exception))
      override def none[A]: O[Option[A]] = transform.toOther(futureWrapper.none)
      override def toFuture[A](a: O[A]): Future[A] = transform.toFuture(a)
      override def toIO[A](a: O[A]): IO[A] = futureWrapper.toIO(transform.toFuture(a))
      override def foldLeft[A, U](initial: U, after: Option[A], stream: Stream[A, O], drop: Int, take: Option[Int])(operation: (U, A) => U): O[U] =
        transform.toOther(futureWrapper.foldLeft(initial, after, stream.asFuture(futureWrapper), drop, take)(operation))
      override def collectFirst[A](previous: A, stream: Stream[A, O])(condition: A => Boolean): O[Option[A]] =
        transform.toOther(futureWrapper.collectFirst(previous, stream.asFuture)(condition))
    }

  def buildSyncWrap[O[_]](transform: BlockingIOTransformer[O]): Wrap[O] =
    new Wrap[O] {
      override def apply[A](a: => A): O[A] = transform.toOther(ioWrap.apply(a))
      override def foreach[A, B](a: A)(f: A => B): Unit = ioWrap.foreach(a)(f)
      override def map[A, B](a: A)(f: A => B): O[B] = transform.toOther(ioWrap.map(a)(f))
      override def flatMap[A, B](fa: O[A])(f: A => O[B]): O[B] = transform.toOther(ioWrap.flatMap(transform.toIO(fa))(a => transform.toIO(f(a))))
      override def success[A](value: A): O[A] = transform.toOther(ioWrap.success(value))
      override def failure[A](exception: Throwable): O[A] = transform.toOther(ioWrap.failure(exception))
      override def none[A]: O[Option[A]] = transform.toOther(ioWrap.none)
      override def toFuture[A](a: O[A]): Future[A] = transform.toIO(a).toFuture
      override def toIO[A](a: O[A]): IO[A] = transform.toIO(a)
      override def foldLeft[A, U](initial: U, after: Option[A], stream: Stream[A, O], drop: Int, take: Option[Int])(operation: (U, A) => U): O[U] =
        transform.toOther(ioWrap.foldLeft(initial, after, stream.asIO(ioWrap), drop, take)(operation))
      override def collectFirst[A](previous: A, stream: Stream[A, O])(condition: A => Boolean): O[Option[A]] =
        transform.toOther(ioWrap.collectFirst(previous, stream.asIO)(condition))
    }

  implicit val tryWrap: Wrap[Try] =
    new Wrap[Try] {
      override def apply[A](a: => A): Try[A] = Try(a)
      override def map[A, B](a: A)(f: A => B): Try[B] = Try(f(a))
      override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
      override def flatMap[A, B](fa: Try[A])(f: A => Try[B]): Try[B] = fa.flatMap(f)
      override def success[A](value: A): Try[A] = scala.util.Success(value)
      override def none[A]: Try[Option[A]] = scala.util.Success(None)
      override def toFuture[A](a: Try[A]): Future[A] = Future.fromTry(a)
      override def toIO[A](a: Try[A]): IO[A] = IO.fromTry(a)
      override def failure[A](exception: Throwable): Try[A] = scala.util.Failure(exception)

      override def foldLeft[A, U](initial: U, after: Option[A], stream: Stream[A, Try], drop: Int, take: Option[Int])(operation: (U, A) => U): Try[U] =
        ioWrap.foldLeft(initial, after, stream.asIO(ioWrap), drop, take)(operation).toTry //use ioWrap and convert that result to try.

      @tailrec
      override def collectFirst[A](previous: A, stream: Stream[A, Try])(condition: A => Boolean): Try[Option[A]] =
        stream.next(previous) match {
          case success @ scala.util.Success(Some(nextA)) =>
            if (condition(nextA))
              success
            else
              collectFirst(nextA, stream)(condition)

          case none @ scala.util.Success(None) =>
            none

          case failure @ scala.util.Failure(_) =>
            failure
        }

    }

  implicit val ioWrap: Wrap[IO] =
    new Wrap[IO] {
      override def apply[A](a: => A): IO[A] = IO(a)
      override def map[A, B](a: A)(f: A => B): IO[B] = IO(f(a))
      override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
      override def flatMap[A, B](fa: IO[A])(f: A => IO[B]): IO[B] = fa.flatMap(f)
      override def success[A](value: A): IO[A] = IO.Success(value)
      override def failure[A](exception: Throwable): IO[A] = IO.Failure(exception)
      override def none[A]: IO[Option[A]] = IO.none
      override def toFuture[A](a: IO[A]): Future[A] = a.toFuture
      override def toIO[A](a: IO[A]): IO[A] = a
      override def foldLeft[A, U](initial: U, after: Option[A], stream: Stream[A, IO], drop: Int, take: Option[Int])(operation: (U, A) => U): IO[U] = {
        @tailrec
        def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): IO[U] =
          if (take.contains(currentSize))
            IO.Success(previousResult)
          else
            stream.next(previous) match {
              case IO.Success(Some(next)) =>
                if (drop >= 1) {
                  fold(next, drop - 1, currentSize, previousResult)
                } else {
                  val nextResult =
                    try {
                      operation(previousResult, next)
                    } catch {
                      case exception: Throwable =>
                        return IO.Failure(exception)
                    }
                  fold(next, drop, currentSize + 1, nextResult)
                }

              case IO.Success(None) =>
                IO.Success(previousResult)

              case IO.Failure(exception) =>
                IO.Failure(exception)
            }

        if (take.contains(0))
          IO.Success(initial)
        else
          after.map(stream.next).getOrElse(stream.headOption) match {
            case IO.Success(Some(first)) =>
              if (drop >= 1)
                fold(first, drop - 1, 0, initial)
              else {
                val next =
                  try {
                    operation(initial, first)
                  } catch {
                    case throwable: Throwable =>
                      return IO.Failure(throwable)
                  }
                fold(first, drop, 1, next)
              }

            case IO.Success(None) =>
              IO.Success(initial)

            case IO.Failure(exception) =>
              IO.Failure(exception)
          }
      }

      @tailrec
      override def collectFirst[A](previous: A, stream: Stream[A, IO])(condition: A => Boolean): IO[Option[A]] =
        stream.next(previous) match {
          case success @ IO.Success(Some(nextA)) =>
            if (condition(nextA))
              success
            else
              collectFirst(nextA, stream)(condition)

          case none @ IO.Success(None) =>
            none

          case failure @ IO.Failure(_) =>
            failure
        }
    }

  implicit def futureWrap(implicit ec: ExecutionContext): Wrap[Future] =
    futureWrap(timeout = 60.seconds)

  implicit def futureWrap(timeout: FiniteDuration)(implicit ec: ExecutionContext): Wrap[Future] =
    new Wrap[Future] {
      override def apply[A](a: => A): Future[A] = Future(a)
      override def map[A, B](a: A)(f: A => B): Future[B] = Future(f(a))
      override def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] = fa.flatMap(f)
      override def success[A](value: A): Future[A] = Future.successful(value)
      override def failure[A](exception: Throwable): Future[A] = Future.failed(exception)
      override def none[A]: Future[Option[A]] = Future.successful(None)
      override def foreach[A, B](a: A)(f: A => B): Unit = f(a)
      override def toFuture[A](a: Future[A]): Future[A] = a
      override def toIO[A](a: Future[A]): IO[A] = IO(Await.result(a, timeout))
      override def foldLeft[A, U](initial: U, after: Option[A], stream: Stream[A, Future], drop: Int, take: Option[Int])(operation: (U, A) => U): Future[U] = {
        def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): Future[U] =
          if (take.contains(currentSize))
            Future.successful(previousResult)
          else
            stream
              .next(previous)
              .flatMap {
                case Some(next) =>
                  if (drop >= 1) {
                    fold(next, drop - 1, currentSize, previousResult)
                  } else {
                    try {
                      val newResult = operation(previousResult, next)
                      fold(next, drop, currentSize + 1, newResult)
                    } catch {
                      case throwable: Throwable =>
                        Future.failed(throwable)
                    }
                  }

                case None =>
                  Future.successful(previousResult)
              }

        if (take.contains(0))
          Future.successful(initial)
        else
          after.map(stream.next).getOrElse(stream.headOption) flatMap {
            case Some(first) =>
              if (drop >= 1) {
                fold(first, drop - 1, 0, initial)
              } else {
                try {
                  val nextResult = operation(initial, first)
                  fold(first, drop, 1, nextResult)
                } catch {
                  case throwable: Throwable =>
                    Future.failed(throwable)
                }
              }

            case None =>
              Future.successful(initial)
          }
      }

      override def collectFirst[A](previous: A, stream: Stream[A, Future])(condition: A => Boolean): Future[Option[A]] =
        stream.next(previous) flatMap {
          case some @ Some(nextA) =>
            if (condition(nextA))
              Future.successful(some)
            else
              collectFirst(nextA, stream)(condition)

          case None =>
            Future.successful(None)
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
