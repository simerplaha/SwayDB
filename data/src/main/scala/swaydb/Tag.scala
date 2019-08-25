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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

import swaydb.IO.ApiIO

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/**
 * [[Tag]]s are used to tag databases operations (side-effects) into types that can be
 * used to build custom Sync and Async wrappers.
 */
sealed trait Tag[T[_]] {
  def apply[A](a: => A): T[A]
  def foreach[A, B](a: A)(f: A => B): Unit
  def map[A, B](a: A)(f: A => B): T[B]
  def flatMap[A, B](fa: T[A])(f: A => T[B]): T[B]
  def success[A](value: A): T[A]
  def failure[A](exception: Throwable): T[A]
  def none[A]: T[Option[A]]
  def foldLeft[A, U](initial: U, after: Option[A], stream: swaydb.Stream[A, T], drop: Int, take: Option[Int])(operation: (U, A) => U): T[U]
  def collectFirst[A](previous: A, stream: swaydb.Stream[A, T])(condition: A => Boolean): T[Option[A]]
  def fromIO[E: ErrorHandler, A](a: IO[E, A]): T[A]
  def toTag[X[_]](implicit converter: Tag.Converter[T, X]): Tag[X]
  def defer[B](f: => T[B]): T[B] =
    flatMap[Unit, B](success(()))(_ => f)
}

object Tag {

  object Implicits {
    implicit class TagImplicits[A, T[_] : Tag](a: T[A])(implicit tag: Tag[T]) {
      @inline def map[B](f: A => B): T[B] =
        tag.flatMap(a) {
          a =>
            tag.map[A, B](a)(f)
        }

      @inline def flatMap[B](f: A => T[B]): T[B] =
        tag.flatMap(a)(f)
    }
  }

  /**
   * Converts containers. More tags can be created from existing Tags with this trait using [[Tag.toTag]]
   */
  trait Converter[A[_], B[_]] {
    def to[T](a: A[T]): B[T]
    def from[T](a: B[T]): A[T]
  }

  implicit val optionToTry = new Converter[Option, Try] {
    override def to[T](a: Option[T]): Try[T] =
      Try(a.get)

    override def from[T](a: Try[T]): Option[T] =
      a.toOption
  }

  implicit val tryToOption = new Converter[Try, Option] {
    override def to[T](a: Try[T]): Option[T] =
      a.toOption

    override def from[T](a: Option[T]): Try[T] =
      Try(a.get)
  }

  implicit val tryToIO = new Converter[Try, IO.ApiIO] {
    override def to[T](a: Try[T]): ApiIO[T] =
      IO.fromTry[Error.API, T](a)

    override def from[T](a: ApiIO[T]): Try[T] =
      a.toTry
  }

  implicit val ioToTry = new Converter[IO.ApiIO, Try] {
    override def to[T](a: ApiIO[T]): Try[T] =
      a.toTry
    override def from[T](a: Try[T]): ApiIO[T] =
      IO.fromTry[Error.API, T](a)
  }

  implicit val ioToOption = new Converter[IO.ApiIO, Option] {
    override def to[T](a: ApiIO[T]): Option[T] =
      a.toOption
    override def from[T](a: Option[T]): ApiIO[T] =
      IO[Error.API, T](a.get)
  }

  sealed trait ConverterBase[T[_], X[_]] {
    def base: Tag[T]

    def baseConverter: Tag.Converter[T, X]

    val flipConverter =
      new Tag.Converter[X, T] {
        override def to[A](a: X[A]): T[A] =
          baseConverter.from(a)

        override def from[A](a: T[A]): X[A] =
          baseConverter.to(a)
      }

    def apply[A](a: => A): X[A] =
      baseConverter.to(base.apply(a))

    def foreach[A, B](a: A)(f: A => B): Unit =
      base.foreach(a)(f)

    def map[A, B](a: A)(f: A => B): X[B] =
      baseConverter.to(base.map(a)(f))

    def flatMap[A, B](fa: X[A])(f: A => X[B]): X[B] =
      baseConverter.to {
        base.flatMap(baseConverter.from(fa)) {
          a =>
            baseConverter.from(f(a))
        }
      }

    def success[A](value: A): X[A] =
      baseConverter.to(base.success(value))

    def failure[A](exception: Throwable): X[A] =
      baseConverter.to(base.failure(exception))

    def none[A]: X[Option[A]] =
      baseConverter.to(base.none)

    def foldLeft[A, U](initial: U, after: Option[A], stream: Stream[A, X], drop: Int, take: Option[Int])(operation: (U, A) => U): X[U] =
      baseConverter.to(base.foldLeft(initial, after, stream.to[T](base, flipConverter), drop, take)(operation))

    def collectFirst[A](previous: A, stream: Stream[A, X])(condition: A => Boolean): X[Option[A]] =
      baseConverter.to(base.collectFirst(previous, stream.to[T](base, flipConverter))(condition))

    def fromIO[E: ErrorHandler, A](a: IO[E, A]): X[A] =
      baseConverter.to(base.fromIO(a))
  }

  trait Sync[T[_]] extends Tag[T] { self =>
    def isSuccess[A](a: T[A]): Boolean
    def isFailure[A](a: T[A]): Boolean
    def exception[A](a: T[A]): Option[Throwable]
    def getOrElse[A, B >: A](a: T[A])(b: => B): B
    def orElse[A, B >: A](a: T[A])(b: T[B]): T[B]

    def toTag[X[_]](implicit converter: Tag.Converter[T, X]): Tag.Sync[X] =
      new Tag.Sync[X] with ConverterBase[T, X] {
        override val base: Tag[T] = self

        override val baseConverter: Converter[T, X] = converter

        override def exception[A](a: X[A]): Option[Throwable] =
          self.exception(converter.from(a))

        override def isSuccess[A](a: X[A]): Boolean =
          self.isSuccess(converter.from(a))

        override def isFailure[A](a: X[A]): Boolean =
          self.isFailure(converter.from(a))

        override def getOrElse[A, B >: A](a: X[A])(b: => B): B =
          self.getOrElse[A, B](converter.from(a))(b)

        override def orElse[A, B >: A](a: X[A])(b: X[B]): X[B] =
          converter.to(self.orElse[A, B](converter.from(a))(converter.from(b)))
      }
  }

  trait Async[T[_]] extends Tag[T] { self =>
    def fromPromise[A](a: Promise[A]): T[A]
    def isComplete[A](a: T[A]): Boolean
    def isIncomplete[A](a: T[A]): Boolean =
      !isComplete(a)

    def toTag[X[_]](implicit converter: Tag.Converter[T, X]): Tag.Async[X] =
      new Tag.Async[X] with ConverterBase[T, X] {
        override val base: Tag[T] = self

        override val baseConverter: Converter[T, X] = converter

        override def fromPromise[A](a: Promise[A]): X[A] =
          converter.to(self.fromPromise(a))

        override def isComplete[A](a: X[A]): Boolean =
          self.isComplete(converter.from(a))
      }
  }

  implicit val dbIO: Tag.Sync[IO.ApiIO] =
    new Tag.Sync[IO.ApiIO] {

      import swaydb.Error.API.ErrorHandler

      override def apply[A](a: => A): IO.ApiIO[A] =
        IO(a)

      def isSuccess[A](a: IO.ApiIO[A]): Boolean =
        a.isSuccess

      def isFailure[A](a: IO.ApiIO[A]): Boolean =
        a.isFailure

      override def map[A, B](a: A)(f: A => B): IO.ApiIO[B] =
        IO(f(a))

      override def foreach[A, B](a: A)(f: A => B): Unit =
        f(a)

      override def flatMap[A, B](fa: IO.ApiIO[A])(f: A => IO.ApiIO[B]): IO.ApiIO[B] =
        fa.flatMap(f)

      override def success[A](value: A): IO.ApiIO[A] =
        IO.successful(value)

      override def failure[A](exception: Throwable): IO.ApiIO[A] =
        IO.failed(exception)

      override def exception[A](a: ApiIO[A]): Option[Throwable] =
        a.failed.map(_.exception).toOption

      override def getOrElse[A, B >: A](a: ApiIO[A])(b: => B): B =
        a.getOrElse(b)

      override def orElse[A, B >: A](a: ApiIO[A])(b: ApiIO[B]): ApiIO[B] =
        a.orElse(b)

      override def none[A]: IO.ApiIO[Option[A]] =
        IO.none

      override def foldLeft[A, U](initial: U, after: Option[A], stream: swaydb.Stream[A, IO.ApiIO], drop: Int, take: Option[Int])(operation: (U, A) => U): IO.ApiIO[U] = {
        @tailrec
        def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): IO.ApiIO[U] =
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
                        return IO.failed(exception)
                    }
                  fold(next, drop, currentSize + 1, nextResult)
                }

              case IO.Success(None) =>
                IO.Success(previousResult)

              case IO.Failure(error) =>
                IO.Failure(error)
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
                      return IO.failed(throwable)
                  }
                fold(first, drop, 1, next)
              }

            case IO.Success(None) =>
              IO.Success(initial)

            case IO.Failure(error) =>
              IO.Failure(error)
          }
      }

      @tailrec
      override def collectFirst[A](previous: A, stream: swaydb.Stream[A, IO.ApiIO])(condition: A => Boolean): IO.ApiIO[Option[A]] =
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
      override def fromIO[E: ErrorHandler, A](a: IO[E, A]): IO.ApiIO[A] =
        IO[Error.API, A](a.get)
    }

  implicit val tryTag: Tag.Sync[Try] = dbIO.toTag[Try]

  implicit val optionTag: Tag.Sync[Option] = dbIO.toTag[Option]

  implicit def future(implicit ec: ExecutionContext): Tag.Async[Future] =
    new Async[Future] {
      override def apply[A](a: => A): Future[A] =
        Future(a)

      override def map[A, B](a: A)(f: A => B): Future[B] =
        Future(f(a))

      override def flatMap[A, B](fa: Future[A])(f: A => Future[B]): Future[B] =
        fa.flatMap(f)

      override def success[A](value: A): Future[A] =
        Future.successful(value)

      override def failure[A](exception: Throwable): Future[A] =
        Future.failed(exception)

      override def none[A]: Future[Option[A]] =
        Future.successful(None)

      override def foreach[A, B](a: A)(f: A => B): Unit =
        f(a)

      def fromPromise[A](a: Promise[A]): Future[A] =
        a.future

      def isComplete[A](a: Future[A]): Boolean =
        a.isCompleted

      override def foldLeft[A, U](initial: U, after: Option[A], stream: swaydb.Stream[A, Future], drop: Int, take: Option[Int])(operation: (U, A) => U): Future[U] = {
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

      override def collectFirst[A](previous: A, stream: swaydb.Stream[A, Future])(condition: A => Boolean): Future[Option[A]] =
        stream.next(previous) flatMap {
          case some @ Some(nextA) =>
            if (condition(nextA))
              Future.successful(some)
            else
              collectFirst(nextA, stream)(condition)

          case None =>
            Future.successful(None)
        }

      override def fromIO[E: ErrorHandler, A](a: IO[E, A]): Future[A] = a.toFuture
    }
}
