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

  /**
   * For Async [[Tag]]s [[apply]] will always run asynchronously but to cover
   * cases where the operation might already be executed [[point]] is used.
   *
   * @example All SwayDB writes occur synchronously using [[IO]]. Running completed [[IO]] in a [[Future]]
   *          will have a performance cost. [[point]] is used to cover these cases and [[IO]]
   *          types that are complete are directly converted to Future in current thread.
   */
  def point[B](f: => T[B]): T[B] =
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

      @inline def foreach[B](f: A => B): Unit =
        tag.flatMap(a) {
          value =>
            tag.foreach(value)(f)
            a
        }
    }
  }

  /**
   * Converts containers. More tags can be created from existing Tags with this trait using [[Tag.toTag]]
   */
  trait Converter[A[_], B[_]] {
    def to[T](a: A[T]): B[T]
    def from[T](a: B[T]): A[T]
  }

  object Converter {
    implicit val optionToTry = new Converter[Option, Try] {
      override def to[T](a: Option[T]): Try[T] =
        tryTag(a.get)

      override def from[T](a: Try[T]): Option[T] =
        a.toOption
    }

    implicit val tryToOption = new Converter[Try, Option] {
      override def to[T](a: Try[T]): Option[T] =
        a.toOption

      override def from[T](a: Option[T]): Try[T] =
        tryTag(a.get)
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

    implicit val throwableToApiIO = new Tag.Converter[IO.ThrowableIO, IO.ApiIO] {
      override def to[T](a: IO.ThrowableIO[T]): IO.ApiIO[T] =
        IO[swaydb.Error.API, T](a.get)

      override def from[T](a: IO.ApiIO[T]): IO.ThrowableIO[T] =
        IO(a.get)
    }

    implicit val throwableToTry = new Tag.Converter[IO.ThrowableIO, Try] {
      override def to[T](a: IO.ThrowableIO[T]): Try[T] =
        a.toTry

      override def from[T](a: Try[T]): IO.ThrowableIO[T] =
        IO.fromTry(a)
    }

    implicit val throwableToOption = new Tag.Converter[IO.ThrowableIO, Option] {
      override def to[T](a: IO.ThrowableIO[T]): Option[T] =
        a.toOption

      override def from[T](a: Option[T]): IO.ThrowableIO[T] =
        IO(a.get)
    }

    implicit val throwableToUnit = new Tag.Converter[IO.ThrowableIO, IO.UnitIO] {
      override def to[T](a: IO.ThrowableIO[T]): IO.UnitIO[T] =
        IO[Unit, T](a.get)(ErrorHandler.Unit)

      override def from[T](a: IO.UnitIO[T]): IO.ThrowableIO[T] =
        IO(a.get)
    }

    implicit val throwableToNothing = new Tag.Converter[IO.ThrowableIO, IO.NothingIO] {
      override def to[T](a: IO.ThrowableIO[T]): IO.NothingIO[T] =
        IO[Nothing, T](a.get)(ErrorHandler.Nothing)

      override def from[T](a: IO.NothingIO[T]): IO.ThrowableIO[T] =
        IO(a.get)
    }
  }

  private sealed trait ToTagBase[T[_], X[_]] {
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
      new Tag.Sync[X] with ToTagBase[T, X] {
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
      new Tag.Async[X] with ToTagBase[T, X] {
        override val base: Tag[T] = self

        override val baseConverter: Converter[T, X] = converter

        override def fromPromise[A](a: Promise[A]): X[A] =
          converter.to(self.fromPromise(a))

        override def isComplete[A](a: X[A]): Boolean =
          self.isComplete(converter.from(a))
      }
  }

  implicit val throwableIO: Tag.Sync[IO.ThrowableIO] =
    new Tag.Sync[IO.ThrowableIO] {

      override def apply[A](a: => A): IO.ThrowableIO[A] =
        IO(a)

      def isSuccess[A](a: IO.ThrowableIO[A]): Boolean =
        a.isRight

      def isFailure[A](a: IO.ThrowableIO[A]): Boolean =
        a.isLeft

      override def map[A, B](a: A)(f: A => B): IO.ThrowableIO[B] =
        IO(f(a))

      override def foreach[A, B](a: A)(f: A => B): Unit =
        f(a)

      override def flatMap[A, B](fa: IO.ThrowableIO[A])(f: A => IO.ThrowableIO[B]): IO.ThrowableIO[B] =
        fa.flatMap(f)

      override def success[A](value: A): IO.ThrowableIO[A] =
        IO.successful(value)

      override def failure[A](exception: Throwable): IO.ThrowableIO[A] =
        IO.left(exception)

      override def exception[A](a: IO.ThrowableIO[A]): Option[Throwable] =
        a.left.toOption

      override def getOrElse[A, B >: A](a: IO.ThrowableIO[A])(b: => B): B =
        a.getOrElse(b)

      override def orElse[A, B >: A](a: IO.ThrowableIO[A])(b: IO.ThrowableIO[B]): IO.ThrowableIO[B] =
        a.orElse(b)

      override def none[A]: IO.ThrowableIO[Option[A]] =
        IO.none

      override def foldLeft[A, U](initial: U, after: Option[A], stream: swaydb.Stream[A, IO.ThrowableIO], drop: Int, take: Option[Int])(operation: (U, A) => U): IO.ThrowableIO[U] = {
        @tailrec
        def fold(previous: A, drop: Int, currentSize: Int, previousResult: U): IO.ThrowableIO[U] =
          if (take.contains(currentSize))
            IO.Right(previousResult)
          else
            stream.next(previous) match {
              case IO.Right(Some(next)) =>
                if (drop >= 1) {
                  fold(next, drop - 1, currentSize, previousResult)
                } else {
                  val nextResult =
                    try {
                      operation(previousResult, next)
                    } catch {
                      case exception: Throwable =>
                        return IO.left(exception)
                    }
                  fold(next, drop, currentSize + 1, nextResult)
                }

              case IO.Right(None) =>
                IO.Right(previousResult)

              case IO.Left(error) =>
                IO.Left(error)
            }

        if (take.contains(0))
          IO.Right(initial)
        else
          after.map(stream.next).getOrElse(stream.headOption) match {
            case IO.Right(Some(first)) =>
              if (drop >= 1)
                fold(first, drop - 1, 0, initial)
              else {
                val next =
                  try {
                    operation(initial, first)
                  } catch {
                    case throwable: Throwable =>
                      return IO.left(throwable)
                  }
                fold(first, drop, 1, next)
              }

            case IO.Right(None) =>
              IO.Right(initial)

            case IO.Left(error) =>
              IO.Left(error)
          }
      }

      @tailrec
      override def collectFirst[A](previous: A, stream: swaydb.Stream[A, IO.ThrowableIO])(condition: A => Boolean): IO.ThrowableIO[Option[A]] =
        stream.next(previous) match {
          case success @ IO.Right(Some(nextA)) =>
            if (condition(nextA))
              success
            else
              collectFirst(nextA, stream)(condition)

          case none @ IO.Right(None) =>
            none

          case failure @ IO.Left(_) =>
            failure
        }
      override def fromIO[E: ErrorHandler, A](a: IO[E, A]): IO.ThrowableIO[A] =
        IO[Throwable, A](a.get)
    }

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

  implicit val apiIO: Tag.Sync[IO.ApiIO] = throwableIO.toTag[IO.ApiIO]

  implicit val unit: Tag.Sync[IO.UnitIO] = throwableIO.toTag[IO.UnitIO]

  implicit val nothing: Tag.Sync[IO.UnitIO] = throwableIO.toTag[IO.UnitIO]

  implicit val tryTag: Tag.Sync[Try] = throwableIO.toTag[Try]

  implicit val option: Tag.Sync[Option] = throwableIO.toTag[Option]
}
