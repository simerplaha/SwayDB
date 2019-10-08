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

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{Consumer, Predicate, Supplier}

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.Reserve
import swaydb.data.slice.Slice
import swaydb.data.util.Javaz.JavaFunction

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.mutable.ListBuffer
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * [[IO.Right]] and [[IO.Left]] are similar to types in [[scala.util.Try]].
 *
 * [[IO.Defer]] is for performing synchronous and asynchronous IO.
 */

sealed trait IO[+L, +R] {
  def isLeft: Boolean
  def isRight: Boolean

  @throws[Throwable]
  def get: R

  def getOrElse[B >: R](default: => B): B
  def javaGetOrElse[B >: R](supplier: Supplier[B]): B = getOrElse(supplier.asScala())

  def orElse[L2 >: L : IO.ExceptionHandler, B >: R](default: => IO[L2, B]): IO[L2, B]
  def javaOrElse[L2 >: L, B >: R](supplier: Supplier[IO[L2, B]])(implicit exceptionHandler: IO.ExceptionHandler[L2]): IO[L2, B] =
    orElse(supplier.asScala())

  def foreach[B](f: R => B): Unit
  def javaForeach(function: Consumer[R@uncheckedVariance]): Unit =
    foreach(function.asScala)

  def map[B](f: R => B): IO[L, B]
  def javaMap[B](function: JavaFunction[R, B]@uncheckedVariance): IO[L, B] =
    map(function.asScala)

  def flatMap[L2 >: L : IO.ExceptionHandler, B](f: R => IO[L2, B]): IO[L2, B]

  def javaFlatMap[L2 >: L, B](function: JavaFunction[R, IO[L2, B]]@uncheckedVariance)(implicit exceptionHandler: IO.ExceptionHandler[L2]): IO[L2, B] =
    flatMap(function.asScala)

  def exists(f: R => Boolean): Boolean
  def javaExists(predicate: Predicate[R]@uncheckedVariance): Boolean =
    exists(predicate.asScala)

  def filter(p: R => Boolean): IO[L, R]
  def javaFilter(predicate: Predicate[R]@uncheckedVariance) =
    filter(predicate.asScala)

  def valueOrElse[B](f: R => B, orElse: => B): B
  def javaValueOrElse[B](function: JavaFunction[R, B]@uncheckedVariance, orElse: Supplier[B]): B =
    valueOrElse(function.asScala, orElse.asScala())

  @inline final def withFilter(p: R => Boolean): WithFilter = new WithFilter(p)
  class WithFilter(p: R => Boolean) {
    def map[B](f: R => B): IO[L, B] =
      IO.this filter p map f

    def flatMap[L2 >: L : IO.ExceptionHandler, B](f: R => IO[L2, B]): IO[L2, B] =
      IO.this filter p flatMap f

    def foreach[B](f: R => B): Unit =
      IO.this filter p foreach f

    def withFilter(q: R => Boolean): WithFilter =
      new WithFilter(x => p(x) && q(x))
  }

  def left: IO[Throwable, L]
  def right: IO[Throwable, R]

  def recoverWith[L2 >: L : IO.ExceptionHandler, B >: R](f: PartialFunction[L, IO[L2, B]]): IO[L2, B]
  def javaRecoverWith[L2 >: L](function: JavaFunction[L, IO[L2, R]]@uncheckedVariance)(implicit exceptionHandler: IO.ExceptionHandler[L2]): IO[L2, R] =
    recoverWith {
      case result =>
        function.apply(result)
    }

  def recover[B >: R](f: PartialFunction[L, B]): IO[L, B]
  def javaRecover(function: JavaFunction[L, R]@uncheckedVariance): IO[L, R] =
    recover {
      case result =>
        function.apply(result)
    }

  def flatten[L2, R2](implicit ev: R <:< IO[L2, R2]): IO[L2, R2]

  def onLeftSideEffect(f: IO.Left[L, R] => Unit): IO[L, R]
  def javaOnLeftSideEffect(consumer: Consumer[IO.Left[L, R]]@uncheckedVariance): IO[L, R] =
    onLeftSideEffect(consumer.asScala)

  def onRightSideEffect(f: R => Unit): IO[L, R]
  def javaOnRightSideEffect(consumer: Consumer[R]@uncheckedVariance): IO[L, R] =
    onRightSideEffect(consumer.asScala)

  def onCompleteSideEffect(f: IO[L, R] => Unit): IO[L, R]
  def javaOnCompleteSideEffect(consumer: Consumer[IO[L, R]]@uncheckedVariance): IO[L, R] =
    onCompleteSideEffect(consumer.asScala)

  def toOption: Option[R]
  def javaToOptional: Optional[R@uncheckedVariance] =
    toOption.asJava

  def toOptionValue: IO[L, Option[R]]
  def javaToOptionValue: IO[L, Optional[R]@uncheckedVariance] =
    toOptionValue.map(_.asJava)

  def toEither: Either[L, R]
  def toFuture: Future[R]
  def javaToFuture: CompletionStage[R@uncheckedVariance] =
    toFuture.toJava

  def toTry: scala.util.Try[R]
  def toDefer[L2 >: L : IO.ExceptionHandler]: IO.Defer[L2, R] =
    IO.Defer.io[L2, R](io = this)

  def javaToDefer(implicit exceptionHandler: IO.ExceptionHandler[L@uncheckedVariance]): IO.Defer[L, R] =
    IO.Defer.io[L, R](io = this)
}

object IO {

  /**
   * [[IO]] type with Throwable error type. Here all errors are returned as exception.
   */
  type ThrowableIO[T] = IO[Throwable, T]
  /**
   * [[IO]] type with Nothing error type. Nothing indicates this IO type can never result in an error.
   */
  type NothingIO[T] = IO[Nothing, T]
  /**
   * [[IO]] type with Unit error type. Unit indicates this IO type can never result in an error.
   */
  type UnitIO[T] = IO[Unit, T]
  /**
   * [[IO]] type used to access database APIs.
   */
  type ApiIO[T] = IO[Error.API, T]
  /**
   * [[IO]] type for database boot up.
   */
  type BootIO[T] = IO[Error.Boot, T]

  sealed trait Done
  final case object Done extends Done

  val unit: IO.Right[Nothing, Unit] = IO.Right()(IO.ExceptionHandler.Nothing)
  val unitUnit: IO.Right[Nothing, IO.Right[Nothing, Unit]] = IO.Right(IO.Right()(IO.ExceptionHandler.Nothing))(IO.ExceptionHandler.Nothing)
  val none: IO.Right[Nothing, Option[Nothing]] = IO.Right(None)(IO.ExceptionHandler.Nothing)
  val `false`: IO.Right[Nothing, Boolean] = IO.Right(false)(IO.ExceptionHandler.Nothing)
  val `true`: IO.Right[Nothing, Boolean] = IO.Right(true)(IO.ExceptionHandler.Nothing)
  val someTrue: IO[Nothing, Some[Boolean]] = IO.Right(Some(true))(IO.ExceptionHandler.Nothing)
  val someFalse: IO[Nothing, Some[Boolean]] = IO.Right(Some(false))(IO.ExceptionHandler.Nothing)
  val zero: IO.Right[Nothing, Int] = IO.Right(0)(IO.ExceptionHandler.Nothing)
  val zeroZero: IO[Nothing, IO.Right[Nothing, Int]] = IO.Right(IO.Right(0)(IO.ExceptionHandler.Nothing))(IO.ExceptionHandler.Nothing)
  val emptyBytes: IO.Right[Nothing, Slice[Byte]] = IO.Right(Slice.emptyBytes)(IO.ExceptionHandler.Nothing)
  val emptySeqBytes: IO.Right[Nothing, Seq[Slice[Byte]]] = IO.Right(Seq.empty[Slice[Byte]])(IO.ExceptionHandler.Nothing)
  val done: IO.Right[Nothing, Done] = IO.Right(Done)(IO.ExceptionHandler.Nothing)

  implicit class IterableIOImplicit[E: IO.ExceptionHandler, A: ClassTag](iterable: Iterable[A]) {

    def foreachIO[R](f: A => IO[E, R], failFast: Boolean = true): Option[IO.Left[E, R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Left[E, R]] = None
      while (it.hasNext && (failure.isEmpty || !failFast)) {
        f(it.next()) onLeftSideEffect {
          case failed @ IO.Left(_) =>
            failure = Some(failed)
        }
      }
      failure
    }

    //returns the first IO.Right(Some(_)).
    def untilSome[R](f: A => IO[E, Option[R]]): IO[E, Option[(R, A)]] = {
      iterable.iterator foreach {
        item =>
          f(item) match {
            case IO.Right(Some(value)) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Right[E, Option[(R, A)]](Some(value, item))

            case IO.Right(None) =>
            //continue reading

            case IO.Left(error) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Left(error)
          }
      }
      IO.none
    }

    def untilSomeValue[R](f: A => IO[E, Option[R]]): IO[E, Option[R]] = {
      iterable.iterator foreach {
        item =>
          f(item) match {
            case IO.Right(Some(value)) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Right[E, Option[R]](Some(value))

            case IO.Right(None) =>
            //continue reading

            case IO.Left(error) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Left(error)
          }
      }
      IO.none
    }

    def mapRecoverIO[R: ClassTag](block: A => IO[E, R],
                                  recover: (Slice[R], IO.Left[E, Slice[R]]) => Unit = (_: Slice[R], _: IO.Left[E, Slice[R]]) => (),
                                  failFast: Boolean = true): IO[E, Slice[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Left[E, Slice[R]]] = None
      val results = Slice.create[R](iterable.size)

      while ((!failFast || failure.isEmpty) && it.hasNext) {
        block(it.next()) match {
          case IO.Right(value) =>
            results add value

          case failed @ IO.Left(_) =>
            failure = Some(IO.Left[E, Slice[R]](failed.value))
        }
      }

      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Right[E, Slice[R]](results)
      }
    }

    def mapRecover[R: ClassTag](block: A => R,
                                recover: (Slice[R], Throwable) => Unit = (_: Slice[R], _: Throwable) => (),
                                failFast: Boolean = true): Slice[R] = {
      val it = iterable.iterator
      var failure: Option[Throwable] = None
      val successes = Slice.create[R](iterable.size)

      while ((!failFast || failure.isEmpty) && it.hasNext) {
        try
          successes add block(it.next())
        catch {
          case exception: Throwable =>
            failure = Some(exception)
        }
      }

      failure match {
        case Some(throwable) =>
          recover(successes, throwable)
          throw throwable

        case None =>
          successes
      }
    }

    def flatMapRecoverIO[R](ioBlock: A => IO[E, Iterable[R]],
                            recover: (Iterable[R], IO.Left[E, Slice[R]]) => Unit = (_: Iterable[R], _: IO.Left[E, Iterable[R]]) => (),
                            failFast: Boolean = true): IO[E, Iterable[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Left[E, Slice[R]]] = None
      val results = ListBuffer.empty[R]

      while ((!failFast || failure.isEmpty) && it.hasNext) {
        ioBlock(it.next()) match {
          case IO.Right(value) =>
            value foreach (results += _)

          case failed @ IO.Left(_) =>
            failure = Some(IO.Left[E, Slice[R]](failed.value))
        }
      }

      failure match {
        case Some(value) =>
          recover(results, value)
          value

        case None =>
          IO.Right[E, Iterable[R]](results)
      }
    }

    def flatMapRecoverThrowable[R](ioBlock: A => Iterable[R],
                                   recover: (Iterable[R], Throwable) => Unit = (_: Iterable[R], _: Throwable) => (),
                                   failFast: Boolean = true): Iterable[R] = {
      val it = iterable.iterator
      var failure: Option[Throwable] = None
      val results = ListBuffer.empty[R]

      while ((!failFast || failure.isEmpty) && it.hasNext)
        try
          ioBlock(it.next()) foreach (results += _)
        catch {
          case throwable: Throwable =>
            failure = Some(throwable)
        }

      failure match {
        case Some(throwable) =>
          recover(results, throwable)
          throw throwable

        case None =>
          results
      }
    }

    def foldLeftRecoverIO[R](r: R,
                             failFast: Boolean = true,
                             recover: (R, IO.Left[E, R]) => Unit = (_: R, _: IO.Left[E, R]) => ())(f: (R, A) => IO[E, R]): IO[E, R] = {
      val it = iterable.iterator
      var failure: Option[IO.Left[E, R]] = None
      var result: R = r

      while ((!failFast || failure.isEmpty) && it.hasNext) {
        f(result, it.next()) match {
          case IO.Right(value) =>
            if (failure.isEmpty)
              result = value

          case failed @ IO.Left(_) =>
            failure = Some(IO.Left[E, R](failed.value))
        }
      }

      failure match {
        case Some(failure) =>
          recover(result, failure)
          failure

        case None =>
          IO.Right[E, R](result)
      }
    }

    def foldLeftRecover[R](r: R,
                           failFast: Boolean = true,
                           recover: (R, Throwable) => Unit = (_: R, _: Throwable) => ())(f: (R, A) => R): R = {
      val it = iterable.iterator
      var failure: Option[Throwable] = None
      var result: R = r

      while ((!failFast || failure.isEmpty) && it.hasNext)
        try
          result = f(result, it.next())
        catch {
          case throwable: Throwable =>
            failure = Some(throwable)
        }

      failure match {
        case Some(failure) =>
          recover(result, failure)
          throw failure

        case None =>
          result
      }
    }
  }

  @inline final def tryOrNone[A](block: => A): Option[A] =
    try
      Option(block)
    catch {
      case _: Throwable =>
        None
    }

  @inline def when[E: IO.ExceptionHandler, T, F](condition: => Boolean, onTrue: => IO[E, T], onFalse: => T)(f: T => IO[E, F]): IO[E, F] =
    if (condition)
      onTrue flatMap f
    else
      f(onFalse)

  @inline def when[E: IO.ExceptionHandler, T](condition: => Boolean, onTrue: => IO[E, T], onFalse: => IO[E, T]): IO[E, T] =
    if (condition)
      onTrue
    else
      onFalse

  @inline final def javaApply[E, A](supplier: Supplier[A])(implicit exceptionHandler: IO.ExceptionHandler[E]): IO[E, A] =
    apply(supplier.asScala())

  @inline final def javaApply[A](supplier: Supplier[A]): IO[Throwable, A] =
    apply[Throwable, A](supplier.asScala())(IO.ExceptionHandler.Throwable)

  @inline final def apply[E: IO.ExceptionHandler, A](f: => A): IO[E, A] =
    try IO.Right[E, A](f) catch {
      case ex: Throwable =>
        IO.Left(value = IO.ExceptionHandler.toError[E](ex))
    }

  object Catch {
    @inline final def apply[E: IO.ExceptionHandler, A](f: => IO[E, A]): IO[E, A] =
      try
        f
      catch {
        case ex: Throwable =>
          IO.Left(value = IO.ExceptionHandler.toError[E](ex))
      }
  }

  def fromTry[E: IO.ExceptionHandler, A](tryBlock: Try[A]): IO[E, A] =
    tryBlock match {
      case scala.util.Success(value) =>
        IO.Right[E, A](value)

      case scala.util.Failure(exception) =>
        IO.Left[E, A](value = IO.ExceptionHandler.toError[E](exception))
    }

  def javaExceptionHandler(): ExceptionHandler.type =
    ExceptionHandler

  trait ExceptionHandler[E] {
    def toException(f: E): Throwable
    def toError(e: Throwable): E
    protected[swaydb] def recover(f: E): Option[Reserve[Unit]] = None
  }

  trait RecoverableExceptionHandler[E] extends IO.ExceptionHandler[E] {
    def recoverFrom(f: E): Option[Reserve[Unit]]

    override protected[swaydb] def recover(f: E): Option[Reserve[Unit]] =
      recoverFrom(f)
  }

  object ExceptionHandler extends LazyLogging {
    def toException[E](error: E)(implicit errorHandler: IO.ExceptionHandler[E]): Throwable =
      errorHandler.toException(error)

    def toError[E](exception: Throwable)(implicit errorHandler: IO.ExceptionHandler[E]): E =
      errorHandler.toError(exception)

    def recover[E](e: E)(implicit errorHandler: IO.ExceptionHandler[E]): Option[Reserve[Unit]] =
      try
        errorHandler.recover(e)
      catch {
        case exception: Throwable =>
          logger.error("Failed to fetch Reserve. Stopping recovery.", exception)
          scala.None
      }

    object Nothing extends IO.ExceptionHandler[Nothing] {
      override def toError(e: Throwable): Nothing =
        throw new scala.Exception("Nothing cannot be created from Exception.", e)

      override def toException(f: Nothing): Throwable =
        new Exception("Nothing value.")
    }

    object Unit extends IO.ExceptionHandler[Unit] {
      override def toError(e: Throwable): Unit =
        throw new scala.Exception("Unit cannot be created from Exception.", e)

      override def toException(f: Unit): Throwable =
        new Exception("Unit value.")
    }

    object None extends IO.ExceptionHandler[scala.None.type] {
      override def toError(e: Throwable): scala.None.type =
        throw new scala.Exception("None cannot be created from Exception.", e)

      override def toException(f: scala.None.type): Throwable =
        new Exception("None value.")
    }

    def javaThrowable(): IO.ExceptionHandler[Throwable] = Throwable

    implicit object Throwable extends IO.ExceptionHandler[Throwable] {
      override def toError(e: Throwable): Throwable =
        e

      override def toException(f: Throwable): Throwable =
        f
    }

    trait Promise[T] extends IO.ExceptionHandler[scala.concurrent.Promise[T]] {
      override def toException(promise: scala.concurrent.Promise[T]): Throwable =
        promise.future.value match {
          case Some(scala.util.Failure(failure)) =>
            failure

          case Some(scala.util.Success(_)) =>
            new Exception("Exception cannot be created from successful Promise.")

          case scala.None =>
            new Exception("Exception cannot be created from an incomplete Promise.")
        }

      override def toError(e: Throwable): scala.concurrent.Promise[T] =
        scala.concurrent.Promise.failed(e)
    }

    object PromiseUnit extends IO.ExceptionHandler.Promise[Unit]
  }

  /** *********************
   * **********************
   * **********************
   * ******** RIGHT *******
   * **********************
   * **********************
   * **********************
   */

  final case class Right[+L: IO.ExceptionHandler, +R](value: R) extends IO[L, R] {
    override def get: R =
      value

    override def isLeft: Boolean =
      false

    override def isRight: Boolean =
      true

    override def left: IO.Left[Throwable, L] =
      IO.Left[Throwable, L](new UnsupportedOperationException("Value is IO.Right"))(IO.ExceptionHandler.Throwable)

    override def right: IO.Right[Throwable, R] =
      IO.Right[Throwable, R](get)(IO.ExceptionHandler.Throwable)

    override def exists(f: R => Boolean): Boolean =
      f(value)

    override def getOrElse[B >: R](default: => B): B =
      get

    override def orElse[F >: L : IO.ExceptionHandler, B >: R](default: => IO[F, B]): IO.Right[F, B] =
      this

    override def valueOrElse[B](f: R => B, orElse: => B): B =
      f(value)

    override def foreach[B](f: R => B): Unit =
      f(get)

    override def map[B](f: R => B): IO[L, B] =
      IO[L, B](f(get))

    override def flatMap[F >: L : IO.ExceptionHandler, B](f: R => IO[F, B]): IO[F, B] =
      IO.Catch(f(get))

    override def flatten[F, B](implicit ev: R <:< IO[F, B]): IO[F, B] =
      get

    override def recover[B >: R](f: PartialFunction[L, B]): IO[L, B] =
      this

    override def recoverWith[F >: L : IO.ExceptionHandler, B >: R](f: PartialFunction[L, IO[F, B]]): IO[F, B] =
      this

    override def toOption: Option[R] =
      Some(get)

    def toOptionValue: IO[L, Option[R]] =
      IO.Right(Some(value))

    override def toEither: Either[L, R] =
      scala.util.Right(get)

    override def filter(p: R => Boolean): IO[L, R] =
      IO.Catch(if (p(get)) this else IO.failed[L, R](new NoSuchElementException("Predicate does not hold for " + get)))

    override def toFuture: Future[R] =
      Future.successful(get)

    override def toTry: scala.util.Try[R] =
      scala.util.Success(get)

    override def onLeftSideEffect(f: IO.Left[L, R] => Unit): IO.Right[L, R] =
      this

    override def onRightSideEffect(f: R => Unit): IO.Right[L, R] = {
      try f(get) finally {}
      this
    }

    override def onCompleteSideEffect(f: IO[L, R] => Unit): IO[L, R] = {
      try f(this) finally {}
      this
    }
  }

  @inline final def throwable(message: String): Throwable =
    new Exception(message)

  @inline final def throwable(message: String, inner: Throwable): Throwable =
    new Exception(message, inner)

  @inline final def failed[E: IO.ExceptionHandler, A](exception: Throwable): IO.Left[E, A] =
    new IO.Left[E, A](IO.ExceptionHandler.toError[E](exception))

  @inline final def failed[E: IO.ExceptionHandler, A](exceptionMessage: String): IO.Left[E, A] =
    new IO.Left[E, A](IO.ExceptionHandler.toError[E](new scala.Exception(exceptionMessage)))

  /** *********************
   * **********************
   * **********************
   * ******** LEFT ********
   * **********************
   * **********************
   * **********************
   */

  final case class Left[+L: IO.ExceptionHandler, +R](value: L) extends IO[L, R] {
    def exception: Throwable =
      IO.ExceptionHandler.toException(value)

    @throws[Throwable]
    override def get: R =
      throw exception

    override def isLeft: Boolean =
      true

    override def isRight: Boolean =
      false

    override def left: IO.Right[Throwable, L] =
      IO.Right[Throwable, L](value)(IO.ExceptionHandler.Throwable)

    override def right: IO.Left[Throwable, R] =
      IO.Left[Throwable, R](new UnsupportedOperationException("Value is IO.Left", IO.ExceptionHandler.toException(value)))(IO.ExceptionHandler.Throwable)

    def isRecoverable =
      IO.ExceptionHandler.recover(value).isDefined

    override def exists(f: R => Boolean): Boolean =
      false

    override def getOrElse[B >: R](default: => B): B =
      default

    def valueOrElse[B](f: R => B, orElse: => B): B =
      orElse

    override def orElse[L2 >: L : IO.ExceptionHandler, B >: R](default: => IO[L2, B]): IO[L2, B] =
      IO.Catch(default)

    override def foreach[B](f: R => B): Unit =
      ()

    override def map[B](f: R => B): IO.Left[L, B] =
      this.asInstanceOf[IO.Left[L, B]]

    override def flatMap[F >: L : IO.ExceptionHandler, B](f: R => IO[F, B]): IO.Left[F, B] =
      this.asInstanceOf[IO.Left[F, B]]

    override def flatten[F, B](implicit ev: R <:< IO[F, B]): IO.Left[F, B] =
      this.asInstanceOf[IO.Left[F, B]]

    override def recover[B >: R](f: PartialFunction[L, B]): IO[L, B] =
      IO.Catch {
        if (f isDefinedAt value)
          IO.Right[L, B](f(value))
        else
          this
      }

    override def recoverWith[F >: L : IO.ExceptionHandler, B >: R](f: PartialFunction[L, IO[F, B]]): IO[F, B] =
      IO.Catch {
        if (f isDefinedAt value)
          f(value)
        else
          this
      }

    def recoverTo[F >: L : IO.ExceptionHandler, B](io: => IO.Defer[F, B]): IO.Defer[F, B] =
      if (this.isRecoverable)
        IO.Defer[F, B](io.toIO.get, this.value)
      else
        IO.Defer[F, B](throw IO.ExceptionHandler.toException(this.value))

    override def toOption: Option[R] =
      None

    def toOptionValue: IO[L, Option[R]] =
      IO.Left(value)

    override def toEither: Either[L, R] =
      scala.util.Left(value)

    override def filter(p: R => Boolean): IO.Left[L, R] =
      this

    override def toFuture: Future[R] =
      Future.failed(exception)

    override def toTry: scala.util.Try[R] =
      scala.util.Failure(exception)

    override def onLeftSideEffect(f: IO.Left[L, R] => Unit): IO.Left[L, R] = {
      try f(this) finally {}
      this
    }

    override def onCompleteSideEffect(f: IO[L, R] => Unit): IO[L, R] =
      onLeftSideEffect(f)

    override def onRightSideEffect(f: R => Unit): IO.Left[L, R] =
      this
  }

  def fromFuture[L: IO.ExceptionHandler, R](future: Future[R])(implicit ec: ExecutionContext): IO.Defer[L, R] = {
    val reserve = Reserve.busy((), "Future reserve")
    future onComplete {
      _ =>
        Reserve.setFree(reserve)
    }

    /**
     * [[deferredValue]] will only be invoked the reserve is set free which occurs only when the future is complete.
     * But functions like [[IO.Defer.getUnsafe]] will try to access this before the Future is complete will result in failure.
     *
     * This is necessary to avoid blocking Futures.
     */
    def deferredValue =
      future.value map {
        case scala.util.Success(value) =>
          //success
          value

        case scala.util.Failure(exception) =>
          //throw Future's failure so deferred can react to this.
          //wrap it in another Exception incase the inner exception is recoverable because the future is complete
          //and cannot be recovered.
          throw new Exception(exception)
      } getOrElse {
        //Accessing Future when its incomplete.
        throw swaydb.Exception.GetOnIncompleteDeferredFutureIO(reserve)
      }

    val error = swaydb.Error.ReservedResource(reserve)

    //Deferred instance that will handle the outcome of the Future
    val recoverableDeferred =
      IO.Defer[swaydb.Error.Segment, R](
        value = deferredValue,
        error = error
      )

    //Deferred that returns the result of the above deferred when completed.
    IO.Defer[L, R](recoverableDeferred.toIO.get)
  }

  /** **********************************
   * **********************************
   * **********************************
   * ************ DEFERRED ************
   * **********************************
   * **********************************
   * **********************************/

  object Defer extends LazyLogging {

    val maxRecoveriesBeforeWarn = 5

    val none: IO.Defer[Nothing, None.type] = new IO.Defer[Nothing, None.type](() => None, None)(swaydb.IO.ExceptionHandler.Nothing)
    val done: IO.Defer[Nothing, Done] = new IO.Defer[Nothing, Done](() => Done, None)(swaydb.IO.ExceptionHandler.Nothing)
    val unit: IO.Defer[Nothing, Unit] = new IO.Defer[Nothing, Unit](() => (), None)(swaydb.IO.ExceptionHandler.Nothing)
    val zero: IO.Defer[Nothing, Int] = new IO.Defer[Nothing, Int](() => 0, None)(swaydb.IO.ExceptionHandler.Nothing)

    @inline final def apply[E: IO.ExceptionHandler, A](value: => A): IO.Defer[E, A] =
      new Defer(() => value, None)

    @inline final def apply[E: IO.ExceptionHandler, A](value: => A, error: E): IO.Defer[E, A] =
      new Defer(() => value, Some(error))

    @inline final def io[E: IO.ExceptionHandler, A](io: => IO[E, A]): IO.Defer[E, A] =
      new IO.Defer(() => io.get, None)

    @inline private final def runAndRecover[E: IO.ExceptionHandler, A](f: IO.Defer[E, A]): Either[IO[E, A], IO.Defer[E, A]] =
      try
        scala.util.Left(IO.Right[E, A](f.getUnsafe))
      catch {
        case ex: Throwable =>
          val error = IO.ExceptionHandler.toError[E](ex)
          if (IO.ExceptionHandler.recover(error).isDefined)
            scala.util.Right(f.copy(error = Some(error)))
          else
            scala.util.Left(IO.Left(error))
      }
  }

  final case class Defer[+E: IO.ExceptionHandler, +A] private(private val operation: () => A,
                                                              error: Option[E],
                                                              private val recovery: Option[_ => IO.Defer[E, A]] = None) extends LazyLogging {

    @volatile private var _value: Option[Any] = None

    private def getValue = _value.map(_.asInstanceOf[A])

    //a deferred IO is completed if it's not reserved.
    def isReady: Boolean =
      error forall (IO.ExceptionHandler.recover[E](_).forall(_.isFree))

    def isBusy =
      !isReady

    def isComplete: Boolean =
      getValue.isDefined

    def isPending: Boolean =
      !isComplete

    def isSuccess: Boolean =
      isComplete || toIO.isRight

    def isFailure: Boolean =
      isPending && toIO.isLeft

    private[Defer] def getUnsafe: A = {
      //Runs composed functions does not perform any recovery.
      def forceGet: A =
        getValue getOrElse {
          val got = operation()
          _value = Some(got)
          got
        }

      if (_value.isDefined || !isBusy)
        forceGet
      else
        error map {
          error =>
            throw IO.ExceptionHandler.toException[E](error)
        } getOrElse forceGet
    }

    def toIO: IO[E, A] =
      IO(getUnsafe)

    def run[B >: A, T[_]](implicit tag: Tag[T]): T[B] =
      tag match {
        case sync: Tag.Sync[T] =>
          runSync(sync)

        //if a tag is provided that implements isComplete then execute it directly
        case async: Tag.Async.Retryable[T] =>
          runAsync(async)

        case async: Tag.Async[T] =>
          //else run the Tag as Future and return it's result.
          implicit val ec = async.executionContext
          async.fromFuture(runAsync[B, Future])
      }

    /**
     * Opens all [[IO.Defer]] types to read the final value in a blocking manner.
     */
    def runIO: IO[E, A] = {

      def blockIfNeeded(deferred: IO.Defer[E, A]): Unit =
        deferred.error foreach {
          error =>
            ExceptionHandler.recover(error) foreach {
              reserve =>
                logger.trace(s"Blocking. ${reserve.name}")
                Reserve.blockUntilFree(reserve)
                logger.trace(s"Freed. ${reserve.name}")
            }
        }

      @tailrec
      def doRun(deferred: IO.Defer[E, A], tried: Int): IO[E, A] = {
        if (tried > 0) blockIfNeeded(deferred)
        IO.Defer.runAndRecover(deferred) match {
          case scala.util.Left(io) =>
            logger.trace(s"Run! isCached: ${getValue.isDefined}. ${io.getClass.getSimpleName}")
            io match {
              case success @ IO.Right(_) =>
                success

              case IO.Left(error) =>
                logger.trace(s"Run! isCached: ${getValue.isDefined}. ${io.getClass.getSimpleName}")
                if (recovery.isDefined) //pattern matching is not allowing @tailrec. So .get is required here.
                  doRun(recovery.get.asInstanceOf[E => IO.Defer[E, A]](error), 0)
                else
                  io
            }

          case scala.util.Right(deferred) =>
            logger.trace(s"Retry! isCached: ${getValue.isDefined}. ${deferred.error}")
            if (tried > 0 && tried % IO.Defer.maxRecoveriesBeforeWarn == 0)
              logger.warn(s"${Thread.currentThread().getName}: Competing reserved resource accessed via IO. Times accessed: $tried. Reserve: ${deferred.error.flatMap(error => IO.ExceptionHandler.recover(error).map(_.name))}")
            doRun(deferred, tried + 1)
        }
      }

      doRun(this, 0)
    }

    /**
     * TODO -  Similar to [[runIO]]. [[runIO]] should be calling this function
     * to build it's execution process.
     */
    private def runSync[B >: A, T[_]](implicit tag: Tag.Sync[T]): T[B] = {

      def blockIfNeeded(deferred: IO.Defer[E, B]): Unit =
        deferred.error foreach {
          error =>
            ExceptionHandler.recover(error) foreach {
              reserve =>
                logger.trace(s"Blocking. ${reserve.name}")
                Reserve.blockUntilFree(reserve)
                logger.trace(s"Freed. ${reserve.name}")
            }
        }

      @tailrec
      def doRun(deferred: IO.Defer[E, B], tried: Int): T[B] = {
        if (tried > 0) blockIfNeeded(deferred)
        IO.Defer.runAndRecover(deferred) match {
          case scala.util.Left(io) =>
            logger.trace(s"Run! isCached: ${getValue.isDefined}. ${io.getClass.getSimpleName}")
            io match {
              case success @ IO.Right(_) =>
                tag.fromIO(success)

              case IO.Left(error) =>
                logger.trace(s"Run! isCached: ${getValue.isDefined}. ${io.getClass.getSimpleName}")
                if (recovery.isDefined) //pattern matching is not allowing @tailrec. So .get is required here.
                  doRun(recovery.get.asInstanceOf[E => IO.Defer[E, B]](error), 0)
                else
                  tag.fromIO(io)
            }

          case scala.util.Right(deferred) =>
            logger.trace(s"Retry! isCached: ${getValue.isDefined}. ${deferred.error}")
            if (tried > 0 && tried % IO.Defer.maxRecoveriesBeforeWarn == 0)
              logger.warn(s"${Thread.currentThread().getName}: Competing reserved resource accessed via runSync. Times accessed: $tried. Reserve: ${deferred.error.flatMap(error => IO.ExceptionHandler.recover(error).map(_.name))}")
            doRun(deferred, tried + 1)
        }
      }

      doRun(this, 0)
    }

    private def runAsync[B >: A, T[_]](implicit tag: Tag.Async.Retryable[T]): T[B] = {

      /**
       * If the value is already fetched [[isPending]] run in current thread
       * else return a T that listens for the value to be complete.
       */
      def delayedRun(deferred: IO.Defer[E, B]): Option[T[Unit]] =
        deferred.error flatMap {
          error =>
            ExceptionHandler.recover(error) flatMap {
              reserve =>
                val promise = Reserve.promise(reserve)
                if (promise.isCompleted)
                  None
                else
                  Some(tag.fromPromise(promise))
            }
        }

      def runDelayed(deferred: IO.Defer[E, B],
                     tried: Int,
                     async: T[Unit]): T[B] =
        tag.flatMap(async) {
          _ =>
            runNow(deferred, tried)
        }

      //TO-DO moved Options.scala to data package.
      def when[X](condition: Boolean)(success: => Option[X]): Option[X] =
        if (condition)
          success
        else
          None

      @tailrec
      def runNow(deferred: IO.Defer[E, B], tried: Int): T[B] =
        when(tried > 0)(delayedRun(deferred)) match {
          case Some(async) if tag.isIncomplete(async) =>
            logger.trace(s"Run delayed! isCached: ${getValue.isDefined}.")
            runDelayed(deferred, tried, async)

          case Some(_) | None =>
            logger.trace(s"Run no delay! isCached: ${getValue.isDefined}")
            //no delay required run in stack safe manner.
            IO.Defer.runAndRecover(deferred) match {
              case scala.util.Left(io) =>
                io match {
                  case success @ IO.Right(_) =>
                    tag.fromIO(success)

                  case IO.Left(error) =>
                    if (recovery.isDefined) //pattern matching is not allowing @tailrec. So .get is required here.
                      runNow(recovery.get.asInstanceOf[E => IO.Defer[E, B]](error), 0)
                    else
                      tag.fromIO(io)
                }

              case scala.util.Right(deferred) =>
                if (tried > 0 && tried % IO.Defer.maxRecoveriesBeforeWarn == 0)
                  logger.warn(s"${Thread.currentThread().getName}: Competing reserved resource accessed via Async. Times accessed: $tried. Reserve: ${deferred.error.flatMap(error => IO.ExceptionHandler.recover(error).map(_.name))}")
                runNow(deferred, tried + 1)
            }
        }

      runNow(this, 0)
    }

    def getOrElse[B >: A](default: => B): B =
      getValue getOrElse default

    def map[B](f: A => B): IO.Defer[E, B] =
      IO.Defer[E, B](
        operation = () => f(getUnsafe),
        error = error
      )

    def flatMap[F >: E : IO.ExceptionHandler, B](f: A => IO.Defer[F, B]): IO.Defer[F, B] =
      IO.Defer[F, B](
        operation = () => f(getUnsafe).getUnsafe,
        error = error
      )

    def flatMapIO[F >: E : IO.ExceptionHandler, B](f: A => IO[F, B]): IO.Defer[F, B] =
      IO.Defer[F, B](
        operation = () => f(getUnsafe).get,
        error = error
      )

    def recover[B >: A](f: PartialFunction[E, B]): IO.Defer[E, B] =
      copy(
        recovery =
          Some(
            (error: E) =>
              IO.Defer(f(error))
          )
      )

    def recoverWith[F >: E : IO.ExceptionHandler, B >: A](f: PartialFunction[E, IO.Defer[F, B]]): IO.Defer[F, B] =
      copy(
        recovery =
          Some(
            (error: E) =>
              f(error)
          )
      )

    //flattens using blocking IO.
    def flatten[F, B](implicit ev: A <:< IO.Defer[F, B]): IO.Defer[F, B] =
      runIO.get
  }
}