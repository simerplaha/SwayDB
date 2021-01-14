/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.{Consumer, Predicate, Supplier}

import swaydb.data.util.Java._

import scala.compat.java8.FutureConverters._

object IO {

  trait ExceptionHandler[E] {
    def toException(error: E): Throwable
    def toError(exception: Throwable): E
  }

  val throwableExceptionHandler: ExceptionHandler[Throwable] =
    fromScala(swaydb.IO.ExceptionHandler.Throwable)

  val integerNeverExceptionHandler: ExceptionHandler[Integer] =
    fromScala(swaydb.IO.ExceptionHandler.neverException)

  val stringNeverExceptionHandler: ExceptionHandler[java.lang.String] =
    fromScala(swaydb.IO.ExceptionHandler.neverException)

  val longNeverExceptionHandler: ExceptionHandler[java.lang.Long] =
    fromScala(swaydb.IO.ExceptionHandler.neverException)

  val doubleNeverExceptionHandler: ExceptionHandler[java.lang.Double] =
    fromScala(swaydb.IO.ExceptionHandler.neverException)

  val booleanNeverExceptionHandler: ExceptionHandler[java.lang.Boolean] =
    fromScala(swaydb.IO.ExceptionHandler.neverException)

  val characterNeverExceptionHandler: ExceptionHandler[java.lang.Character] =
    fromScala(swaydb.IO.ExceptionHandler.neverException)

  val byteNeverExceptionHandler: ExceptionHandler[java.lang.Byte] =
    fromScala(swaydb.IO.ExceptionHandler.neverException)

  def neverExceptionHandler[T](): ExceptionHandler[T] =
    fromScala[T](swaydb.IO.ExceptionHandler.neverException[T])

  def fromScala[T](exceptionHandler: swaydb.IO.ExceptionHandler[T]): swaydb.java.IO.ExceptionHandler[T] =
    new ExceptionHandler[T] {
      override def toException(error: T): Throwable =
        exceptionHandler.toException(error)

      override def toError(exception: Throwable): T =
        exceptionHandler.toError(exception)
    }

  def toScala[T](self: ExceptionHandler[T]): swaydb.IO.ExceptionHandler[T] =
    new swaydb.IO.ExceptionHandler[T] {
      override def toException(error: T): Throwable =
        self.toException(error)

      override def toError(exception: Throwable): T =
        self.toError(exception)
    }

  def fromScala[L, R](io: swaydb.IO[L, R], exceptionHandler: IO.ExceptionHandler[L]) =
    new IO(io)(exceptionHandler)

  def fromScala[R](io: swaydb.IO[Throwable, R]) =
    new IO(io)(throwableExceptionHandler)

  def run[O](supplier: Supplier[O]): IO[Throwable, O] =
    new IO(swaydb.IO[Throwable, O](supplier.get())(toScala(throwableExceptionHandler)))(throwableExceptionHandler)

  def run[L, O](supplier: Supplier[O], exceptionHandler: ExceptionHandler[L]): IO[L, O] =
    new IO(swaydb.IO[L, O](supplier.get())(toScala(exceptionHandler)))(exceptionHandler)

  def right[R](right: R): IO[Throwable, R] =
    new IO(swaydb.IO.Right(right))(throwableExceptionHandler)

  def right[L, R](right: R, exceptionHandler: IO.ExceptionHandler[L]): IO[L, R] =
    new IO(swaydb.IO.Right(right)(toScala(exceptionHandler)))(exceptionHandler)

  def rightNeverException[L, R](right: R): IO[L, R] = {
    implicit val never: ExceptionHandler[L] = neverExceptionHandler()
    new IO(swaydb.IO.Right(right)(toScala(never)))
  }

  def left[R](left: Throwable): IO[Throwable, R] =
    new IO(swaydb.IO.Left(left)(toScala(throwableExceptionHandler)))(throwableExceptionHandler)

  def leftNeverException[L, R](left: L): IO[L, R] = {
    implicit val never = swaydb.IO.ExceptionHandler.neverException[L]
    new IO(swaydb.IO.Left(left))(fromScala(never))
  }

  def left[L, R](left: L, exceptionHandler: IO.ExceptionHandler[L]): IO[L, R] =
    new IO(swaydb.IO.Left(left)(toScala(exceptionHandler)))(exceptionHandler)

  def defer[R](supplier: Supplier[R]): Defer[Throwable, R] =
    Defer(swaydb.IO.Defer[Throwable, R](supplier.get())(toScala(throwableExceptionHandler)))(throwableExceptionHandler)

  def defer[L, R](supplier: Supplier[R], exceptionHandler: IO.ExceptionHandler[L]): Defer[L, R] =
    Defer(swaydb.IO.Defer[L, R](supplier.get())(toScala(exceptionHandler)))(exceptionHandler)

  case class Defer[E, R](asScala: swaydb.IO.Defer[E, R])(implicit val exceptionHandler: IO.ExceptionHandler[E]) {

    private implicit val scalaExceptionHandler = IO.toScala(exceptionHandler)

    //a deferred IO is completed if it's not reserved.
    def isReady: java.lang.Boolean =
      asScala.isReady

    def isBusy: java.lang.Boolean =
      asScala.isBusy

    def isComplete: java.lang.Boolean =
      asScala.isComplete

    def isPending: java.lang.Boolean =
      asScala.isPending

    def isSuccess: java.lang.Boolean =
      asScala.isSuccess

    def isFailure: java.lang.Boolean =
      asScala.isFailure

    @throws[Throwable]
    def tryRun: R =
      run

    def run: R =
      asScala.runIO.get

    def orElseGet[B >: R](supplier: Supplier[B]): B =
      asScala.getOrElse(supplier.get())

    def or[B >: R](supplier: Supplier[Defer[E, B]]): Defer[E, B] =
      Defer(asScala.orElse(supplier.get().asScala))

    def orIO[B >: R](supplier: Supplier[IO[E, B]]): Defer[E, B] =
      Defer[E, B](asScala.orElseIO(supplier.get().asScala))

    def forEach(consumer: Consumer[R]): Unit =
      asScala.toIO.foreach(consumer.accept)

    def map[B](function: JavaFunction[R, B]): Defer[E, B] =
      Defer(asScala.map(result => function.apply(result)))

    def flatMap[B](function: JavaFunction[R, Defer[E, B]]): Defer[E, B] =
      Defer(asScala.flatMap[E, B](result => function.apply(result).asScala))

    def flatMapIO[B](function: JavaFunction[R, IO[E, B]]): Defer[E, B] =
      Defer(asScala.flatMapIO[E, B](result => function.apply(result).asScala))

    def exists(predicate: Predicate[R]): Defer[E, Boolean] =
      Defer(asScala.exists(predicate.test))

    def recoverWith[B >: R](function: JavaFunction[E, Defer[E, B]]): Defer[E, B] =
      Defer(
        asScala.recoverWith {
          case result =>
            function.apply(result).asScala
        }
      )

    def recover[B >: R](function: JavaFunction[E, B]): Defer[E, B] =
      Defer(
        asScala.recover {
          case result =>
            function(result)
        }
      )

    def toOption: Optional[R] =
      asScala.toIO.toOption.asJava

    def toFuture: CompletionStage[R] =
      asScala.toIO.toFuture.toJava
  }
}

class IO[L, R](val asScala: swaydb.IO[L, R])(implicit val exceptionHandler: IO.ExceptionHandler[L]) {

  private implicit val scalaExceptionHandler = IO.toScala(exceptionHandler)

  def isLeft: Boolean =
    asScala.isLeft

  def isRight: Boolean =
    asScala.isRight

  def leftIO: IO[Throwable, L] =
    new IO(asScala.left)(IO.throwableExceptionHandler)

  def rightIO: IO[Throwable, R] =
    new IO(asScala.right)(IO.throwableExceptionHandler)

  @throws[Throwable]
  def tryGetLeft: L =
    asScala.left.get

  @throws[UnsupportedOperationException]
  def tryGetRight: R =
    asScala.right.get

  @throws[Throwable]
  def tryGet: R =
    asScala.get

  def getLeft =
    asScala.left.get

  def getRight: R =
    get

  def get: R =
    asScala.get

  def map[B](function: JavaFunction[R, B]): IO[L, B] =
    new IO(asScala.map(result => function.apply(result)))

  def flatMap[B](function: JavaFunction[R, IO[L, B]]): IO[L, B] =
    new IO(asScala.flatMap[L, B](result => function.apply(result).asScala))

  /**
   * Difference between [[map]] and [[transform]] is that [[transform]] does not
   * recovery from exception if the function F throws an Exception.
   */
  def transform[B](function: JavaFunction[R, B]): IO[L, B] =
    new IO(asScala.transform(result => function.apply(result)))

  def andThen[B](supplier: Supplier[B]): IO[L, B] =
    new IO(asScala.andThen(supplier.get()))

  def and[B](supplier: Supplier[IO[L, B]]): IO[L, B] =
    new IO(asScala.and[L, B](supplier.get().asScala))

  def orElseGet[B >: R](supplier: Supplier[B]): B =
    asScala.getOrElse(supplier.get())

  def or[B >: R](supplier: Supplier[IO[L, B]]): IO[L, B] =
    new IO[L, B](asScala.orElse(supplier.get().asScala))

  def forEach(consumer: Consumer[R]): Unit =
    asScala.foreach(consumer.accept)

  def exists(predicate: Predicate[R]): Boolean =
    asScala.exists(predicate.test)

  def filter(predicate: Predicate[R]): IO[L, R] =
    new IO(asScala.filter(predicate.test))

  def recoverWith[B >: R](function: JavaFunction[L, IO[L, B]]): IO[L, B] =
    new IO(
      asScala.recoverWith {
        case result =>
          function.apply(result).asScala
      }
    )

  def recover[B >: R](function: JavaFunction[L, B]): IO[L, B] =
    new IO(
      asScala.recover {
        case result =>
          function(result)
      }
    )

  def onLeftSideEffect(consumer: Consumer[L]): IO[L, R] =
    new IO(
      asScala.onLeftSideEffect {
        left =>
          consumer.accept(left.value)
      }
    )

  def onRightSideEffect(consumer: Consumer[R]): IO[L, R] =
    new IO(
      asScala.onRightSideEffect(consumer.accept)
    )

  def onCompleteSideEffect(consumer: Consumer[IO[L, R]]): IO[L, R] =
    new IO(
      asScala.onCompleteSideEffect {
        io =>
          consumer.accept(new IO(io)) //fixme - is using `this` be enough?
      }
    )

  def toOptional: Optional[R] =
    asScala.toOption.asJava

  def toFuture: CompletionStage[R] =
    asScala.toFuture.toJava

  override def equals(obj: Any): Boolean =
    obj match {
      case io: IO[_, _] =>
        asScala.equals(io.asScala)

      case _ => false
    }

  override def hashCode(): Int =
    asScala.hashCode()
}
