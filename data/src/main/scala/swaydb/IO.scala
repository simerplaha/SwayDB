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

import swaydb.data.Reserve
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

/**
  * [[IO.Success]] and [[IO.Failure]] are similar to types in [[scala.util.Try]].
  *
  * [[IO.Deferred]] is for performing synchronous and asynchronous IO.
  */
sealed trait IO[+E, +A] {
  def isFailure: Boolean
  def isSuccess: Boolean
  def getOrElse[B >: A](default: => B): B
  def orElse[F >: E : ErrorHandler, B >: A](default: => IO[F, B]): IO[F, B]
  def get: A
  def foreach[B](f: A => B): Unit
  def map[B](f: A => B): IO[E, B]
  def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO[F, B]
  def exists(f: A => Boolean): Boolean
  def filter(p: A => Boolean): IO[E, A]
  @inline final def withFilter(p: A => Boolean): WithFilter = new WithFilter(p)
  class WithFilter(p: A => Boolean) {
    def map[B](f: A => B): IO[E, B] = IO.this filter p map f
    def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO[F, B] = IO.this filter p flatMap f
    def foreach[B](f: A => B): Unit = IO.this filter p foreach f
    def withFilter(q: A => Boolean): WithFilter = new WithFilter(x => p(x) && q(x))
  }
  def onFailureSideEffect(f: IO.Failure[E, A] => Unit): IO[E, A]
  def onSuccessSideEffect(f: A => Unit): IO[E, A]
  def onCompleteSideEffect(f: IO[E, A] => Unit): IO[E, A]
  def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B]
  def recover[B >: A](f: PartialFunction[E, B]): IO[E, B]
  def toOption: Option[A]
  def flatten[F, B](implicit ev: A <:< IO[F, B]): IO[F, B]
  def failed: IO[Nothing, E]
  def toEither: Either[E, A]
  def toFuture: Future[A]
  def toTry: scala.util.Try[A]
  def toDeferred(implicit errorHandler: ErrorHandler[E]): IO.Deferred[E, A] =
    IO.Deferred[E, A](io = this)
}

object IO {

  type TIO[T] = IO[Throwable, T]
  type NIO[T] = IO[Nothing, T]

  sealed trait Done
  final case object Done extends Done

  val unit: IO.Success[Nothing, Unit] = IO.Success()(ErrorHandler.Nothing)
  val none: IO.Success[Nothing, Option[Nothing]] = IO.Success(None)(ErrorHandler.Nothing)
  val `false`: Success[Nothing, Boolean] = IO.Success(false)(ErrorHandler.Nothing)
  val `true`: Success[Nothing, Boolean] = IO.Success(true)(ErrorHandler.Nothing)
  val someTrue: IO[Nothing, Some[Boolean]] = IO.Success(Some(true))(ErrorHandler.Nothing)
  val someFalse: IO[Nothing, Some[Boolean]] = IO.Success(Some(false))(ErrorHandler.Nothing)
  val zero: Success[Nothing, Int] = IO.Success(0)(ErrorHandler.Nothing)
  val emptyBytes: Success[Nothing, Slice[Byte]] = IO.Success(Slice.emptyBytes)(ErrorHandler.Nothing)
  val emptySeqBytes: Success[Nothing, Seq[Slice[Byte]]] = IO.Success(Seq.empty[Slice[Byte]])(ErrorHandler.Nothing)
  val done: Success[Nothing, Done] = IO.Success(Done)(ErrorHandler.Nothing)

  implicit class IterableIOImplicit[E: ErrorHandler, A: ClassTag](iterable: Iterable[A]) {

    def foreachIO[R](f: A => IO[E, R], failFast: Boolean = true): Option[IO.Failure[E, R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[E, R]] = None
      while (it.hasNext && (failure.isEmpty || !failFast)) {
        f(it.next()) onFailureSideEffect {
          case failed @ IO.Failure(_) =>
            failure = Some(failed)
        }
      }
      failure
    }

    //returns the first IO.Success(Some(_)).
    def untilSome[R](f: A => IO[E, Option[R]]): IO[E, Option[(R, A)]] = {
      iterable.iterator foreach {
        item =>
          f(item) match {
            case IO.Success(Some(value)) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Success[E, Option[(R, A)]](Some(value, item))

            case IO.Success(None) =>
            //continue reading

            case IO.Failure(error) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Failure(error)
          }
      }
      IO.none
    }

    def untilSomeResult[R](f: A => IO[E, Option[R]]): IO[E, Option[R]] = {
      iterable.iterator foreach {
        item =>
          f(item) match {
            case IO.Success(Some(value)) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Success[E, Option[R]](Some(value))

            case IO.Success(None) =>
            //continue reading

            case IO.Failure(error) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Failure(error)
          }
      }
      IO.none
    }

    def mapIO[R: ClassTag](block: A => IO[E, R],
                           recover: (Slice[R], IO.Failure[E, Slice[R]]) => Unit = (_: Slice[R], _: IO.Failure[E, Slice[R]]) => (),
                           failFast: Boolean = true): IO[E, Slice[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[E, Slice[R]]] = None
      val results = Slice.create[R](iterable.size)
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        block(it.next()) match {
          case IO.Success(value) =>
            results add value

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure[E, Slice[R]](failed.error))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Success[E, Slice[R]](results)
      }
    }

    def flatMapIO[R: ClassTag](ioBlock: A => IO[E, Iterable[R]],
                               recover: (Iterable[R], IO.Failure[E, Slice[R]]) => Unit = (_: Iterable[R], _: IO.Failure[E, Iterable[R]]) => (),
                               failFast: Boolean = true): IO[E, Iterable[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[E, Slice[R]]] = None
      val results = ListBuffer.empty[R]
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        ioBlock(it.next()) match {
          case IO.Success(value) =>
            value foreach (results += _)

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure[E, Slice[R]](failed.error))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value

        case None =>
          IO.Success[E, Iterable[R]](results)
      }
    }

    def foldLeftIO[R: ClassTag](r: R,
                                failFast: Boolean = true,
                                recover: (R, IO.Failure[E, R]) => Unit = (_: R, _: IO.Failure[E, R]) => ())(f: (R, A) => IO[E, R]): IO[E, R] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[E, R]] = None
      var result: R = r
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        f(result, it.next()) match {
          case IO.Success(value) =>
            if (failure.isEmpty)
              result = value

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure[E, R](failed.error))
        }
      }
      failure match {
        case Some(failure) =>
          recover(result, failure)
          failure

        case None =>
          IO.Success[E, R](result)
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

  @inline final def apply[E: ErrorHandler, A](f: => A): IO[E, A] =
    try IO.Success[E, A](f) catch {
      case ex: Throwable =>
        IO.Failure(error = ErrorHandler.fromException[E](ex))
    }

  object Catch {
    @inline final def apply[E: ErrorHandler, A](f: => IO[E, A]): IO[E, A] =
      try
        f
      catch {
        case ex: Throwable =>
          IO.Failure(error = ErrorHandler.fromException[E](ex))
      }
  }

  def fromTry[E: ErrorHandler, A](tryBlock: Try[A]): IO[E, A] =
    tryBlock match {
      case scala.util.Success(value) =>
        IO.Success[E, A](value)

      case scala.util.Failure(exception) =>
        IO.Failure[E, A](error = ErrorHandler.fromException[E](exception))
    }

  def successful[E: ErrorHandler, A](value: A): IO.Success[E, A] =
    new Success[E, A](value)

  final case class Success[+E: ErrorHandler, +A](value: A) extends IO[E, A] {
    override def isFailure: Boolean = false
    override def isSuccess: Boolean = true
    override def get: A = value
    override def exists(f: A => Boolean): Boolean = f(value)
    override def getOrElse[B >: A](default: => B): B = get
    override def orElse[F >: E : ErrorHandler, B >: A](default: => IO[F, B]): IO.Success[F, B] = this
    override def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO[F, B] = IO.Catch(f(get))
    override def flatten[F, B](implicit ev: A <:< IO[F, B]): IO[F, B] = get
    override def foreach[B](f: A => B): Unit = f(get)
    override def map[B](f: A => B): IO[E, B] = IO[E, B](f(get))
    override def recover[B >: A](f: PartialFunction[E, B]): IO[E, B] = this
    override def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B] = this
    override def failed: IO[Nothing, E] = IO.failed[Nothing, E](new UnsupportedOperationException("IO.Success.failed"))(ErrorHandler.Nothing)
    override def toOption: Option[A] = Some(get)
    override def toEither: Either[E, A] = Right(get)
    override def filter(p: A => Boolean): IO[E, A] =
      IO.Catch(if (p(get)) this else IO.failed[E, A](new NoSuchElementException("Predicate does not hold for " + get)))
    override def toFuture: Future[A] = Future.successful(get)
    override def toTry: scala.util.Try[A] = scala.util.Success(get)
    override def onFailureSideEffect(f: IO.Failure[E, A] => Unit): IO.Success[E, A] = this
    override def onSuccessSideEffect(f: A => Unit): IO.Success[E, A] = {
      try f(get) finally {}
      this
    }
    override def onCompleteSideEffect(f: IO[E, A] => Unit): IO[E, A] = {
      try f(this) finally {}
      this
    }
  }

  @inline final def failed[E: ErrorHandler, A](exception: Throwable): IO.Failure[E, A] =
    new IO.Failure[E, A](ErrorHandler.fromException[E](exception))

  @inline final def failed[E: ErrorHandler, A](message: String): IO.Failure[E, A] =
    new IO.Failure[E, A](ErrorHandler.fromException[E](new scala.Exception(message)))

  final case class Failure[+E: ErrorHandler, +A](error: E) extends IO[E, A] {
    def isCompleted: Boolean = true
    override def isFailure: Boolean = true
    override def isSuccess: Boolean = false
    override def get: A = throw exception
    override def getOrElse[B >: A](default: => B): B = default
    override def orElse[F >: E : ErrorHandler, B >: A](default: => IO[F, B]): IO[F, B] = IO.Catch(default)
    override def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO.Failure[F, B] = this.asInstanceOf[IO.Failure[F, B]]
    override def flatten[F, B](implicit ev: A <:< IO[F, B]): IO.Failure[F, B] = this.asInstanceOf[IO.Failure[F, B]]
    override def foreach[B](f: A => B): Unit = ()
    override def map[B](f: A => B): IO.Failure[E, B] = this.asInstanceOf[IO.Failure[E, B]]
    override def recover[B >: A](f: PartialFunction[E, B]): IO[E, B] =
      IO.Catch(if (f isDefinedAt error) IO.Success[E, B](f(error)) else this)

    override def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B] =
      IO.Catch(if (f isDefinedAt error) f(error) else this)

    override def failed: IO.Success[Nothing, E] = IO.Success[Nothing, E](error)(ErrorHandler.Nothing)
    override def toOption: Option[A] = None
    override def toEither: Either[E, A] = Left(error)
    override def filter(p: A => Boolean): IO.Failure[E, A] = this
    override def toFuture: Future[A] = Future.failed(exception)
    override def toTry: scala.util.Try[A] = scala.util.Failure(exception)
    override def onFailureSideEffect(f: IO.Failure[E, A] => Unit): IO.Failure[E, A] = {
      try f(this) finally {}
      this
    }
    override def onCompleteSideEffect(f: IO[E, A] => Unit): IO[E, A] = onFailureSideEffect(f)
    override def onSuccessSideEffect(f: A => Unit): IO.Failure[E, A] = this
    def exception: Throwable = ErrorHandler.toException(error)
    override def exists(f: A => Boolean): Boolean = false
  }

  /** **********************************
    * **********************************
    * **********************************
    * ************ DEFERRED ************
    * **********************************
    * **********************************
    * **********************************/

  object Deferred {

    val none = IO.Deferred(None, swaydb.Error.ReservedResource(Reserve()))
    val done = IO.Deferred(IO.Done, swaydb.Error.ReservedResource(Reserve()))
    val unit = IO.Deferred((), swaydb.Error.ReservedResource(Reserve()))
    val zero = IO.Deferred(0, swaydb.Error.ReservedResource(Reserve()))

    @inline final def apply[E: ErrorHandler, A](io: IO[E, A]): IO.Deferred[E, A] =
      new IO.Deferred(() => io.get, None)

    @inline final def apply[E: ErrorHandler, A](value: => A): IO.Deferred[E, A] =
      new Deferred(() => value, None)

    @inline private final def apply[E: ErrorHandler, A](value: => A, error: E): IO.Deferred[E, A] =
      new Deferred(() => value, Some(error))

    @inline private final def apply[E: ErrorHandler, A](value: => A, error: Option[E]): IO.Deferred[E, A] =
      new Deferred(() => value, error)

    def fromFuture[E >: swaydb.Error.ReservedIO : ErrorHandler, A](future: Future[A])(implicit ec: ExecutionContext): IO.Deferred[E, A] =
      IO.Deferred[E, A](future)

    final def apply[E >: swaydb.Error.ReservedIO : ErrorHandler, A](future: Future[A])(implicit ec: ExecutionContext): IO.Deferred[E, A] = {
      val reserve = Reserve[Unit](())
      future onComplete {
        _ =>
          Reserve.setFree(reserve)
      }

      /**
        * This value will only be accessed by [[IO.Deferred]] only when the Future completes. But functions like
        * [[IO.Deferred.get]] will try to access this before the Future is complete will result in failure.
        *
        * This is necessary to avoid blocking Futures.
        */
      def deferredValue =
        future.value map {
          case scala.util.Success(value) =>
            value

          case scala.util.Failure(exception) =>
            throw exception
        } getOrElse {
          //Accessing Future when its incomplete.
          throw swaydb.Exception.GetOnIncompleteDeferredFutureIO(reserve)
        }

      IO.Deferred(
        value = deferredValue,
        error = swaydb.Error.ReservedResource(reserve)
      )
    }

    @inline private final def recover[E: ErrorHandler, A](f: => A): Either[IO[E, A], IO.Deferred[E, A]] =
      try Left(IO.Success[E, A](f)) catch {
        case ex: Throwable =>
          recover[E, A](ex, f)
      }

    private def recover[E: ErrorHandler, A](exception: Throwable, f: => A): Either[Failure[E, A], Deferred[E, A]] = {
      val error = ErrorHandler.fromException[E](exception)
      ErrorHandler.reserve(error) map {
        _ =>
          Right(IO.Deferred(f, error))
      } getOrElse Left(IO.Failure(error))
    }
  }

  final case class Deferred[+E: ErrorHandler, +A](private val value: () => A,
                                                  error: Option[E]) {

    @volatile private var _value: Option[Any] = None

    private def getValue = _value.map(_.asInstanceOf[A])

    //a deferred IO is completed if it's not reserved.
    def isCompleted: Boolean =
      error.flatMap(ErrorHandler.reserve[E]) forall (_.isFree)

    def isValueDefined: Boolean =
      getValue.isDefined

    def isValueEmpty: Boolean =
      !isValueDefined

    def isFailure: Boolean = false
    def isSuccess: Boolean = false
    def isDeferred: Boolean = true
    def isBusy =
      error.flatMap(ErrorHandler.reserve[E]) exists (_.isBusy)

    @throws[scala.Exception]
    def get: A = {
      //Runs composed functions does not perform any recovery.
      def forceGet: A =
        getValue getOrElse {
          val got = value()
          _value = Some(got)
          got
        }

      if (_value.isDefined || !isBusy)
        forceGet
      else
        error map {
          error =>
            throw ErrorHandler.toException[E](error)
        } getOrElse forceGet
    }

    /**
      * Opens all [[IO.Deferred]] types to read the final value in a blocking manner.
      */
    def runBlocking: IO[E, A] = {

      @tailrec
      def doRun(later: IO.Deferred[E, A]): IO[E, A] = {
        error.flatMap(ErrorHandler.reserve[E]) match {
          case Some(reserve) =>
            Reserve.blockUntilFree(reserve)
            later.run match {
              case Left(io) =>
                io

              case Right(deferred) =>
                doRun(deferred)
            }

          case None =>
            IO(get)
        }
      }

      doRun(this)
    }

    /**
      * Opens all [[IO.Deferred]] types to read the final value in a non-blocking manner.
      */
    def runInFuture(implicit ec: ExecutionContext): Future[A] = {

      def doRun(deferred: IO.Deferred[E, A]): Future[A] =
        error.flatMap(ErrorHandler.reserve[E]) map {
          reserve =>
            Reserve.future(reserve).map(_ => deferred.run) flatMap {
              case Left(io) =>
                io.toFuture

              case Right(later: IO.Deferred[E, A]) =>
                doRun(later)
            }
        } getOrElse {
          getValue.map(Future.successful) getOrElse Future(get)
        }

      doRun(this)
    }

    def run: Either[IO[E, A], IO.Deferred[E, A]] =
      if (getValue.isDefined || !isBusy)
        IO.Deferred.recover[E, A](get)
      else
        Right(this)

    def getOrElse[B >: A](default: => B): B =
      getValue getOrElse default

    def map[B](f: A => B): IO.Deferred[E, B] =
      IO.Deferred[E, B](
        value = f(get),
        error = error
      )

    def flatMap[F >: E : ErrorHandler, B](f: A => IO.Deferred[F, B]): IO.Deferred[F, B] =
      IO.Deferred[F, B](
        value = f(get).get,
        error = error
      )

    def flatMapIO[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO.Deferred[F, B] =
      IO.Deferred[F, B](
        value = f(get).get,
        error = error
      )

    def flatten[F, B](implicit ev: A <:< IO.Deferred[F, B]): IO.Deferred[F, B] =
      get

    def recover[B >: A](f: PartialFunction[E, B]): IO.Deferred[E, B] =
      IO.Deferred[E, B](
        value = IO[E, B](get).recover(f).get,
        error = error
      )

    def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO.Deferred[F, B]]): IO.Deferred[F, B] =
      IO.Deferred[E, B](
        value =
          IO(get) recover {
            case error =>
              f(error).get
          } get,
        error = error
      )
  }
}