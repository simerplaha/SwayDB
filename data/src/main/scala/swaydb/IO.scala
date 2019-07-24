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

import swaydb.ErrorHandler._
import swaydb.data.Reserve
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

/**
  * [[IO.Success]] and [[IO.Failure]] are similar to types in [[scala.util.Try]].
  *
  * [[IO.Defer]] is for performing synchronous and asynchronous IO.
  */
sealed trait IO[+E, +A] {
  def isFailure: Boolean
  def isSuccess: Boolean
  def isDeferred: Boolean
  def getOrElse[B >: A](default: => B): B
  def orElse[F >: E : ErrorHandler, B >: A](default: => IO[F, B]): IO[F, B]
  def get: A
  def foreach[B](f: A => B): Unit
  def map[B](f: A => B): IO[E, B]
  def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO[F, B]
  def asDeferred: IO.Defer[E, A]
  def asIO: IO[E, A]
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
}

object IO {

  type TIO[T] = IO[Throwable, T]
  type NIO[T] = IO[Nothing, T]

  sealed trait OK
  final case object OK extends OK

  val unit: IO.Success[Nothing, Unit] = IO.Success()(Nothing)
  val none: IO.Success[Nothing, Option[Nothing]] = IO.Success(None)(Nothing)
  val `false`: Success[Nothing, Boolean] = IO.Success(false)(Nothing)
  val `true`: Success[Nothing, Boolean] = IO.Success(true)(Nothing)
  val someTrue: IO[Nothing, Some[Boolean]] = IO.Success(Some(true))(Nothing)
  val someFalse: IO[Nothing, Some[Boolean]] = IO.Success(Some(false))(Nothing)
  val zero: Success[Nothing, Int] = IO.Success(0)(Nothing)
  val emptyBytes: Success[Nothing, Slice[Byte]] = IO.Success(Slice.emptyBytes)(Nothing)
  val emptySeqBytes: Success[Nothing, Seq[Slice[Byte]]] = IO.Success(Seq.empty[Slice[Byte]])(Nothing)
  val ok: Success[Nothing, OK.type] = IO.Success(OK)(Nothing)

  sealed trait Defer[+E, +A] {
    def isFailure: Boolean
    def isSuccess: Boolean
    def isDeferred: Boolean
    def flatMap[F >: E : ErrorHandler, B](f: A => IO.Defer[F, B]): IO.Defer[F, B]
    def mapDeferred[B](f: A => B): IO.Defer[E, B]
    def get: A
    def run: IO.Defer[E, A]
    def runIfFileExists: IO.Defer[E, A]
    def runBlocking: IO[E, A]
    def runBlockingIfFileExists: IO[E, A]
    def runInFuture(implicit ec: ExecutionContext): Future[A]
    def runInFutureIfFileExists(implicit ec: ExecutionContext): Future[A]
    def getOrElse[B >: A](default: => B): B
    def recover[B >: A](f: PartialFunction[E, B]): IO[E, B]
    def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B]
    def failed: IO[Nothing, E]
    def flattenDeferred[F, B](implicit ev: A <:< IO.Defer[F, B]): IO.Defer[F, B]
  }

  implicit class IterableIOImplicit[E: ErrorHandler, A: ClassTag](iterable: Iterable[A]) {

    def foreachIO[R](f: A => IO[E, R], failFast: Boolean = true): Option[IO.Failure[E, R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[E, R]] = None
      while (it.hasNext && (failure.isEmpty || !failFast)) {
        f(it.next()) match {
          case failed @ IO.Failure(_) =>
            failure = Some(failed)
          case _: IO.Success[_, _] =>
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

  object CatchLeak {
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

  def fromFuture[E: ErrorHandler, A](future: Future[A])(implicit ec: ExecutionContext): IO.Defer[E, A] =
    IO.Defer[E, A](future)

  def successful[E: ErrorHandler, A](value: A): IO.Success[E, A] =
    new Success[E, A](value)

  final case class Success[+E: ErrorHandler, +A](value: A) extends IO.Defer[E, A] with IO[E, A] {
    override def isFailure: Boolean = false
    override def isSuccess: Boolean = true
    override def isDeferred: Boolean = false
    override def get: A = value
    override def exists(f: A => Boolean): Boolean = f(value)
    override def run: IO.Success[E, A] = this
    override def runIfFileExists: IO.Success[E, A] = this
    override def runBlocking: IO.Success[E, A] = this
    override def runBlockingIfFileExists: IO[E, A] = this
    override def runInFutureIfFileExists(implicit ec: ExecutionContext): Future[A] = Future.successful(value)
    override def runInFuture(implicit ec: ExecutionContext): Future[A] = Future.successful(value)
    override def getOrElse[B >: A](default: => B): B = get
    override def orElse[F >: E : ErrorHandler, B >: A](default: => IO[F, B]): IO.Success[F, B] = this
    override def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO[F, B] = f(get)
    override def flatMap[F >: E : ErrorHandler, B](f: A => IO.Defer[F, B]): IO.Defer[F, B] = f(get)
    override def flatten[F, B](implicit ev: A <:< IO[F, B]): IO[F, B] = get
    override def flattenDeferred[F, B](implicit ev: A <:< IO.Defer[F, B]): IO.Defer[F, B] = get
    override def foreach[B](f: A => B): Unit = f(get)
    override def map[B](f: A => B): IO[E, B] = IO[E, B](f(get))
    override def mapDeferred[B](f: A => B): IO.Defer[E, B] = IO[E, B](f(get)).asInstanceOf[IO.Defer[E, B]]
    override def recover[B >: A](f: PartialFunction[E, B]): IO[E, B] = this
    override def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B] = this
    override def failed: IO[Nothing, E] = IO.failed[Nothing, E](new UnsupportedOperationException("IO.Success.failed"))(Nothing)
    override def toOption: Option[A] = Some(get)
    override def toEither: Either[E, A] = Right(get)
    override def filter(p: A => Boolean): IO[E, A] =
      IO.CatchLeak(if (p(get)) this else IO.failed[E, A](new NoSuchElementException("Predicate does not hold for " + get)))
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
    override def asDeferred: IO.Defer[E, A] = this
    override def asIO: IO[E, A] = this
  }

  object Defer {

    @inline final def recover[E: ErrorHandler, A](f: => A): IO.Defer[E, A] =
      try IO.Success[E, A](f) catch {
        case ex: Throwable =>
          recover[E, A](ex, f)
      }

    def recover[E: ErrorHandler, A](failure: IO.Failure[E, A], operation: => A): IO.Defer[E, A] =
      ErrorHandler.shouldRecover(failure.error) map {
        _ =>
          IO.Deferred(operation, failure.error)
      } getOrElse failure

    def recover[E: ErrorHandler, A](exception: Throwable, operation: => A): IO.Defer[E, A] = {
      val error = ErrorHandler.fromException[E](exception)
      ErrorHandler.shouldRecover(error) map {
        _ =>
          IO.Deferred(operation, error)
      } getOrElse IO.Failure(error)
    }

    @inline final def apply[E: ErrorHandler, A](value: => A, error: E): IO.Defer[E, A] =
      new Deferred(_ => value, error)

    final def apply[E: ErrorHandler, A](future: Future[A])(implicit ec: ExecutionContext): IO.Defer[E, A] = {
      val error =
        new ErrorHandler[E] {
          val reserve = Reserve(())
          override def reserve(e: E): Option[Reserve[Unit]] = Some(reserve)
          override def toException(e: E): Throwable = ErrorHandler.toException[E](e)
          override def fromException[F <: E](e: Throwable): F = ErrorHandler.fromException[F](e)
        }

      future onComplete {
        _ =>
          Reserve.setFree(error.reserve)
      }

      //here value will only be access when the above busy boolean is true
      //so the value should always exists at the time of Await.result
      //therefore the cost of blocking should be negligible.
      IO.Defer(
        value = Await.result(future, Duration.Zero),
        error = error.asInstanceOf[E]
      )
    }
  }

  object Deferred {
    @inline final def apply[E: ErrorHandler, A](value: => A, error: E): Deferred[E, A] =
      new Deferred(_ => value, error)
  }

  final case class Deferred[+E: ErrorHandler, A](value: Unit => A,
                                                 error: E) extends IO.Defer[E, A] {

    @volatile private var _value: Option[A] = None

    def isValueDefined: Boolean =
      _value.isDefined

    def isValueEmpty: Boolean =
      !isValueDefined

    def isFailure: Boolean = false
    def isSuccess: Boolean = false
    def isDeferred: Boolean = true
    def isBusy = ErrorHandler.shouldRecover(error).exists(_.isBusy)

    /**
      * Runs composed functions does not perform any recovery.
      */
    private def forceGet: A =
      _value getOrElse {
        val got = value()
        _value = Some(got)
        got
      }

    override def runBlockingIfFileExists: IO[E, A] = {
      @tailrec
      def doGet(later: IO.Deferred[E, A]): IO[E, A] = {
        ErrorHandler.shouldRecover(later.error) match {
          case Some(reserve) =>
            Reserve.blockUntilFree(reserve)
            later.runIfFileExists match {
              case success @ IO.Success(_) =>
                success

              case deferred: IO.Deferred[E, A] =>
                doGet(deferred)

              case failure @ IO.Failure(_) =>
                failure
            }

          case None =>
            IO.Failure(error)
        }
      }

      doGet(this)
    }

    /**
      * Opens all [[IO.Defer]] types to read the final value in a blocking manner.
      */
    def runBlocking: IO[E, A] = {

      @tailrec
      def doGet(later: IO.Deferred[E, A]): IO[E, A] = {
        ErrorHandler.shouldRecover(later.error) match {
          case Some(reserve) =>
            Reserve.blockUntilFree(reserve)
            later.run match {
              case success @ IO.Success(_) =>
                success

              case deferred: IO.Deferred[E, A] =>
                doGet(deferred)

              case failure @ IO.Failure(_) =>
                failure
            }

          case None =>
            IO.Failure(error)
        }
      }

      doGet(this)
    }

    /**
      * Opens all [[IO.Defer]] types to read the final value in a non-blocking manner.
      */
    def runInFuture(implicit ec: ExecutionContext): Future[A] = {

      def doGet(later: IO.Deferred[E, A]): Future[A] =
        ErrorHandler.shouldRecover(later.error) map {
          reserve =>
            Reserve.future(reserve).map(_ => later.run) flatMap {
              case IO.Success(value) =>
                Future.successful(value)

              case later: IO.Deferred[E, A] =>
                doGet(later)

              case IO.Failure(error) =>
                Future.failed(ErrorHandler.toException(error))
            }
        } getOrElse Future.failed(ErrorHandler.toException(error))

      doGet(this)
    }

    /**
      * Opens all [[IO.Defer]] types to read the final value in a non-blocking manner.
      */
    def runInFutureIfFileExists(implicit ec: ExecutionContext): Future[A] = {

      def doGet(later: IO.Deferred[E, A]): Future[A] =
        ErrorHandler.shouldRecover(later.error) map {
          reserve =>
            Reserve.future(reserve).map(_ => later.runIfFileExists) flatMap {
              case IO.Success(value) =>
                Future.successful(value)

              case deferred: IO.Deferred[E, A] =>
                doGet(deferred)

              case IO.Failure(error) =>
                Future.failed(ErrorHandler.toException(error))
            }
        } getOrElse Future.failed(ErrorHandler.toException(error))

      doGet(this)
    }

    @throws[Exception]
    def get: A =
      if (_value.isDefined || !isBusy)
        forceGet
      else
        throw ErrorHandler.toException(error)

    def run: IO.Defer[E, A] =
      if (_value.isDefined || !isBusy)
        IO.Defer.recover[E, A](get)
      else
        this

    def runIfFileExists: IO.Defer[E, A] =
      if (_value.isDefined || !isBusy)
        IO.Defer.recover[E, A](get)
      else
        this

    def getOrElse[B >: A](default: => B): B =
      IO[E, B](forceGet).getOrElse(default)

    def flatMap[F: ErrorHandler, B](f: A => IO.Defer[F, B]): IO.Deferred[F, B] =
      new IO.Deferred[F, B](
        value = _ => f(get).get,
        error = error.asInstanceOf[F]
      )

    def flattenDeferred[F, B](implicit ev: A <:< IO.Defer[F, B]): IO.Defer[F, B] = forceGet
    def map[B](f: A => B): Deferred[E, B] = IO.Deferred[E, B]((_: Unit) => f(forceGet), error)
    def mapDeferred[B](f: A => B): IO.Defer[E, B] = map(f)
    def recover[B >: A](f: PartialFunction[E, B]): IO[E, B] = IO[E, B](forceGet).recover(f)
    def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B] = IO[E, B](forceGet).recoverWith(f)
    def failed: IO[Nothing, E] = IO[E, A](forceGet).failed
  }

  @inline final def failed[E: ErrorHandler, A](exception: Throwable): IO.Failure[E, A] =
    new IO.Failure[E, A](ErrorHandler.fromException[E](exception))

  @inline final def failed[E: ErrorHandler, A](message: String): IO.Failure[E, A] =
    new IO.Failure[E, A](ErrorHandler.fromException[E](new Exception(message)))

  final case class Failure[+E: ErrorHandler, +A](error: E) extends IO.Defer[E, A] with IO[E, A] {
    override def isFailure: Boolean = true
    override def isSuccess: Boolean = false
    override def isDeferred: Boolean = false
    override def get: A = throw exception
    override def run: IO.Failure[E, A] = this
    override def runIfFileExists: IO.Failure[E, A] = this
    override def runBlocking: IO.Failure[E, A] = this
    override def runBlockingIfFileExists: IO[E, A] = this
    override def runInFutureIfFileExists(implicit ec: ExecutionContext): Future[A] = Future.failed(exception)
    override def runInFuture(implicit ec: ExecutionContext): Future[A] = Future.failed(exception)
    override def getOrElse[B >: A](default: => B): B = default
    override def orElse[F >: E : ErrorHandler, B >: A](default: => IO[F, B]): IO[F, B] = IO.CatchLeak(default)
    override def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO.Failure[F, B] = this.asInstanceOf[IO.Failure[F, B]]
    override def flatMap[F >: E : ErrorHandler, B](f: A => IO.Defer[F, B]): IO.Defer[F, B] = this.asInstanceOf[IO.Defer[F, B]]
    override def flatten[F, B](implicit ev: A <:< IO[F, B]): IO.Failure[F, B] = this.asInstanceOf[IO.Failure[F, B]]
    override def flattenDeferred[F, B](implicit ev: A <:< IO.Defer[F, B]): IO.Defer[F, B] = this.asInstanceOf[IO.Defer[F, B]]
    override def foreach[B](f: A => B): Unit = ()
    override def map[B](f: A => B): IO.Failure[E, B] = this.asInstanceOf[IO.Failure[E, B]]
    override def mapDeferred[B](f: A => B): IO.Defer[E, B] = this.asInstanceOf[IO.Defer[E, B]]
    override def recover[B >: A](f: PartialFunction[E, B]): IO[E, B] =
      IO.CatchLeak(if (f isDefinedAt error) IO.Success[E, B](f(error)) else this)

    override def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B] =
      IO.CatchLeak(if (f isDefinedAt error) f(error) else this)

    override def failed: IO.Success[Nothing, E] = IO.Success[Nothing, E](error)(Nothing)
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
    def recoverToDeferred[F >: E : ErrorHandler, B](operation: => IO.Defer[F, B]): IO.Defer[F, B] =
      IO.Defer.recover[F, Unit](()).flatMap[F, B] {
        _ =>
          operation
      }

    override def asDeferred: IO.Defer[E, A] = this
    override def asIO: IO[E, A] = this
    override def exists(f: A => Boolean): Boolean = false
  }
}