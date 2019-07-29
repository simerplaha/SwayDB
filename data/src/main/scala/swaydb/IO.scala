/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any toIO version.
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

import com.typesafe.scalalogging.LazyLogging
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
  def failed: IO[Throwable, E]
  def toEither: Either[E, A]
  def toFuture: Future[A]
  def toTry: scala.util.Try[A]
  def toDeferred[F >: E : ErrorHandler]: IO.Deferred[F, A] =
    IO.Deferred.io[F, A](io = this)
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
    override def foreach[B](f: A => B): Unit = f(get)
    override def map[B](f: A => B): IO[E, B] = IO[E, B](f(get))
    override def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO[F, B] = IO.Catch(f(get))
    override def flatten[F, B](implicit ev: A <:< IO[F, B]): IO[F, B] = get
    override def recover[B >: A](f: PartialFunction[E, B]): IO[E, B] = this
    override def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B] = this
    override def failed: IO[Throwable, E] = IO.failed[Throwable, E](new UnsupportedOperationException("IO.Success.failed"))(ErrorHandler.Throwable)
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
    override def isFailure: Boolean = true
    override def isSuccess: Boolean = false
    override def get: A = throw exception
    override def exists(f: A => Boolean): Boolean = false
    override def getOrElse[B >: A](default: => B): B = default
    override def orElse[F >: E : ErrorHandler, B >: A](default: => IO[F, B]): IO[F, B] = IO.Catch(default)
    override def foreach[B](f: A => B): Unit = ()
    override def map[B](f: A => B): IO.Failure[E, B] = this.asInstanceOf[IO.Failure[E, B]]
    override def flatMap[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO.Failure[F, B] = this.asInstanceOf[IO.Failure[F, B]]
    override def flatten[F, B](implicit ev: A <:< IO[F, B]): IO.Failure[F, B] = this.asInstanceOf[IO.Failure[F, B]]
    override def recover[B >: A](f: PartialFunction[E, B]): IO[E, B] =
      IO.Catch(if (f isDefinedAt error) IO.Success[E, B](f(error)) else this)

    override def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO[F, B]]): IO[F, B] =
      IO.Catch(if (f isDefinedAt error) f(error) else this)

    override def failed: IO.Success[Throwable, E] = IO.Success[Throwable, E](error)(ErrorHandler.Throwable)
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
  }

  def fromFuture[E: ErrorHandler, A](future: Future[A])(implicit ec: ExecutionContext): IO.Deferred[E, A] = {
    val reserve = Reserve[Unit]((), "fromFuture")
    future onComplete {
      _ =>
        Reserve.setFree(reserve)
    }

    /**
      * [[deferredValue]] will only be invoked the reserve is set free which occurs only when the future is complete.
      * But functions like [[IO.Deferred.getUnsafe]] will try to access this before the Future is complete will result in failure.
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
      IO.Deferred[swaydb.Error.Recoverable, A](
        value = deferredValue,
        error = error
      )

    //Deferred that returns the result of the above deferred when completed.
    IO.Deferred[E, A](recoverableDeferred.getUnsafe)
  }

  /** **********************************
    * **********************************
    * **********************************
    * ************ DEFERRED ************
    * **********************************
    * **********************************
    * **********************************/

  object Deferred extends LazyLogging {

    val maxRecoveriesBeforeWarn = 1000

    val none = IO.Deferred(None, swaydb.Error.ReservedResource(Reserve(name = "Deferred.none")))
    val done = IO.Deferred(IO.Done, swaydb.Error.ReservedResource(Reserve(name = "Deferred.done")))
    val unit = IO.Deferred((), swaydb.Error.ReservedResource(Reserve(name = "Deferred.unit")))
    val zero = IO.Deferred(0, swaydb.Error.ReservedResource(Reserve(name = "Deferred.zero")))

    @inline final def apply[E: ErrorHandler, A](value: => A): IO.Deferred[E, A] =
      new Deferred(() => value, None)

    @inline final def apply[E: ErrorHandler, A](value: => A, error: E): IO.Deferred[E, A] =
      new Deferred(() => value, Some(error))

    @inline final def io[E: ErrorHandler, A](io: => IO[E, A]): IO.Deferred[E, A] =
      new IO.Deferred(() => io.get, None)

    @inline private final def runAndRecover[E: ErrorHandler, A](f: IO.Deferred[E, A]): Either[IO[E, A], IO.Deferred[E, A]] =
      try
        Left(IO.Success[E, A](f.getUnsafe))
      catch {
        case ex: Throwable =>
          val error = ErrorHandler.fromException[E](ex)
          if (ErrorHandler.reserve(error).isDefined)
            Right(IO.Deferred(f.getUnsafe, error))
          else
            Left(IO.Failure(error))
      }
  }

  final case class Deferred[+E: ErrorHandler, +A] private(operation: () => A,
                                                          error: Option[E],
                                                          private val recovery: Option[_ => IO.Deferred[E, A]] = None) extends LazyLogging {

    @volatile private var _value: Option[Any] = None
    private def getValue = _value.map(_.asInstanceOf[A])

    //a deferred IO is completed if it's not reserved.
    def isReady: Boolean =
      error.flatMap(ErrorHandler.reserve[E]) forall (_.isFree)

    def isComplete: Boolean =
      getValue.isDefined

    def isPending: Boolean =
      !isComplete

    def isBusy =
      error.flatMap(ErrorHandler.reserve[E]) exists (_.isBusy)

    @throws[scala.Exception]
    private[IO] def getUnsafe: A = {
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
            throw ErrorHandler.toException[E](error)
        } getOrElse forceGet
    }

    /**
      * Opens all [[IO.Deferred]] types to read the final value in a blocking manner.
      */
    def runIO: IO[E, A] = {

      def blockIfNeeded(deferred: IO.Deferred[E, A]): Unit =
        deferred.error foreach {
          error =>
            ErrorHandler.reserve(error) foreach {
              reserve =>
                logger.debug(s"Blocking. ${reserve.name}")
                Reserve.blockUntilFree(reserve)
                logger.debug(s"Freed. ${reserve.name}")
            }
        }

      @tailrec
      def doRun(deferred: IO.Deferred[E, A], tried: Int): IO[E, A] = {
        blockIfNeeded(deferred)
        IO.Deferred.runAndRecover(deferred) match {
          case Left(io) =>
            logger.debug(s"Run! isCached: ${getValue.isDefined}. ${io.getClass.getSimpleName}")
            (recovery: @unchecked) match {
              case Some(recovery: ((E) => IO.Deferred[E, A])) =>
                io match {
                  case success @ IO.Success(_) =>
                    success

                  case IO.Failure(error) =>
                    logger.debug(s"Run! isCached: ${getValue.isDefined}. ${io.getClass.getSimpleName}")
                    doRun(recovery(error), 0)
                }

              case None =>
                io
            }

          case Right(deferred) =>
            logger.debug(s"Retry! isCached: ${getValue.isDefined}. ${deferred.error}")
            if (tried > 0 && tried % IO.Deferred.maxRecoveriesBeforeWarn == 0)
              logger.warn(s"${Thread.currentThread().getName}: Competing reserved resource accessed via IO. Times accessed: $tried. Reserve: ${deferred.error.flatMap(error => ErrorHandler.reserve(error).map(_.name))}")
            doRun(deferred, tried + 1)
        }
      }

      doRun(this, 0)
    }

    /**
      * Run the deferred IO without blocking.
      */
    def runFuture(implicit ec: ExecutionContext): Future[A] = {

      /**
        * If the value is already fetched [[isPending]] run in current thread
        * else return a Future that listens for the value to be complete.
        */
      def delayedRun(deferred: IO.Deferred[E, A]): Option[Future[Unit]] =
        deferred.error flatMap {
          error =>
            ErrorHandler.reserve(error) flatMap {
              reserve =>
                Reserve.futureOption(reserve)
            }
        }

      def runDelayed(deferred: IO.Deferred[E, A],
                     tried: Int,
                     future: Future[Unit]): Future[A] =
        future flatMap {
          _ =>
            runNow(deferred, tried)
        }

      @tailrec
      def runNow(deferred: IO.Deferred[E, A], tried: Int): Future[A] =
        delayedRun(deferred) match {
          case Some(delayedFuture) if !delayedFuture.isCompleted =>
            logger.debug(s"Run delayed! isCached: ${getValue.isDefined}.")
            runDelayed(deferred, tried, delayedFuture)

          case Some(_) | None =>
            logger.debug(s"Run no delay! isCached: ${getValue.isDefined}")
            //no delay required run in stack safe manner.
            IO.Deferred.runAndRecover(deferred) match {
              case Left(io) =>
                (recovery: @unchecked) match {
                  case Some(recovery: ((E) => IO.Deferred[E, A])) =>
                    io match {
                      case success @ IO.Success(_) =>
                        success.toFuture

                      case IO.Failure(error) =>
                        runNow(recovery(error), 0)
                    }

                  case None =>
                    io.toFuture
                }

              case Right(deferred) =>
                if (tried > 0 && tried % IO.Deferred.maxRecoveriesBeforeWarn == 0)
                  logger.warn(s"${Thread.currentThread().getName}: Competing reserved resource accessed via IO. Times accessed: $tried. Reserve: ${deferred.error.flatMap(error => ErrorHandler.reserve(error).map(_.name))}")
                runNow(deferred, tried + 1)
            }
        }

      runNow(this, 0)
    }

    def getOrElse[B >: A](default: => B): B =
      getValue getOrElse default

    def map[B](f: A => B): IO.Deferred[E, B] =
      IO.Deferred[E, B](
        operation = () => f(getUnsafe),
        error = error
      )

    def flatMap[F >: E : ErrorHandler, B](f: A => IO.Deferred[F, B]): IO.Deferred[F, B] =
      IO.Deferred[F, B](
        operation = () => f(getUnsafe).getUnsafe,
        error = error
      )

    def flatMapIO[F >: E : ErrorHandler, B](f: A => IO[F, B]): IO.Deferred[F, B] =
      IO.Deferred[F, B](
        operation = () => f(getUnsafe).get,
        error = error
      )

    def recover[B >: A](f: PartialFunction[E, B]): IO.Deferred[E, B] =
      copy(
        recovery =
          Some(
            (error: E) =>
              IO.Deferred(f(error))
          )
      )

    def recoverWith[F >: E : ErrorHandler, B >: A](f: PartialFunction[E, IO.Deferred[F, B]]): IO.Deferred[F, B] =
      copy(
        recovery =
          Some(
            (error: E) =>
              f(error)
          )
      )

    //flattens using blocking IO.
    def flatten[F, B](implicit ev: A <:< IO.Deferred[F, B]): IO.Deferred[F, B] =
      runIO.get
  }
}