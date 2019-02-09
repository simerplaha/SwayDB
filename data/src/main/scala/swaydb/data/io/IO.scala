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

import java.io.FileNotFoundException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.{NoSuchFileException, Path}
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import swaydb.data.slice.{Slice, SliceReader}

/**
  * Similar to [[scala.util.Try]] but adds another type [[IO.Async]].
  */
sealed trait IO[+T] {
  def isFailure: Boolean
  def isSuccess: Boolean
  def isLater: Boolean
  def getOrElse[U >: T](default: => U): U
  def orElse[U >: T](default: => IO[U]): IO[U]
  def unsafeGet: T
  def foreach[U](f: T => U): Unit
  def flatMap[U](f: T => IO[U]): IO[U]
  def map[U](f: T => U): IO[U]
  def filter(p: T => Boolean): IO[T]
  @inline final def withFilter(p: T => Boolean): WithFilter = new WithFilter(p)
  class WithFilter(p: T => Boolean) {
    def map[U](f: T => U): IO[U] = IO.this filter p map f
    def flatMap[U](f: T => IO[U]): IO[U] = IO.this filter p flatMap f
    def foreach[U](f: T => U): Unit = IO.this filter p foreach f
    def withFilter(q: T => Boolean): WithFilter = new WithFilter(x => p(x) && q(x))
  }
  def onFailure[U >: T](f: IO.Failure[U] => Unit): IO[U]
  def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U]
  def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U]
  def toOption: Option[T]
  def flatten[U](implicit ev: T <:< IO[U]): IO[U]
  def failed: IO[Throwable]
  def toEither: Either[Throwable, T]
  def toFuture: Future[T]
  def toTry: scala.util.Try[T]
}

object IO {
  case class BusyException(error: Error.Busy) extends Exception("Is busy")

  val unit: IO.Success[Unit] = IO.Success()
  val none = IO.Success(None)
  val `false` = IO.Success(false)
  val `true` = IO.Success(true)
  val zero = IO.Success(0)
  val emptyReader = IO.Success(SliceReader(Slice.emptyBytes))
  val emptyBytes = IO.Success(Slice.emptyBytes)
  val noneTime = IO.Success(None)
  val emptySeqBytes = IO.Success(Seq.empty[Slice[Byte]])

  sealed trait Async[+T] {
    def isFailure: Boolean
    def isSuccess: Boolean
    def isLater: Boolean
    def flatMap[U](f: T => Async[U]): Async[U]
    def mapAsync[U](f: T => U): Async[U]
    def unsafeGet: T
    def safeGet: IO.Async[T]
    def safeGetBlocking: IO[T]
    def safeGetFuture(implicit ec: ExecutionContext): Future[IO[T]]
    def getOrElse[U >: T](default: => U): U
    def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U]
    def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U]
    def failed: IO[Throwable]
    def flatten[U](implicit ev: T <:< IO.Async[U]): IO.Async[U]
  }

  implicit class IterableIOImplicit[T: ClassTag](iterable: Iterable[T]) {

    def foreachIO[R](f: T => IO[R], failFast: Boolean = true): Option[IO.Failure[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[R]] = None
      while (it.hasNext && (failure.isEmpty || !failFast)) {
        f(it.next()) match {
          case failed @ IO.Failure(_) =>
            failure = Some(failed)
          case _ =>
        }
      }
      failure
    }

    //returns the first IO.Success(Some(_)).
    def untilSome[R](f: T => IO[Option[R]]): IO[Option[(R, T)]] = {
      iterable.iterator foreach {
        item =>
          f(item) match {
            case IO.Success(Some(value)) =>
              return IO.Success(Some(value, item))

            case IO.Success(None) =>

            case IO.Failure(exception) =>
              return IO.Failure(exception)
          }
      }
      IO.none
    }

    def mapIO[R: ClassTag](ioBlock: T => IO[R],
                           recover: (Slice[R], IO.Failure[Slice[R]]) => Unit = (_: Slice[R], _: IO.Failure[Slice[R]]) => (),
                           failFast: Boolean = true): IO[Slice[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[Slice[R]]] = None
      val results = Slice.create[R](iterable.size)
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        ioBlock(it.next()) match {
          case IO.Success(value) =>
            results add value

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.error))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Success(results)
      }
    }

    def flattenIO[R: ClassTag](ioBlock: T => IO[Iterable[R]],
                               recover: (Iterable[R], IO.Failure[Slice[R]]) => Unit = (_: Iterable[R], _: IO.Failure[Iterable[R]]) => (),
                               failFast: Boolean = true): IO[Iterable[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[Slice[R]]] = None
      val results = ListBuffer.empty[R]
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        ioBlock(it.next()) match {
          case IO.Success(value) =>
            value foreach (results += _)

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.error))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Success(results)
      }
    }

    def foldLeftIO[R: ClassTag](r: R,
                                failFast: Boolean = true,
                                recover: (R, IO.Failure[R]) => Unit = (_: R, _: IO.Failure[R]) => ())(f: (R, T) => IO[R]): IO[R] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[R]] = None
      var result: R = r
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        f(result, it.next()) match {
          case IO.Success(value) =>
            if (failure.isEmpty)
              result = value

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.error))
        }
      }
      failure match {
        case Some(failure) =>
          recover(result, failure)
          failure
        case None =>
          IO.Success(result)
      }
    }
  }

  sealed trait Error {
    def toException: Throwable
  }

  object Error {

    sealed trait Busy extends Error {
      def busy: BusyBoolean
      def isFree: Boolean =
        !busy.isBusy
    }

    case object None extends Busy {
      override def busy: BusyBoolean = BusyBoolean.notBusy
      override def toException: Throwable = new Exception("Not busy")
    }

    case class OpeningFile(file: Path, busy: BusyBoolean) extends Busy {
      override def toException: Throwable = new Exception(s"Failed to open file $file")
    }

    case class DecompressingIndex(busy: BusyBoolean) extends Busy {
      override def toException: Throwable = new Exception("Failed to decompress index")
    }

    case class DecompressionValues(busy: BusyBoolean) extends Busy {
      override def toException: Throwable = new Exception("Failed to decompress index")
    }

    case class ReadingHeader(busy: BusyBoolean) extends Busy {
      override def toException: Throwable = new Exception("Failed to decompress index")
    }

    case class FetchingValue(busy: BusyBoolean) extends Busy {
      override def toException: Throwable = new Exception("Failed to decompress index")
    }

    /**
      * This error can also be turned into Busy and LevelActor can use it to listen to when
      * there are no more overlapping Segments.
      */
    case object OverlappingPushSegment extends Error {
      override def toException: Throwable = new Exception("Contains overlapping busy Segments")
    }

    case object NoSegmentsRemoved extends Error {
      override def toException: Throwable = new Exception("No Segments Removed")
    }

    case object NotSentToNextLevel extends Error {
      override def toException: Throwable = new Exception("Not sent to next Level")
    }

    case class ReceivedKeyValuesToMergeWithoutTargetSegment(keyValueCount: Int) extends Error {
      override def toException: Throwable = new Exception(s"Received key-values to merge without target Segment - keyValueCount: $keyValueCount")
    }

    case class System(exception: Throwable) extends Error {
      override def toException: Throwable = exception
    }
  }

  def getOrNone[T](block: => T): Option[T] =
    try
      Option(block)
    catch {
      case _: Exception =>
        None
    }

  def apply[T](f: => T): IO[T] =
    try IO.Success(f) catch {
      case ex: Throwable =>
        IO.Failure(Error.System(ex))
    }

  object Catch {
    def apply[T](f: => IO[T]): IO[T] =
      try
        f
      catch {
        case ex: Exception =>
          IO.Failure(Error.System(ex))
      }
  }

  final case class Success[+T](value: T) extends IO[T] with IO.Async[T] {
    override def isFailure: Boolean = false
    override def isSuccess: Boolean = true
    override def isLater: Boolean = false
    override def unsafeGet: T = value
    override def safeGet: IO.Success[T] = this
    override def safeGetBlocking: IO.Success[T] = this
    override def safeGetFuture(implicit ec: ExecutionContext): Future[IO.Success[T]] = Future.successful(this)
    override def getOrElse[U >: T](default: => U): U = unsafeGet
    override def orElse[U >: T](default: => IO[U]): IO.Success[U] = this
    override def flatMap[U](f: T => IO[U]): IO[U] = IO.Catch(f(value))
    override def flatMap[U](f: T => Async[U]): Async[U] = f(value)
    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = value
    override def flatten[U](implicit ev: T <:< IO.Async[U]): IO.Async[U] = value
    override def foreach[U](f: T => U): Unit = f(value)
    override def map[U](f: T => U): IO[U] = IO[U](f(value))
    override def mapAsync[U](f: T => U): IO.Async[U] = IO(f(unsafeGet)).asInstanceOf[IO.Async[U]]
    override def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U] = this
    override def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U] = this
    override def failed: IO[Throwable] = Failure(new UnsupportedOperationException("IO.Success.failed"))
    override def toOption: Option[T] = Some(value)
    override def toEither: Either[Throwable, T] = Right(value)
    override def filter(p: T => Boolean): IO[T] =
      IO.Catch(if (p(value)) this else IO.Failure(new NoSuchElementException("Predicate does not hold for " + value)))
    override def toFuture: Future[T] = Future.successful(value)
    override def toTry: scala.util.Try[T] = scala.util.Success(value)
    override def onFailure[U >: T](f: IO.Failure[U] => Unit): IO[U] = this
  }

  object Async {
    def runSafe[T](f: => T): IO.Async[T] =
      try IO.Success(f) catch {
        case ex: Throwable =>
          recover(ex, f)
      }

    def recover[T](failure: IO.Failure[T], operation: => T): IO.Async[T] =
      failure.error match {
        case busy: Error.Busy =>
          Async(operation, busy)

        case Error.System(system) =>
          recover(system, operation)

        case Error.OverlappingPushSegment | Error.OverlappingPushSegment | Error.NoSegmentsRemoved | Error.NotSentToNextLevel | _: Error.ReceivedKeyValuesToMergeWithoutTargetSegment =>
          failure
      }

    def recover[T](exception: Throwable, operation: => T): IO.Async[T] =
      exception match {
        //the following Exceptions will occur when a file was being read but
        //it was closed or deleted when it was being read. There is no AtomicBoolean busy
        //associated with these exception and should simply be retried.
        case IO.BusyException(error) => IO.Later(operation, error)
        case _: NoSuchFileException => IO.Later(operation, Error.None)
        case _: FileNotFoundException => IO.Later(operation, Error.None)
        case _: AsynchronousCloseException => IO.Later(operation, Error.None)
        case _: ClosedChannelException => IO.Later(operation, Error.None)
        case _: NullPointerException => IO.Later(operation, Error.None)
        case _ => IO.Failure(exception)
      }

    def apply[T](value: => T, error: Error.Busy): Async[T] =
      new Later(_ => value, error)
  }
  object Later {
    def apply[T](value: => T, error: Error.Busy): Later[T] =
      new Later(_ => value, error)
  }

  final case class Later[T](value: Unit => T,
                            error: Error.Busy) extends IO.Async[T] {

    @volatile private var _value: Option[T] = None

    def isValueDefined: Boolean =
      _value.isDefined

    def isValueEmpty: Boolean =
      !isValueDefined

    def isFailure: Boolean = false
    def isSuccess: Boolean = false
    def isLater: Boolean = true
    def isBusy = error.busy.isBusy

    /**
      * Opens all [[IO.Async]] types to read the final value in a blocking manner.
      */
    def safeGetBlocking: IO[T] = {

      @tailrec
      def doGet(later: IO.Later[T]): IO[T] = {
        BusyBoolean.blockUntilFree(later.error.busy)
        later.safeGet match {
          case success @ IO.Success(_) =>
            success

          case later: IO.Later[T] =>
            doGet(later)

          case failure @ IO.Failure(_) =>
            failure
        }
      }

      doGet(this)
    }

    /**
      * Opens all [[IO.Async]] types to read the final value in a non-blocking manner.
      */
    def safeGetFuture(implicit ec: ExecutionContext): Future[IO[T]] = {

      def doGet(later: IO.Later[T]): Future[IO[T]] =
        BusyBoolean.future(later.error.busy).map(_ => later.safeGet) flatMap {
          case success @ IO.Success(_) =>
            Future.successful(success)

          case later: IO.Later[T] =>
            doGet(later)

          case failure @ IO.Failure(_) =>
            Future.successful(failure)
        }

      doGet(this)
    }

    /**
      * Runs composed functions.
      */
    private def forceGet: T =
      _value getOrElse {
        val got = value()
        _value = Some(got)
        got
      }

    /**
      * If value is readable gets or fetches and return's the value or else throws [[BusyException]].
      *
      * Don't not call this function use [[safeGetFuture]] or [[safeGetBlocking]] instead.
      */
    def unsafeGet: T =
      if (_value.isDefined || !isBusy)
        forceGet
      else
        throw BusyException(error)

    def safeGet: IO.Async[T] =
      if (_value.isDefined || !isBusy)
        IO.Async.runSafe(unsafeGet)
      else
        this

    def getOrElse[U >: T](default: => U): U =
      IO(forceGet).getOrElse(default)

    def flatMap[U](f: T => IO.Async[U]): IO.Later[U] =
      IO.Later(
        value = _ => f(unsafeGet).unsafeGet,
        error = error
      )

    def flatten[U](implicit ev: T <:< IO.Async[U]): IO.Async[U] = forceGet
    def map[U](f: T => U): IO.Async[U] = IO.Later[U]((_: Unit) => f(forceGet), error)
    def mapAsync[U](f: T => U): IO.Async[U] = map(f)
    def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U] = IO(forceGet).recover(f)
    def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U] = IO(forceGet).recoverWith(f)
    def failed: IO[Throwable] = IO(forceGet).failed
  }

  object Failure {
    def apply[T](exception: Throwable): IO.Failure[T] =
      IO.Failure(Error.System(exception))

    val busyOverlappingPushSegments = IO.Failure(IO.Error.OverlappingPushSegment)
  }

  final case class Failure[+T](error: Error) extends IO[T] with IO.Async[T] {
    override def isFailure: Boolean = true
    override def isSuccess: Boolean = false
    override def isLater: Boolean = false
    override def unsafeGet: T = throw error.toException
    override def safeGet: IO.Failure[T] = this
    override def safeGetBlocking: IO.Failure[T] = this
    override def safeGetFuture(implicit ec: ExecutionContext): Future[IO.Failure[T]] = Future.successful(this)
    override def getOrElse[U >: T](default: => U): U = default
    override def orElse[U >: T](default: => IO[U]): IO[U] = IO.Catch(default)
    override def flatMap[U](f: T => IO[U]): IO[U] = this.asInstanceOf[IO[U]]
    override def flatMap[U](f: T => IO.Async[U]): IO.Async[U] = this.asInstanceOf[Async[U]]
    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = this.asInstanceOf[IO[U]]
    override def flatten[U](implicit ev: T <:< IO.Async[U]): IO.Async[U] = this.asInstanceOf[IO.Async[U]]
    override def foreach[U](f: T => U): Unit = ()
    override def map[U](f: T => U): IO[U] = this.asInstanceOf[IO[U]]
    override def mapAsync[U](f: T => U): IO.Async[U] = this.asInstanceOf[IO.Async[U]]
    override def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U] =
      IO.Catch(if (f isDefinedAt error.toException) Success(f(error.toException)) else this)

    override def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U] =
      IO.Catch(if (f isDefinedAt error.toException) f(error.toException) else this)

    override def failed: IO[Throwable] = Success(error.toException)
    override def toOption: Option[T] = None
    override def toEither: Either[Throwable, T] = Left(error.toException)
    override def filter(p: T => Boolean): IO[T] = this
    override def toFuture: Future[T] = Future.failed(error.toException)
    override def toTry: scala.util.Try[T] = scala.util.Failure(error.toException)
    override def onFailure[U >: T](f: IO.Failure[U] => Unit): IO[U] = {
      f(this)
      this
    }
    def exception: Throwable = error.toException
    //    def toAsync[U >: T](operation: => U): IO.Async[U] = IO.Failure.async(this, operation)
    def recoverToAsync[U](operation: => IO.Async[U]): IO.Async[U] =
      IO.Async.recover(this, ()) flatMap {
        _ =>
          operation
      }
  }
}