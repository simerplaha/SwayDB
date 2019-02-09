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
  * [[IO.Success]] and [[IO.Failure]] are similar to types in [[scala.util.Try]].
  *
  * [[IO.Async]] is for performing synchronous and asynchronous IO.
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
  def recoverWith[U >: T](f: PartialFunction[IO.Error, IO[U]]): IO[U]
  def recover[U >: T](f: PartialFunction[IO.Error, U]): IO[U]
  def toOption: Option[T]
  def flatten[U](implicit ev: T <:< IO[U]): IO[U]
  def failed: IO[IO.Error]
  def toEither: Either[IO.Error, T]
  def toFuture: Future[T]
  def toTry: scala.util.Try[T]
}

object IO {

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
    def flatMapAsync[U](f: T => IO.Async[U]): IO.Async[U]
    def mapAsync[U](f: T => U): IO.Async[U]
    def unsafeGet: T
    def safeGet: IO.Async[T]
    def safeGetBlocking: IO[T]
    def safeGetFuture(implicit ec: ExecutionContext): Future[IO[T]]
    def getOrElse[U >: T](default: => U): U
    def recover[U >: T](f: PartialFunction[IO.Error, U]): IO[U]
    def recoverWith[U >: T](f: PartialFunction[IO.Error, IO[U]]): IO[U]
    def failed: IO[IO.Error]
    def flattenAsync[U](implicit ev: T <:< IO.Async[U]): IO.Async[U]
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

  /**
    * Exception types for all known [[IO.Error]]s that can occur. Each [[IO.Error]] can be converted to
    * Exception which which can then be converted back to [[IO.Error]].
    *
    * SwayDB's code itself does not use these exception it uses [[IO.Error]] type. These types are handy when
    * converting an [[IO]] type to [[scala.util.Try]] by the client using [[IO.toTry]].
    */
  object Exception {
    case class Busy(error: Error.Busy) extends Exception("Is busy")
    case class OpeningFile(file: Path, busy: BusyBoolean) extends Exception(s"Failed to open file $file")
    case class DecompressingIndex(busy: BusyBoolean) extends Exception("Failed to decompress index")
    case class DecompressionValues(busy: BusyBoolean) extends Exception("Failed to decompress values")
    case class FetchingValue(busy: BusyBoolean) extends Exception("Failed to fetch value")
    case class ReadingHeader(busy: BusyBoolean) extends Exception("Failed to read header")
    case object OverlappingPushSegment extends Exception("Contains overlapping busy Segments")
    case object NoSegmentsRemoved extends Exception("No Segments Removed")
    case object NotSentToNextLevel extends Exception("Not sent to next Level")
    case class ReceivedKeyValuesToMergeWithoutTargetSegment(keyValueCount: Int) extends Exception(s"Received key-values to merge without target Segment - keyValueCount: $keyValueCount")
  }

  sealed trait Error {
    def exception: Throwable
  }

  object Error {

    def apply[T](exception: Throwable): IO.Error =
      exception match {
        //known Exception that can occur which can return their typed Error version.
        case exception: IO.Exception.Busy => exception.error
        case exception: IO.Exception.OpeningFile => Error.OpeningFile(exception.file, exception.busy)
        case exception: IO.Exception.DecompressingIndex => Error.DecompressingIndex(exception.busy)
        case exception: IO.Exception.DecompressionValues => Error.DecompressionValues(exception.busy)
        case exception: IO.Exception.FetchingValue => Error.FetchingValue(exception.busy)
        case exception: IO.Exception.ReadingHeader => Error.ReadingHeader(exception.busy)
        case exception: IO.Exception.ReceivedKeyValuesToMergeWithoutTargetSegment => Error.ReceivedKeyValuesToMergeWithoutTargetSegment(exception.keyValueCount)

        case IO.Exception.OverlappingPushSegment => Error.OverlappingPushSegment
        case IO.Exception.NoSegmentsRemoved => Error.NoSegmentsRemoved
        case IO.Exception.NotSentToNextLevel => Error.NotSentToNextLevel

        //the following Exceptions will occur when a file was being read but
        //it was closed or deleted when it was being read. There is no AtomicBoolean busy
        //associated with these exception and should simply be retried.
        case exception: NoSuchFileException => Error.NoSuchFile(exception)
        case exception: FileNotFoundException => Error.FileNotFound(exception)
        case exception: AsynchronousCloseException => Error.AsynchronousClose(exception)
        case exception: ClosedChannelException => Error.ClosedChannel(exception)
        case exception: NullPointerException => Error.NullPointer(exception)
        case exception => Error.Fatal(exception)
      }

    sealed trait Busy extends Error {
      def busy: BusyBoolean
      def isFree: Boolean =
        !busy.isBusy
    }

    case class OpeningFile(file: Path, busy: BusyBoolean) extends Busy {
      override def exception: IO.Exception.OpeningFile = IO.Exception.OpeningFile(file, busy)
    }

    case class NoSuchFile(exception: NoSuchFileException) extends Busy {
      override def busy: BusyBoolean = BusyBoolean.notBusy
    }

    case class FileNotFound(exception: FileNotFoundException) extends Busy {
      override def busy: BusyBoolean = BusyBoolean.notBusy
    }

    case class AsynchronousClose(exception: AsynchronousCloseException) extends Busy {
      override def busy: BusyBoolean = BusyBoolean.notBusy
    }

    case class ClosedChannel(exception: ClosedChannelException) extends Busy {
      override def busy: BusyBoolean = BusyBoolean.notBusy
    }

    case class NullPointer(exception: NullPointerException) extends Busy {
      override def busy: BusyBoolean = BusyBoolean.notBusy
    }

    case class DecompressingIndex(busy: BusyBoolean) extends Busy {
      override def exception: IO.Exception.DecompressingIndex = IO.Exception.DecompressingIndex(busy)
    }

    case class DecompressionValues(busy: BusyBoolean) extends Busy {
      override def exception: IO.Exception.DecompressionValues = IO.Exception.DecompressionValues(busy)
    }

    case class ReadingHeader(busy: BusyBoolean) extends Busy {
      override def exception: IO.Exception.ReadingHeader = IO.Exception.ReadingHeader(busy)
    }

    case class FetchingValue(busy: BusyBoolean) extends Busy {
      override def exception: IO.Exception.FetchingValue = IO.Exception.FetchingValue(busy)
    }

    /**
      * This error can also be turned into Busy and LevelActor can use it to listen to when
      * there are no more overlapping Segments.
      */
    case object OverlappingPushSegment extends Error {
      override def exception: Throwable = IO.Exception.OverlappingPushSegment
    }

    case object NoSegmentsRemoved extends Error {
      override def exception: Throwable = IO.Exception.NoSegmentsRemoved
    }

    case object NotSentToNextLevel extends Error {
      override def exception: Throwable = IO.Exception.NotSentToNextLevel
    }

    case class ReceivedKeyValuesToMergeWithoutTargetSegment(keyValueCount: Int) extends Error {
      override def exception: IO.Exception.ReceivedKeyValuesToMergeWithoutTargetSegment =
        IO.Exception.ReceivedKeyValuesToMergeWithoutTargetSegment(keyValueCount)
    }

    /**
      * Error that are not known and indicate something unexpected went wrong like a file corruption.
      *
      * Pre-cautions and implemented in place to even recover from these failures using tools like AppendixRepairer.
      * This Error is not expected to occur on healthy databases.
      */
    case class Fatal(exception: Throwable) extends Error
  }

  def getOrNone[T](block: => T): Option[T] =
    try
      Option(block)
    catch {
      case _: Throwable =>
        None
    }

  def apply[T](f: => T): IO[T] =
    try IO.Success(f) catch {
      case ex: Throwable =>
        IO.Failure(IO.Error(ex))
    }

  object Catch {
    def apply[T](f: => IO[T]): IO[T] =
      try
        f
      catch {
        case ex: Throwable =>
          IO.Failure(IO.Error(ex))
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
    override def flatMap[U](f: T => IO[U]): IO[U] = IO.Catch(f(unsafeGet))
    override def flatMapAsync[U](f: T => IO.Async[U]): IO.Async[U] = f(unsafeGet)
    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = unsafeGet
    override def flattenAsync[U](implicit ev: T <:< IO.Async[U]): IO.Async[U] = unsafeGet
    override def foreach[U](f: T => U): Unit = f(unsafeGet)
    override def map[U](f: T => U): IO[U] = IO[U](f(unsafeGet))
    override def mapAsync[U](f: T => U): IO.Async[U] = IO(f(unsafeGet)).asInstanceOf[IO.Async[U]]
    override def recover[U >: T](f: PartialFunction[IO.Error, U]): IO[U] = this
    override def recoverWith[U >: T](f: PartialFunction[IO.Error, IO[U]]): IO[U] = this
    override def failed: IO[IO.Error] = Failure(Error.Fatal(new UnsupportedOperationException("IO.Success.failed")))
    override def toOption: Option[T] = Some(unsafeGet)
    override def toEither: Either[IO.Error, T] = Right(unsafeGet)
    override def filter(p: T => Boolean): IO[T] =
      IO.Catch(if (p(unsafeGet)) this else IO.Failure(Error.Fatal(new NoSuchElementException("Predicate does not hold for " + unsafeGet))))
    override def toFuture: Future[T] = Future.successful(unsafeGet)
    override def toTry: scala.util.Try[T] = scala.util.Success(unsafeGet)
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
          IO.Async(operation, busy)

        case Error.Fatal(system) =>
          recover(system, operation)

        case Error.OverlappingPushSegment | Error.OverlappingPushSegment | Error.NoSegmentsRemoved | Error.NotSentToNextLevel | _: Error.ReceivedKeyValuesToMergeWithoutTargetSegment =>
          failure
      }

    def recover[T](exception: Throwable, operation: => T): IO.Async[T] =
      Error(exception) match {
        case error: Error.Busy => IO.Later(operation, error)
        case other: Error => IO.Failure(other)
      }

    def apply[T](value: => T, error: Error.Busy): IO.Async[T] =
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
      * If value is readable gets or fetches and return's the value or else throws [[IO.Exception.Busy]].
      *
      * @throws [[IO.Exception.Busy]]. Used for Java API.
      */
    @throws[IO.Exception.Busy]
    def unsafeGet: T =
      if (_value.isDefined || !isBusy)
        forceGet
      else
        throw IO.Exception.Busy(error)

    def safeGet: IO.Async[T] =
      if (_value.isDefined || !isBusy)
        IO.Async.runSafe(unsafeGet)
      else
        this

    def getOrElse[U >: T](default: => U): U =
      IO(forceGet).getOrElse(default)

    def flatMapAsync[U](f: T => IO.Async[U]): IO.Later[U] =
      IO.Later(
        value = _ => f(unsafeGet).unsafeGet,
        error = error
      )

    def flattenAsync[U](implicit ev: T <:< IO.Async[U]): IO.Async[U] = forceGet
    def map[U](f: T => U): IO.Async[U] = IO.Later[U]((_: Unit) => f(forceGet), error)
    def mapAsync[U](f: T => U): IO.Async[U] = map(f)
    def recover[U >: T](f: PartialFunction[IO.Error, U]): IO[U] = IO(forceGet).recover(f)
    def recoverWith[U >: T](f: PartialFunction[IO.Error, IO[U]]): IO[U] = IO(forceGet).recoverWith(f)
    def failed: IO[IO.Error] = IO(forceGet).failed
  }

  object Failure {
    val busyOverlappingPushSegments = IO.Failure(IO.Error.OverlappingPushSegment)

    def apply[T](exception: Throwable): IO.Failure[T] =
      IO.Failure[T](IO.Error(exception))
  }

  final case class Failure[+T](error: Error) extends IO[T] with IO.Async[T] {
    override def isFailure: Boolean = true
    override def isSuccess: Boolean = false
    override def isLater: Boolean = false
    override def unsafeGet: T = throw error.exception
    override def safeGet: IO.Failure[T] = this
    override def safeGetBlocking: IO.Failure[T] = this
    override def safeGetFuture(implicit ec: ExecutionContext): Future[IO.Failure[T]] = Future.successful(this)
    override def getOrElse[U >: T](default: => U): U = default
    override def orElse[U >: T](default: => IO[U]): IO[U] = IO.Catch(default)
    override def flatMap[U](f: T => IO[U]): IO[U] = this.asInstanceOf[IO[U]]
    override def flatMapAsync[U](f: T => IO.Async[U]): IO.Async[U] = this.asInstanceOf[Async[U]]
    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = this.asInstanceOf[IO[U]]
    override def flattenAsync[U](implicit ev: T <:< IO.Async[U]): IO.Async[U] = this.asInstanceOf[IO.Async[U]]
    override def foreach[U](f: T => U): Unit = ()
    override def map[U](f: T => U): IO[U] = this.asInstanceOf[IO[U]]
    override def mapAsync[U](f: T => U): IO.Async[U] = this.asInstanceOf[IO.Async[U]]
    override def recover[U >: T](f: PartialFunction[IO.Error, U]): IO[U] =
      IO.Catch(if (f isDefinedAt error) Success(f(error)) else this)

    override def recoverWith[U >: T](f: PartialFunction[IO.Error, IO[U]]): IO[U] =
      IO.Catch(if (f isDefinedAt error) f(error) else this)

    override def failed: IO[IO.Error] = Success(error)
    override def toOption: Option[T] = None
    override def toEither: Either[IO.Error, T] = Left(error)
    override def filter(p: T => Boolean): IO[T] = this
    override def toFuture: Future[T] = Future.failed(error.exception)
    override def toTry: scala.util.Try[T] = scala.util.Failure(error.exception)
    override def onFailure[U >: T](f: IO.Failure[U] => Unit): IO[U] = {
      f(this)
      this
    }
    def exception: Throwable = error.exception
    //    def toAsync[U >: T](operation: => U): IO.Async[U] = IO.Failure.async(this, operation)
    def recoverToAsync[U](operation: => IO.Async[U]): IO.Async[U] =
      IO.Async.recover(this, ()) flatMapAsync {
        _ =>
          operation
      }
  }
}