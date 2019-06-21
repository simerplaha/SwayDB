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

package swaydb.data

import java.io.FileNotFoundException
import java.nio.ReadOnlyBufferException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.{NoSuchFileException, Path}

import swaydb.data.slice.{Slice, SliceReader}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

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
  def get: T
  def foreach[U](f: T => U): Unit
  def flatMap[U](f: T => IO[U]): IO[U]
  private[swaydb] def asAsync: IO.Async[T]
  private[swaydb] def asIO: IO[T]
  def map[U](f: T => U): IO[U]
  def exists(f: T => Boolean): Boolean
  def filter(p: T => Boolean): IO[T]
  @inline final def withFilter(p: T => Boolean): WithFilter = new WithFilter(p)
  class WithFilter(p: T => Boolean) {
    def map[U](f: T => U): IO[U] = IO.this filter p map f
    def flatMap[U](f: T => IO[U]): IO[U] = IO.this filter p flatMap f
    def foreach[U](f: T => U): Unit = IO.this filter p foreach f
    def withFilter(q: T => Boolean): WithFilter = new WithFilter(x => p(x) && q(x))
  }
  def onFailureSideEffect(f: IO.Failure[T] => Unit): IO[T]
  def onSuccessSideEffect(f: T => Unit): IO[T]
  def onCompleteSideEffect(f: IO[T] => Unit): IO[T]
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

  sealed trait OK
  final case object OK extends OK

  val unit: IO.Success[Unit] = IO.Success()
  val none = IO.Success(None)
  val `false` = IO.Success(false)
  val `true` = IO.Success(true)
  val someTrue = IO.Success(Some(true))
  val someFalse = IO.Success(Some(false))
  val zero = IO.Success(0)
  val emptyReader = IO.Success(SliceReader(Slice.emptyBytes))
  val emptyBytes = IO.Success(Slice.emptyBytes)
  val emptySeqBytes = IO.Success(Seq.empty[Slice[Byte]])
  val ok = IO.Success(OK)
  val notImplemented = IO.Failure(new NotImplementedError())

  private[swaydb] sealed trait Async[+T] {
    def isFailure: Boolean
    def isSuccess: Boolean
    def isLater: Boolean
    private[swaydb] def flatMap[U](f: T => IO.Async[U]): IO.Async[U]
    private[swaydb] def mapAsync[U](f: T => U): IO.Async[U]
    def get: T
    def safeGet: IO.Async[T]
    def safeGetIfFileExists: IO.Async[T]
    def safeGetBlocking: IO[T]
    def safeGetBlockingIfFileExists: IO[T]
    def safeGetFuture(implicit ec: ExecutionContext): Future[T]
    def safeGetFutureIfFileExists(implicit ec: ExecutionContext): Future[T]
    def getOrElse[U >: T](default: => U): U
    def recover[U >: T](f: PartialFunction[IO.Error, U]): IO[U]
    def recoverWith[U >: T](f: PartialFunction[IO.Error, IO[U]]): IO[U]
    def failed: IO[IO.Error]
    private[swaydb] def flattenAsync[U](implicit ev: T <:< IO.Async[U]): IO.Async[U]
  }

  implicit class IterableIOImplicit[T: ClassTag](iterable: Iterable[T]) {

    def foreachIO[R](f: T => IO[R], failFast: Boolean = true): Option[IO.Failure[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[R]] = None
      while (it.hasNext && (failure.isEmpty || !failFast)) {
        f(it.next()) match {
          case failed @ IO.Failure(_) =>
            failure = Some(failed)
          case _: IO.Success[_] =>
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
              //Not a good idea to break out with return. Needs improvement.
              return IO.Success(Some(value, item))

            case IO.Success(None) =>
            //continue reading

            case IO.Failure(exception) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Failure(exception)
          }
      }
      IO.none
    }

    def mapIO[R: ClassTag](block: T => IO[R],
                           recover: (Slice[R], IO.Failure[Slice[R]]) => Unit = (_: Slice[R], _: IO.Failure[Slice[R]]) => (),
                           failFast: Boolean = true): IO[Slice[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[Slice[R]]] = None
      val results = Slice.create[R](iterable.size)
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        block(it.next()) match {
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

    def flatMapIO[R: ClassTag](ioBlock: T => IO[Iterable[R]],
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
    case class OpeningFile(file: Path, busy: Reserve[Unit]) extends Exception(s"Failed to open file $file")

    case class DecompressingIndex(busy: Reserve[Unit]) extends Exception("Failed to decompress index")
    case class DecompressionValues(busy: Reserve[Unit]) extends Exception("Failed to decompress values")
    case class FetchingValue(busy: Reserve[Unit]) extends Exception("Failed to fetch value")
    case class ReadingHeader(busy: Reserve[Unit]) extends Exception("Failed to read header")
    case class BusyFuture(busy: Reserve[Unit]) extends Exception("Busy future")

    case object OverlappingPushSegment extends Exception("Contains overlapping busy Segments")
    case object NoSegmentsRemoved extends Exception("No Segments Removed")
    case object NotSentToNextLevel extends Exception("Not sent to next Level")
    case class ReceivedKeyValuesToMergeWithoutTargetSegment(keyValueCount: Int) extends Exception(s"Received key-values to merge without target Segment - keyValueCount: $keyValueCount")

    /**
      * Does not have any direct [[IO.Error]] type associated with it since missing functions should be considered as [[IO.Error.Fatal]]
      * and should be resolved otherwise compaction will fail and pause for that Segment and will only continue ones this function
      * is available in function store.
      *
      * [[functionID]] itself is not logged or printed to console since it may contain sensitive data but instead this Exception
      * with the [[functionID]] is returned to the client for reads and the exception's string message is only logged.
      *
      * @param functionID the id of the missing function.
      */
    case class FunctionNotFound(functionID: Slice[Byte]) extends Exception("Function not found for ID.")
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
        case exception: IO.Exception.DecompressionValues => Error.DecompressingValues(exception.busy)
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
        case exception: ReadOnlyBufferException => Error.ReadOnlyBuffer(exception)

        //Fatal error. This error is not expected to occur on a healthy database. This error would indicate corruption.
        //AppendixRepairer can be used to repair map files.
        case exception => Error.Fatal(exception)
      }

    sealed trait Busy extends Error {
      def busy: Reserve[Unit]
      def isFree: Boolean =
        !busy.isBusy
    }

    case class OpeningFile(file: Path, busy: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.OpeningFile = IO.Exception.OpeningFile(file, busy)
    }

    object NoSuchFile {
      def apply(exception: NoSuchFileException) =
        new NoSuchFile(None, Some(exception))

      def apply(path: Path) =
        new NoSuchFile(Some(path), None)
    }
    case class NoSuchFile(path: Option[Path], exp: Option[NoSuchFileException]) extends Busy {
      override def busy: Reserve[Unit] = Reserve()
      override def exception: Throwable = exp getOrElse {
        path match {
          case Some(path) =>
            new NoSuchFileException(path.toString)
          case None =>
            new NoSuchFileException("No path set.")
        }
      }
    }

    case class FileNotFound(exception: FileNotFoundException) extends Busy {
      override def busy: Reserve[Unit] = Reserve()
    }

    case class AsynchronousClose(exception: AsynchronousCloseException) extends Busy {
      override def busy: Reserve[Unit] = Reserve()
    }

    case class ClosedChannel(exception: ClosedChannelException) extends Busy {
      override def busy: Reserve[Unit] = Reserve()
    }

    case class NullPointer(exception: NullPointerException) extends Busy {
      override def busy: Reserve[Unit] = Reserve()
    }

    case class DecompressingIndex(busy: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.DecompressingIndex = IO.Exception.DecompressingIndex(busy)
    }

    case class DecompressingValues(busy: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.DecompressionValues = IO.Exception.DecompressionValues(busy)
    }

    case class ReadingHeader(busy: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.ReadingHeader = IO.Exception.ReadingHeader(busy)
    }

    case class FetchingValue(busy: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.FetchingValue = IO.Exception.FetchingValue(busy)
    }

    case class BusyFuture(busy: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.BusyFuture = IO.Exception.BusyFuture(busy)
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

    case class ReadOnlyBuffer(exception: ReadOnlyBufferException) extends Error

    /**
      * Error that are not known and indicate something unexpected went wrong like a file corruption.
      *
      * Pre-cautions are implemented in place to even recover from these failures using tools like AppendixRepairer.
      * This Error is not expected to occur on healthy databases.
      */
    case class Fatal(exception: Throwable) extends Error
  }

  @inline final def tryOrNone[T](block: => T): Option[T] =
    try
      Option(block)
    catch {
      case _: Throwable =>
        None
    }

  @inline final def apply[T](f: => T): IO[T] =
    try IO.Success(f) catch {
      case ex: Throwable =>
        IO.Failure(IO.Error(ex))
    }

  object CatchLeak {
    @inline final def apply[T](f: => IO[T]): IO[T] =
      try
        f
      catch {
        case ex: Throwable =>
          IO.Failure(IO.Error(ex))
      }
  }

  def fromTry[T](tryBlock: Try[T]): IO[T] =
    tryBlock match {
      case scala.util.Success(value) =>
        IO.Success(value)

      case scala.util.Failure(exception) =>
        IO.Failure(exception)
    }

  def fromFuture[T](future: Future[T])(implicit ec: ExecutionContext): IO.Async[T] =
    IO.Async(future)

  final case class Success[+T](value: T) extends IO[T] with IO.Async[T] {
    override def isFailure: Boolean = false
    override def isSuccess: Boolean = true
    override def isLater: Boolean = false
    override def get: T = value
    override def exists(f: T => Boolean): Boolean = f(value)
    override def safeGet: IO.Success[T] = this
    override def safeGetIfFileExists: IO.Success[T] = this
    override def safeGetBlocking: IO.Success[T] = this
    override def safeGetBlockingIfFileExists: IO[T] = this
    override def safeGetFutureIfFileExists(implicit ec: ExecutionContext): Future[T] = Future.successful(value)
    override def safeGetFuture(implicit ec: ExecutionContext): Future[T] = Future.successful(value)
    override def getOrElse[U >: T](default: => U): U = get
    override def orElse[U >: T](default: => IO[U]): IO.Success[U] = this
    override def flatMap[U](f: T => IO[U]): IO[U] = IO.CatchLeak(f(get))
    private[swaydb] override def flatMap[U](f: T => IO.Async[U]): IO.Async[U] = f(get)
    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = get
    private[swaydb] override def flattenAsync[U](implicit ev: T <:< IO.Async[U]): IO.Async[U] = get
    override def foreach[U](f: T => U): Unit = f(get)
    override def map[U](f: T => U): IO[U] = IO[U](f(get))
    private[swaydb] override def mapAsync[U](f: T => U): IO.Async[U] = IO(f(get)).asInstanceOf[IO.Async[U]]
    override def recover[U >: T](f: PartialFunction[IO.Error, U]): IO[U] = this
    override def recoverWith[U >: T](f: PartialFunction[IO.Error, IO[U]]): IO[U] = this
    override def failed: IO[IO.Error] = Failure(Error.Fatal(new UnsupportedOperationException("IO.Success.failed")))
    override def toOption: Option[T] = Some(get)
    override def toEither: Either[IO.Error, T] = Right(get)
    override def filter(p: T => Boolean): IO[T] =
      IO.CatchLeak(if (p(get)) this else IO.Failure(Error.Fatal(new NoSuchElementException("Predicate does not hold for " + get))))
    override def toFuture: Future[T] = Future.successful(get)
    override def toTry: scala.util.Try[T] = scala.util.Success(get)
    override def onFailureSideEffect(f: IO.Failure[T] => Unit): IO.Success[T] = this
    override def onSuccessSideEffect(f: T => Unit): IO.Success[T] = {
      try f(get) finally {}
      this
    }
    override def onCompleteSideEffect(f: IO[T] => Unit): IO[T] = {
      try f(this) finally {}
      this
    }
    private[swaydb] override def asAsync: IO.Async[T] = this
    private[swaydb] override def asIO: IO[T] = this

  }

  private[swaydb] object Async {

    @inline final def runSafe[T](f: => T): IO.Async[T] =
      try IO.Success(f) catch {
        case ex: Throwable =>
          recover(ex, f)
      }

    @inline final def runSafeIfFileExists[T](f: => T): IO.Async[T] =
      try IO.Success(f) catch {
        case ex: Throwable =>
          recoverIfFileExists(ex, f)
      }

    def recover[T](failure: IO.Failure[T], operation: => T): IO.Async[T] =
      failure.error match {
        case busy: Error.Busy =>
          IO.Async(operation, busy)

        case Error.Fatal(exception) =>
          recover(exception, operation)

        case Error.OverlappingPushSegment |
             Error.NoSegmentsRemoved |
             Error.NotSentToNextLevel |
             _: Error.ReceivedKeyValuesToMergeWithoutTargetSegment |
             _: Error.ReadOnlyBuffer =>
          failure
      }

    def recover[T](exception: Throwable, operation: => T): IO.Async[T] =
      Error(exception) match {
        //@formatter:off
        case error: Error.Busy => IO.Later(operation, error)
        case other: Error => IO.Failure(other)
        //@formatter:on
      }

    def recoverIfFileExists[T](exception: Throwable, operation: => T): IO.Async[T] =
      Error(exception) match {
        //@formatter:off
        case error: Error.FileNotFound => IO.Failure(error)
        case error: Error.NoSuchFile => IO.Failure(error)
        case error: Error.NullPointer => IO.Failure(error)
        case error: Error.Busy => IO.Later(operation, error)
        case other: Error => IO.Failure(other)
        //@formatter:on
      }

    @inline final def apply[T](value: => T, error: Error.Busy): IO.Async[T] =
      new Later(_ => value, error)

    private[swaydb] final def apply[T](future: Future[T])(implicit ec: ExecutionContext): IO.Async[T] = {
      val error = IO.Error.BusyFuture(Reserve(()))
      future onComplete {
        _ =>
          Reserve.setFree(error.busy)
      }

      //here value will only be access when the above busy boolean is true
      //so the value should always exists at the time of Await.result
      //therefore the cost of blocking should be negligible.
      IO.Async(
        value = Await.result(future, Duration.Zero),
        error = error
      )
    }
  }

  private[swaydb] object Later {
    @inline final def apply[T](value: => T, error: Error.Busy): Later[T] =
      new Later(_ => value, error)
  }

  private[swaydb] final case class Later[T](value: Unit => T,
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
      * Runs composed functions does not perform any recovery.
      */
    private def forceGet: T =
      _value getOrElse {
        val got = value()
        _value = Some(got)
        got
      }

    override def safeGetBlockingIfFileExists: IO[T] = {
      @tailrec
      def doGet(later: IO.Later[T]): IO[T] = {
        Reserve.blockUntilFree(later.error.busy)
        later.safeGetIfFileExists match {
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
      * Opens all [[IO.Async]] types to read the final value in a blocking manner.
      */
    def safeGetBlocking: IO[T] = {

      @tailrec
      def doGet(later: IO.Later[T]): IO[T] = {
        Reserve.blockUntilFree(later.error.busy)
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
    def safeGetFuture(implicit ec: ExecutionContext): Future[T] = {

      def doGet(later: IO.Later[T]): Future[T] =
        Reserve.future(later.error.busy).map(_ => later.safeGet) flatMap {
          case IO.Success(value) =>
            Future.successful(value)

          case later: IO.Later[T] =>
            doGet(later)

          case IO.Failure(error) =>
            Future.failed(error.exception)
        }

      doGet(this)
    }

    /**
      * Opens all [[IO.Async]] types to read the final value in a non-blocking manner.
      */
    def safeGetFutureIfFileExists(implicit ec: ExecutionContext): Future[T] = {

      def doGet(later: IO.Later[T]): Future[T] =
        Reserve.future(later.error.busy).map(_ => later.safeGetIfFileExists) flatMap {
          case IO.Success(value) =>
            Future.successful(value)

          case later: IO.Later[T] =>
            doGet(later)

          case IO.Failure(error) =>
            Future.failed(error.exception)
        }

      doGet(this)
    }

    /**
      * If value is readable gets or fetches and return's the value or else throws [[IO.Exception.Busy]].
      *
      * @throws [[IO.Exception.Busy]]. Used for Java API.
      */
    @throws[IO.Exception.Busy]
    def get: T =
      if (_value.isDefined || !isBusy)
        forceGet
      else
        throw IO.Exception.Busy(error)

    def safeGet: IO.Async[T] =
      if (_value.isDefined || !isBusy)
        IO.Async.runSafe(get)
      else
        this

    def safeGetIfFileExists: IO.Async[T] =
      if (_value.isDefined || !isBusy)
        IO.Async.runSafeIfFileExists(get)
      else
        this

    def getOrElse[U >: T](default: => U): U =
      IO(forceGet).getOrElse(default)

    def flatMap[U](f: T => IO.Async[U]): IO.Later[U] =
      IO.Later(
        value = _ => f(get).get,
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
    val overlappingPushSegments = IO.Failure(IO.Error.OverlappingPushSegment)

    @inline final def apply[T](exception: Throwable): IO.Failure[T] =
      IO.Failure[T](IO.Error(exception))
  }

  final case class Failure[+T](error: Error) extends IO[T] with IO.Async[T] {
    override def isFailure: Boolean = true
    override def isSuccess: Boolean = false
    override def isLater: Boolean = false
    override def get: T = throw error.exception
    override def safeGet: IO.Failure[T] = this
    override def safeGetIfFileExists: IO.Failure[T] = this
    override def safeGetBlocking: IO.Failure[T] = this
    override def safeGetBlockingIfFileExists: IO[T] = this
    override def safeGetFutureIfFileExists(implicit ec: ExecutionContext): Future[T] = Future.failed(error.exception)
    override def safeGetFuture(implicit ec: ExecutionContext): Future[T] = Future.failed(error.exception)
    override def getOrElse[U >: T](default: => U): U = default
    override def orElse[U >: T](default: => IO[U]): IO[U] = IO.CatchLeak(default)
    override def flatMap[U](f: T => IO[U]): IO.Failure[U] = this.asInstanceOf[IO.Failure[U]]
    private[swaydb] override def flatMap[U](f: T => IO.Async[U]): IO.Async[U] = this.asInstanceOf[IO.Async[U]]
    override def flatten[U](implicit ev: T <:< IO[U]): IO.Failure[U] = this.asInstanceOf[IO.Failure[U]]
    private[swaydb] override def flattenAsync[U](implicit ev: T <:< IO.Async[U]): IO.Async[U] = this.asInstanceOf[IO.Async[U]]
    override def foreach[U](f: T => U): Unit = ()
    override def map[U](f: T => U): IO.Failure[U] = this.asInstanceOf[IO.Failure[U]]
    private[swaydb] override def mapAsync[U](f: T => U): IO.Async[U] = this.asInstanceOf[IO.Async[U]]
    override def recover[U >: T](f: PartialFunction[IO.Error, U]): IO[U] =
      IO.CatchLeak(if (f isDefinedAt error) IO.Success(f(error)) else this)

    override def recoverWith[U >: T](f: PartialFunction[IO.Error, IO[U]]): IO[U] =
      IO.CatchLeak(if (f isDefinedAt error) f(error) else this)

    override def failed: IO[IO.Error] = IO.Success(error)
    override def toOption: Option[T] = None
    override def toEither: Either[IO.Error, T] = Left(error)
    override def filter(p: T => Boolean): IO.Failure[T] = this
    override def toFuture: Future[T] = Future.failed(error.exception)
    override def toTry: scala.util.Try[T] = scala.util.Failure(error.exception)
    override def onFailureSideEffect(f: IO.Failure[T] => Unit): IO.Failure[T] = {
      try f(this) finally {}
      this
    }
    override def onCompleteSideEffect(f: IO[T] => Unit): IO[T] = onFailureSideEffect(f)
    override def onSuccessSideEffect(f: T => Unit): IO.Failure[T] = this
    def exception: Throwable = error.exception
    private[swaydb] def recoverToAsync[U](operation: => IO.Async[U]): IO.Async[U] =
    //it's already know it's a failure, do not run it again on recovery, just flatMap onto operation.
      IO.Async.recover(this, ()) flatMap {
        _ =>
          operation
      }

    private[swaydb] override def asAsync: IO.Async[T] = this
    private[swaydb] override def asIO: IO[T] = this
    override def exists(f: T => Boolean): Boolean = false
  }
}