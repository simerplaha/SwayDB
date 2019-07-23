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

import java.io.FileNotFoundException
import java.nio.ReadOnlyBufferException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.{NoSuchFileException, Path}

import swaydb.data.Reserve
import swaydb.data.slice.{Slice, SliceReaderSafe}

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
sealed trait IO[+A] {
  def isFailure: Boolean
  def isSuccess: Boolean
  def isDeferred: Boolean
  def getOrElse[B >: A](default: => B): B
  def orElse[B >: A](default: => IO[B]): IO[B]
  def get: A
  def foreach[B](f: A => B): Unit
  def map[B](f: A => B): IO[B]
  def flatMap[B](f: A => IO[B]): IO[B]
  def asDeferred: IO.Defer[A]
  def asIO: IO[A]
  def exists(f: A => Boolean): Boolean
  def filter(p: A => Boolean): IO[A]
  @inline final def withFilter(p: A => Boolean): WithFilter = new WithFilter(p)
  class WithFilter(p: A => Boolean) {
    def map[B](f: A => B): IO[B] = IO.this filter p map f
    def flatMap[B](f: A => IO[B]): IO[B] = IO.this filter p flatMap f
    def foreach[B](f: A => B): Unit = IO.this filter p foreach f
    def withFilter(q: A => Boolean): WithFilter = new WithFilter(x => p(x) && q(x))
  }
  def onFailureSideEffect(f: IO.Failure[A] => Unit): IO[A]
  def onSuccessSideEffect(f: A => Unit): IO[A]
  def onCompleteSideEffect(f: IO[A] => Unit): IO[A]
  def recoverWith[B >: A](f: PartialFunction[IO.Error, IO[B]]): IO[B]
  def recover[B >: A](f: PartialFunction[IO.Error, B]): IO[B]
  def toOption: Option[A]
  def flatten[B](implicit ev: A <:< IO[B]): IO[B]
  def failed: IO[IO.Error]
  def toEither: Either[IO.Error, A]
  def toFuture: Future[A]
  def toTry: scala.util.Try[A]
}

object IO {

  sealed trait OK
  final case object OK extends OK

  val unit: IO.Success[Unit] = IO.Success()
  val none = IO.Success(None)
  val `false` = IO.Success(false)
  val `true` = IO.Success(true)
  val someTrue: IO[Some[Boolean]] = IO.Success(Some(true))
  val someFalse: IO[Some[Boolean]] = IO.Success(Some(false))
  val zero = IO.Success(0)
  val emptyReader = IO.Success(SliceReaderSafe(Slice.emptyBytes))
  val emptyBytes = IO.Success(Slice.emptyBytes)
  val emptySeqBytes = IO.Success(Seq.empty[Slice[Byte]])
  val ok = IO.Success(OK)
  val notImplemented = IO.Failure(new NotImplementedError())

  sealed trait Defer[+A] {
    def isFailure: Boolean
    def isSuccess: Boolean
    def isDeferred: Boolean
    def flatMap[B](f: A => IO.Defer[B]): IO.Defer[B]
    def mapDeferred[B](f: A => B): IO.Defer[B]
    def get: A
    def run: IO.Defer[A]
    def runIfFileExists: IO.Defer[A]
    def runBlocking: IO[A]
    def runBlockingIfFileExists: IO[A]
    def runInFuture(implicit ec: ExecutionContext): Future[A]
    def runInFutureIfFileExists(implicit ec: ExecutionContext): Future[A]
    def getOrElse[B >: A](default: => B): B
    def recover[B >: A](f: PartialFunction[IO.Error, B]): IO[B]
    def recoverWith[B >: A](f: PartialFunction[IO.Error, IO[B]]): IO[B]
    def failed: IO[IO.Error]
    def flattenDeferred[B](implicit ev: A <:< IO.Defer[B]): IO.Defer[B]
  }

  implicit class IterableIOImplicit[A: ClassTag](iterable: Iterable[A]) {

    def foreachIO[R](f: A => IO[R], failFast: Boolean = true): Option[IO.Failure[R]] = {
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
    def untilSome[R](f: A => IO[Option[R]]): IO[Option[(R, A)]] = {
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

    def untilSomeResult[R](f: A => IO[Option[R]]): IO[Option[R]] = {
      iterable.iterator foreach {
        item =>
          f(item) match {
            case IO.Success(Some(value)) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Success(Some(value))

            case IO.Success(None) =>
            //continue reading

            case IO.Failure(exception) =>
              //Not a good idea to break out with return. Needs improvement.
              return IO.Failure(exception)
          }
      }
      IO.none
    }

    def mapIO[R: ClassTag](block: A => IO[R],
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

    def flatMapIO[R: ClassTag](ioBlock: A => IO[Iterable[R]],
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
                                recover: (R, IO.Failure[R]) => Unit = (_: R, _: IO.Failure[R]) => ())(f: (R, A) => IO[R]): IO[R] = {
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
    case class ReservedValue(busy: Reserve[Unit]) extends Exception("Failed to fetch value")
    case class ReadingHeader(busy: Reserve[Unit]) extends Exception("Failed to read header")
    case class NullMappedByteBuffer(exception: Exception, busy: Reserve[Unit]) extends Exception(exception)
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

    def apply[A](exception: Throwable): IO.Error =
      exception match {
        //known Exception that can occur which can return their typed Error version.
        case exception: IO.Exception.Busy => exception.error
        case exception: IO.Exception.OpeningFile => Error.OpeningFile(exception.file, exception.busy)
        case exception: IO.Exception.DecompressingIndex => Error.DecompressingIndex(exception.busy)
        case exception: IO.Exception.DecompressionValues => Error.DecompressingValues(exception.busy)
        case exception: IO.Exception.ReservedValue => Error.ReservedValue(exception.busy)
        case exception: IO.Exception.ReadingHeader => Error.ReadingHeader(exception.busy)
        case exception: IO.Exception.ReceivedKeyValuesToMergeWithoutTargetSegment => Error.ReceivedKeyValuesToMergeWithoutTargetSegment(exception.keyValueCount)
        case exception: IO.Exception.NullMappedByteBuffer => Error.NullMappedByteBuffer(exception)

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
        case exception: ReadOnlyBufferException => Error.ReadOnlyBuffer(exception)

        //Fatal error. This error is not expected to occur on a healthy database. This error would indicate corruption.
        //AppendixRepairer can be used to repair map files.
        case exception: Throwable => Error.Fatal(exception)
      }

    sealed trait Busy extends Error {
      def reserve: Reserve[Unit]
      def isFree: Boolean =
        !reserve.isBusy
    }

    case class OpeningFile(file: Path, reserve: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.OpeningFile = IO.Exception.OpeningFile(file, reserve)
    }

    object NoSuchFile {
      def apply(exception: NoSuchFileException) =
        new NoSuchFile(None, Some(exception))

      def apply(path: Path) =
        new NoSuchFile(Some(path), None)
    }
    case class NoSuchFile(path: Option[Path], exp: Option[NoSuchFileException]) extends Busy {
      override def reserve: Reserve[Unit] = Reserve()
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
      override def reserve: Reserve[Unit] = Reserve()
    }

    case class AsynchronousClose(exception: AsynchronousCloseException) extends Busy {
      override def reserve: Reserve[Unit] = Reserve()
    }

    case class ClosedChannel(exception: ClosedChannelException) extends Busy {
      override def reserve: Reserve[Unit] = Reserve()
    }

    case class NullMappedByteBuffer(exception: IO.Exception.NullMappedByteBuffer) extends Busy {
      override def reserve: Reserve[Unit] = Reserve()
    }

    case class DecompressingIndex(reserve: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.DecompressingIndex = IO.Exception.DecompressingIndex(reserve)
    }

    case class DecompressingValues(reserve: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.DecompressionValues = IO.Exception.DecompressionValues(reserve)
    }

    case class ReadingHeader(reserve: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.ReadingHeader = IO.Exception.ReadingHeader(reserve)
    }

    case class ReservedValue(reserve: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.ReservedValue = IO.Exception.ReservedValue(reserve)
    }

    case class BusyFuture(reserve: Reserve[Unit]) extends Busy {
      override def exception: IO.Exception.BusyFuture = IO.Exception.BusyFuture(reserve)
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
    object Fatal {
      def apply(message: String): Fatal =
        new Fatal(new Exception(message))
    }
    case class Fatal(exception: Throwable) extends Error
  }

  @inline final def tryOrNone[A](block: => A): Option[A] =
    try
      Option(block)
    catch {
      case _: Throwable =>
        None
    }

  @inline final def apply[A](f: => A): IO[A] =
    try IO.Success(f) catch {
      case ex: Throwable =>
        IO.Failure(IO.Error(ex))
    }

  object CatchLeak {
    @inline final def apply[A](f: => IO[A]): IO[A] =
      try
        f
      catch {
        case ex: Throwable =>
          IO.Failure(IO.Error(ex))
      }
  }

  def fromTry[A](tryBlock: Try[A]): IO[A] =
    tryBlock match {
      case scala.util.Success(value) =>
        IO.Success(value)

      case scala.util.Failure(exception) =>
        IO.Failure(exception)
    }

  def fromFuture[A](future: Future[A])(implicit ec: ExecutionContext): IO.Defer[A] =
    IO.Defer(future)

  object Success {
    def apply[A](value: A): IO.Success[A] =
      new Success(value)
  }

  final case class Success[+A](value: A) extends IO.Defer[A] with IO[A] {
    override def isFailure: Boolean = false
    override def isSuccess: Boolean = true
    override def isDeferred: Boolean = false
    override def get: A = value
    override def exists(f: A => Boolean): Boolean = f(value)
    override def run: IO.Success[A] = this
    override def runIfFileExists: IO.Success[A] = this
    override def runBlocking: IO.Success[A] = this
    override def runBlockingIfFileExists: IO[A] = this
    override def runInFutureIfFileExists(implicit ec: ExecutionContext): Future[A] = Future.successful(value)
    override def runInFuture(implicit ec: ExecutionContext): Future[A] = Future.successful(value)
    override def getOrElse[B >: A](default: => B): B = get
    override def orElse[B >: A](default: => IO[B]): IO.Success[B] = this
    override def flatMap[B](f: A => IO[B]): IO[B] = IO.CatchLeak(f(get))
    override def flatMap[B](f: A => IO.Defer[B]): IO.Defer[B] = f(get)
    override def flatten[B](implicit ev: A <:< IO[B]): IO[B] = get
    override def flattenDeferred[B](implicit ev: A <:< IO.Defer[B]): IO.Defer[B] = get
    override def foreach[B](f: A => B): Unit = f(get)
    override def map[B](f: A => B): IO[B] = IO[B](f(get))
    override def mapDeferred[B](f: A => B): IO.Defer[B] = IO(f(get)).asInstanceOf[IO.Defer[B]]
    override def recover[B >: A](f: PartialFunction[IO.Error, B]): IO[B] = this
    override def recoverWith[B >: A](f: PartialFunction[IO.Error, IO[B]]): IO[B] = this
    override def failed: IO[IO.Error] = Failure(Error.Fatal(new UnsupportedOperationException("IO.Success.failed")))
    override def toOption: Option[A] = Some(get)
    override def toEither: Either[IO.Error, A] = Right(get)
    override def filter(p: A => Boolean): IO[A] =
      IO.CatchLeak(if (p(get)) this else IO.Failure(Error.Fatal(new NoSuchElementException("Predicate does not hold for " + get))))
    override def toFuture: Future[A] = Future.successful(get)
    override def toTry: scala.util.Try[A] = scala.util.Success(get)
    override def onFailureSideEffect(f: IO.Failure[A] => Unit): IO.Success[A] = this
    override def onSuccessSideEffect(f: A => Unit): IO.Success[A] = {
      try f(get) finally {}
      this
    }
    override def onCompleteSideEffect(f: IO[A] => Unit): IO[A] = {
      try f(this) finally {}
      this
    }
    override def asDeferred: IO.Defer[A] = this
    override def asIO: IO[A] = this
  }

  object Defer {

    @inline final def recover[A](f: => A): IO.Defer[A] =
      try IO.Success(f) catch {
        case ex: Throwable =>
          recover(ex, f)
      }

    @inline final def recoverIfFileExists[A](f: => A): IO.Defer[A] =
      try IO.Success(f) catch {
        case ex: Throwable =>
          recoverIfFileExists(ex, f)
      }

    def recover[A](failure: IO.Failure[A], operation: => A): IO.Defer[A] =
      failure.error match {
        case busy: Error.Busy =>
          IO.Defer(operation, busy)

        case Error.Fatal(exception) =>
          recover(exception, operation)

        case Error.OverlappingPushSegment |
             Error.NoSegmentsRemoved |
             Error.NotSentToNextLevel |
             _: Error.ReceivedKeyValuesToMergeWithoutTargetSegment |
             _: Error.ReadOnlyBuffer =>
          failure
      }

    def recover[A](exception: Throwable, operation: => A): IO.Defer[A] =
      Error(exception) match {
        case error: Error.Busy =>
          IO.Deferred(operation, error)

        case other: Error =>
          IO.Failure(other)
      }

    def recoverIfFileExists[A](exception: Throwable, operation: => A): IO.Defer[A] =
      Error(exception) match {
        //@formatter:off
        case error: Error.FileNotFound => IO.Failure(error)
        case error: Error.NoSuchFile => IO.Failure(error)
        case error: Error.NullMappedByteBuffer => IO.Failure(error)
        case error: Error.Busy => IO.Deferred(operation, error)
        case other: Error => IO.Failure(other)
        //@formatter:on
      }

    @inline final def apply[A](value: => A, error: Error.Busy): IO.Defer[A] =
      new Deferred(_ => value, error)

    final def apply[A](future: Future[A])(implicit ec: ExecutionContext): IO.Defer[A] = {
      val error = IO.Error.BusyFuture(Reserve(()))
      future onComplete {
        _ =>
          Reserve.setFree(error.reserve)
      }

      //here value will only be access when the above busy boolean is true
      //so the value should always exists at the time of Await.result
      //therefore the cost of blocking should be negligible.
      IO.Defer(
        value = Await.result(future, Duration.Zero),
        error = error
      )
    }
  }

  object Deferred {
    @inline final def apply[A](value: => A, error: Error.Busy): Deferred[A] =
      new Deferred(_ => value, error)
  }

  final case class Deferred[A](value: Unit => A,
                               error: Error.Busy) extends IO.Defer[A] {

    @volatile private var _value: Option[A] = None

    def isValueDefined: Boolean =
      _value.isDefined

    def isValueEmpty: Boolean =
      !isValueDefined

    def isFailure: Boolean = false
    def isSuccess: Boolean = false
    def isDeferred: Boolean = true
    def isBusy = error.reserve.isBusy

    /**
      * Runs composed functions does not perform any recovery.
      */
    private def forceGet: A =
      _value getOrElse {
        val got = value()
        _value = Some(got)
        got
      }

    override def runBlockingIfFileExists: IO[A] = {
      @tailrec
      def doGet(later: IO.Deferred[A]): IO[A] = {
        Reserve.blockUntilFree(later.error.reserve)
        later.runIfFileExists match {
          case success @ IO.Success(_) =>
            success

          case later: IO.Deferred[A] =>
            doGet(later)

          case failure @ IO.Failure(_) =>
            failure
        }
      }

      doGet(this)
    }

    /**
      * Opens all [[IO.Defer]] types to read the final value in a blocking manner.
      */
    def runBlocking: IO[A] = {

      @tailrec
      def doGet(later: IO.Deferred[A]): IO[A] = {
        Reserve.blockUntilFree(later.error.reserve)
        later.run match {
          case success @ IO.Success(_) =>
            success

          case later: IO.Deferred[A] =>
            doGet(later)

          case failure @ IO.Failure(_) =>
            failure
        }
      }

      doGet(this)
    }

    /**
      * Opens all [[IO.Defer]] types to read the final value in a non-blocking manner.
      */
    def runInFuture(implicit ec: ExecutionContext): Future[A] = {

      def doGet(later: IO.Deferred[A]): Future[A] =
        Reserve.future(later.error.reserve).map(_ => later.run) flatMap {
          case IO.Success(value) =>
            Future.successful(value)

          case later: IO.Deferred[A] =>
            doGet(later)

          case IO.Failure(error) =>
            Future.failed(error.exception)
        }

      doGet(this)
    }

    /**
      * Opens all [[IO.Defer]] types to read the final value in a non-blocking manner.
      */
    def runInFutureIfFileExists(implicit ec: ExecutionContext): Future[A] = {

      def doGet(later: IO.Deferred[A]): Future[A] =
        Reserve.future(later.error.reserve).map(_ => later.runIfFileExists) flatMap {
          case IO.Success(value) =>
            Future.successful(value)

          case later: IO.Deferred[A] =>
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
    def get: A =
      if (_value.isDefined || !isBusy)
        forceGet
      else
        throw IO.Exception.Busy(error)

    def run: IO.Defer[A] =
      if (_value.isDefined || !isBusy)
        IO.Defer.recover(get)
      else
        this

    def runIfFileExists: IO.Defer[A] =
      if (_value.isDefined || !isBusy)
        IO.Defer.recoverIfFileExists(get)
      else
        this

    def getOrElse[B >: A](default: => B): B =
      IO(forceGet).getOrElse(default)

    def flatMap[B](f: A => IO.Defer[B]): IO.Deferred[B] =
      IO.Deferred(
        value = _ => f(get).get,
        error = error
      )

    def flattenDeferred[B](implicit ev: A <:< IO.Defer[B]): IO.Defer[B] = forceGet
    def map[B](f: A => B): IO.Defer[B] = IO.Deferred[B]((_: Unit) => f(forceGet), error)
    def mapDeferred[B](f: A => B): IO.Defer[B] = map(f)
    def recover[B >: A](f: PartialFunction[IO.Error, B]): IO[B] = IO(forceGet).recover(f)
    def recoverWith[B >: A](f: PartialFunction[IO.Error, IO[B]]): IO[B] = IO(forceGet).recoverWith(f)
    def failed: IO[IO.Error] = IO(forceGet).failed
  }

  object Failure {
    val overlappingPushSegments = IO.Failure(IO.Error.OverlappingPushSegment)

    @inline final def apply[A](exception: Throwable): IO.Failure[A] =
      IO.Failure[A](IO.Error(exception))

    @inline final def apply[A](message: String): IO.Failure[A] =
      IO.Failure[A](IO.Error(new Exception(message)))
  }

  final case class Failure[+A](error: Error) extends IO.Defer[A] with IO[A] {
    override def isFailure: Boolean = true
    override def isSuccess: Boolean = false
    override def isDeferred: Boolean = false
    override def get: A = throw error.exception
    override def run: IO.Failure[A] = this
    override def runIfFileExists: IO.Failure[A] = this
    override def runBlocking: IO.Failure[A] = this
    override def runBlockingIfFileExists: IO[A] = this
    override def runInFutureIfFileExists(implicit ec: ExecutionContext): Future[A] = Future.failed(error.exception)
    override def runInFuture(implicit ec: ExecutionContext): Future[A] = Future.failed(error.exception)
    override def getOrElse[B >: A](default: => B): B = default
    override def orElse[B >: A](default: => IO[B]): IO[B] = IO.CatchLeak(default)
    override def flatMap[B](f: A => IO[B]): IO.Failure[B] = this.asInstanceOf[IO.Failure[B]]
    override def flatMap[B](f: A => IO.Defer[B]): IO.Defer[B] = this.asInstanceOf[IO.Defer[B]]
    override def flatten[B](implicit ev: A <:< IO[B]): IO.Failure[B] = this.asInstanceOf[IO.Failure[B]]
    override def flattenDeferred[B](implicit ev: A <:< IO.Defer[B]): IO.Defer[B] = this.asInstanceOf[IO.Defer[B]]
    override def foreach[B](f: A => B): Unit = ()
    override def map[B](f: A => B): IO.Failure[B] = this.asInstanceOf[IO.Failure[B]]
    override def mapDeferred[B](f: A => B): IO.Defer[B] = this.asInstanceOf[IO.Defer[B]]
    override def recover[B >: A](f: PartialFunction[IO.Error, B]): IO[B] =
      IO.CatchLeak(if (f isDefinedAt error) IO.Success(f(error)) else this)

    override def recoverWith[B >: A](f: PartialFunction[IO.Error, IO[B]]): IO[B] =
      IO.CatchLeak(if (f isDefinedAt error) f(error) else this)

    override def failed: IO[IO.Error] = IO.Success(error)
    override def toOption: Option[A] = None
    override def toEither: Either[IO.Error, A] = Left(error)
    override def filter(p: A => Boolean): IO.Failure[A] = this
    override def toFuture: Future[A] = Future.failed(error.exception)
    override def toTry: scala.util.Try[A] = scala.util.Failure(error.exception)
    override def onFailureSideEffect(f: IO.Failure[A] => Unit): IO.Failure[A] = {
      try f(this) finally {}
      this
    }
    override def onCompleteSideEffect(f: IO[A] => Unit): IO[A] = onFailureSideEffect(f)
    override def onSuccessSideEffect(f: A => Unit): IO.Failure[A] = this
    def exception: Throwable = error.exception
    def recoverToDeferred[B](operation: => IO.Defer[B]): IO.Defer[B] =
    //it's already know it's a failure, do not run it again on recovery, just flatMap onto operation.
      IO.Defer.recover(this, ()) flatMap {
        _ =>
          operation
      }

    override def asDeferred: IO.Defer[A] = this
    override def asIO: IO[A] = this
    override def exists(f: A => Boolean): Boolean = false
  }
}