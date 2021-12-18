/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb

import swaydb.IO.RecoverableExceptionHandler
import swaydb.effect.Reserve

import java.io.FileNotFoundException
import java.nio.ReadOnlyBufferException
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException}
import java.nio.file.{NoSuchFileException, Path}

protected sealed trait Error {
  def exception: Throwable
}

object Error {

  sealed trait Level extends Error
  sealed trait Log extends Level
  sealed trait Segment extends Level

  /**
   * Errors types for all possible known errors that can occurs on databases start-up.
   */
  sealed trait Boot extends Error
  sealed trait Delete extends Level
  sealed trait Close extends Delete
  sealed trait IO extends Segment with Log with Close
  sealed trait API extends Error

  object Segment {
    implicit object ExceptionHandler extends RecoverableExceptionHandler[Error.Segment] {
      override def toException(f: Error.Segment): Throwable =
        f.exception

      override def toError(e: Throwable): Error.Segment =
        Error(e) match {
          case segment: Error.Segment =>
            segment

          case error: Error =>
            Error.Fatal(error.exception)
        }

      override def recoverFrom(f: Error.Segment): Option[Reserve[Unit]] =
        f match {
          case recoverable: Error.Recoverable =>
            Some(recoverable.reserve)

          case _: Error =>
            None
        }
    }
  }

  object Level {
    implicit object ExceptionHandler extends RecoverableExceptionHandler[Error.Level] {
      override def toException(f: Error.Level): Throwable =
        f.exception

      override def toError(e: Throwable): Error.Level =
        Error(e) match {
          case segment: Error.Level =>
            segment

          case error: Error =>
            Error.Fatal(error.exception)
        }

      override def recoverFrom(f: Error.Level): Option[Reserve[Unit]] =
        f match {
          case recoverable: Error.Recoverable =>
            Some(recoverable.reserve)

          case _: Error =>
            None
        }
    }
  }

  object Log {
    implicit object ExceptionHandler extends RecoverableExceptionHandler[Error.Log] {
      override def toException(f: Error.Log): Throwable =
        f.exception

      override def toError(e: Throwable): Error.Log =
        Error(e) match {
          case segment: Error.Log =>
            segment

          case error: Error =>
            Error.Fatal(error.exception)
        }

      override def recoverFrom(f: Error.Log): Option[Reserve[Unit]] =
        f match {
          case recoverable: Error.Recoverable =>
            Some(recoverable.reserve)

          case _: Error =>
            None
        }
    }
  }

  object Boot {
    implicit object ExceptionHandler extends RecoverableExceptionHandler[Error.Boot] {
      override def toException(f: Error.Boot): Throwable =
        f.exception

      override def toError(e: Throwable): Error.Boot =
        Error(e) match {
          case segment: Error.Boot =>
            segment

          case error: Error =>
            Error.Fatal(error.exception)
        }

      override def recoverFrom(f: Error.Boot): Option[Reserve[Unit]] =
        f match {
          case recoverable: Error.Recoverable =>
            Some(recoverable.reserve)

          case _: Error =>
            None
        }
    }
  }

  object Close {
    implicit object ExceptionHandler extends RecoverableExceptionHandler[Error.Close] {
      override def toException(f: Error.Close): Throwable =
        f.exception

      override def toError(e: Throwable): Error.Close =
        Error(e) match {
          case segment: Error.Close =>
            segment

          case error: Error =>
            Error.Fatal(error.exception)
        }

      override def recoverFrom(f: Error.Close): Option[Reserve[Unit]] =
        f match {
          case recoverable: Error.Recoverable =>
            Some(recoverable.reserve)

          case _: Error =>
            None
        }
    }
  }

  object Delete {
    implicit object ExceptionHandler extends RecoverableExceptionHandler[Error.Delete] {
      override def toException(f: Error.Delete): Throwable =
        f.exception

      override def toError(e: Throwable): Error.Delete =
        Error(e) match {
          case segment: Error.Delete =>
            segment

          case error: Error =>
            Error.Fatal(error.exception)
        }

      override def recoverFrom(f: Error.Delete): Option[Reserve[Unit]] =
        f match {
          case recoverable: Error.Recoverable =>
            Some(recoverable.reserve)

          case _: Error =>
            None
        }
    }
  }

  object API {
    implicit object ExceptionHandler extends RecoverableExceptionHandler[Error.API] {
      override def toException(f: Error.API): Throwable =
        f.exception

      override def toError(e: Throwable): Error.API =
        Error(e) match {
          case segment: Error.API =>
            segment

          case error: Error =>
            Error.Fatal(error.exception)
        }

      override def recoverFrom(f: Error.API): Option[Reserve[Unit]] =
        f match {
          case recoverable: Error.Recoverable =>
            Some(recoverable.reserve)

          case _: Error =>
            None
        }
    }
  }

  object IO {
    implicit object ExceptionHandler extends RecoverableExceptionHandler[Error.IO] {
      override def toException(f: Error.IO): Throwable =
        f.exception

      override def toError(e: Throwable): Error.IO =
        Error(e) match {
          case segment: Error.IO =>
            segment

          case error: Error =>
            Error.Fatal(error.exception)
        }

      override def recoverFrom(f: Error.IO): Option[Reserve[Unit]] =
        f match {
          case recoverable: Error.Recoverable =>
            Some(recoverable.reserve)

          case _: Error =>
            None
        }
    }
  }

  def apply[T](exception: Throwable): Error =
    exception match {
      //known Exception that can occur which can return their typed Error version.
      case exception: Exception.Busy                 => exception.error
      case exception: Exception.OpeningFile          => Error.OpeningFile(exception.file, exception.reserve)
      case exception: Exception.ReservedResource     => Error.ReservedResource(exception.reserve)
      case exception: Exception.NullMappedByteBuffer => Error.NullMappedByteBuffer(exception)
      case exception: Exception.NoSuchFile           => Error.NoSuchFile(exception.file)

      //the following Exceptions will occur when a file was being read but
      //it was closed or deleted when it was being read. There is no AtomicBoolean busy
      //associated with these exception and should simply be retried.
      case exception: NoSuchFileException        => Error.NoSuchFile(exception)
      case exception: FileNotFoundException      => Error.FileNotFound(exception)
      case exception: AsynchronousCloseException => Error.AsynchronousClose(exception)
      case exception: ClosedChannelException     => Error.ClosedChannel(exception)

      case Exception.OverlappingPushSegment         => Error.OverlappingPushSegment
      case Exception.NoSegmentsRemoved              => Error.NoSegmentsRemoved
      case Exception.NotSentToNextLevel             => Error.NotSentToNextLevel
      case exception: Exception.OverlappingFileLock => swaydb.Error.UnableToLockDirectory(exception)

      case exception: ReadOnlyBufferException => Error.ReadOnlyBuffer(exception)

      case exception: Exception.FailedToWriteAllBytes   => Error.FailedToWriteAllBytes(exception)
      case exception: Exception.CannotCopyInMemoryFiles => Error.CannotCopyInMemoryFiles(exception)
      case exception: Exception.SegmentFileMissing      => Error.SegmentFileMissing(exception)
      case exception: Exception.InvalidBaseId           => Error.InvalidKeyValueId(exception)

      case exception: Exception.FunctionNotFound => Error.FunctionNotFound(exception.functionId)

      case exception: Exception.NotAnIntFile     => Error.NotAnIntFile(exception)
      case exception: Exception.UnknownExtension => Error.UnknownExtension(exception)

      case exception: Exception.GetOnIncompleteDeferredFutureIO => Error.GetOnIncompleteDeferredFutureIO(exception)

      case exception: Exception.MissingFunctions => Error.MissingFunctions(exception.functions)

      case exception @ (_: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException) =>
        Error.DataAccess(DataAccess.message, exception)

      //Unknown error.
      case exception: Throwable => Error.Fatal(exception)
    }

  trait Recoverable extends Error.Segment {
    def reserve: Reserve[Unit]
    def isFree: Boolean =
      !reserve.isBusy
  }

  case class OpeningFile(file: Path, reserve: Reserve[Unit]) extends Recoverable with Error.IO {
    override def exception: Exception.OpeningFile = Exception.OpeningFile(file, reserve)
  }

  object NoSuchFile {
    def apply(exception: NoSuchFileException) =
      new NoSuchFile(None, Some(exception))

    def apply(path: Path) =
      new NoSuchFile(Some(path), None)
  }

  case class NoSuchFile(path: Option[Path], exp: Option[NoSuchFileException]) extends Recoverable with Error.IO {
    override def reserve: Reserve[Unit] = Reserve.free(name = s"${this.productPrefix}. Path: $path")
    override def exception: Throwable = exp getOrElse {
      path match {
        case Some(path) =>
          new NoSuchFileException(path.toString)

        case None =>
          new NoSuchFileException("No path set.")
      }
    }
  }

  case class FileNotFound(exception: FileNotFoundException) extends Recoverable with Error.IO {
    override def reserve: Reserve[Unit] = Reserve.free(name = s"${this.productPrefix}")
  }

  case class AsynchronousClose(exception: AsynchronousCloseException) extends Recoverable with Error.IO {
    override def reserve: Reserve[Unit] = Reserve.free(name = s"${this.productPrefix}")
  }

  case class ClosedChannel(exception: ClosedChannelException) extends Recoverable with Error.IO {
    override def reserve: Reserve[Unit] = Reserve.free(name = s"${this.productPrefix}")
  }

  case class NullMappedByteBuffer(exception: Exception.NullMappedByteBuffer) extends Recoverable with Error.IO {
    override def reserve: Reserve[Unit] = Reserve.free(name = s"${this.productPrefix}")
  }

  case class ReservedResource(reserve: Reserve[Unit]) extends Recoverable with Error.Close with Error.Delete with Error.Boot with Error.API {
    override def exception: Exception.ReservedResource = Exception.ReservedResource(reserve)
  }

  case class MissingFunctions(functions: Iterable[String]) extends Error.Close with Error.Delete with Error.Boot with Error.API {
    override def exception: Exception.MissingFunctions = Exception.MissingFunctions(functions)
  }

  case class GetOnIncompleteDeferredFutureIO(exception: Exception.GetOnIncompleteDeferredFutureIO) extends Recoverable with Error.IO {
    def reserve = exception.reserve
  }

  case class NotAnIntFile(exception: Exception.NotAnIntFile) extends Error.IO {
    def path = exception.path
  }
  case class UnknownExtension(exception: Exception.UnknownExtension) extends Error.IO {
    def path = exception.path
  }

  /**
   * This error can also be turned into Busy and LevelActor can use it to listen to when
   * there are no more overlapping Segments.
   */
  case object OverlappingPushSegment extends Error.Level {
    override def exception: Throwable = Exception.OverlappingPushSegment
  }

  case object NoSegmentsRemoved extends Error.Level {
    override def exception: Throwable = Exception.NoSegmentsRemoved
  }

  case object NotSentToNextLevel extends Error.Level {
    override def exception: Throwable = Exception.NotSentToNextLevel
  }

  case class ReadOnlyBuffer(exception: ReadOnlyBufferException) extends Error.IO

  case class FunctionNotFound(functionId: String) extends Error.API with Error.Segment {
    override def exception: Throwable = swaydb.Exception.FunctionNotFound(functionId)
  }

  case class UnableToLockDirectory(exception: swaydb.Exception.OverlappingFileLock) extends Error.Boot

  object DataAccess {
    def message =
      "IO the input or the accessed data was in incorrect format/order. Please see the exception to find out the cause."
  }
  case class DataAccess(message: String, exception: Throwable) extends Error.IO
  case class SegmentFileMissing(exception: Exception.SegmentFileMissing) extends Error.Boot {
    def path = exception.path
  }

  case class FailedToWriteAllBytes(exception: Exception.FailedToWriteAllBytes) extends Error.IO {
    def written: Int = exception.written
    def expected: Int = exception.expected
    def bytesSize: Int = exception.bytesSize
  }

  case class CannotCopyInMemoryFiles(exception: Exception.CannotCopyInMemoryFiles) extends Error.Level {
    def file = exception.file
  }

  case class InvalidKeyValueId(exception: Exception.InvalidBaseId) extends Error.Segment {
    def id = exception.id
  }

  /**
   * Error that are not known and indicate something unexpected went wrong like a file corruption.
   *
   * Pre-cautions are implemented in place to even recover from these failures using tools like AppendixRepairer.
   * This Error is not expected to occur on healthy databases.
   */
  object Fatal {
    def apply(message: String): Fatal =
      new Fatal(new scala.Exception(message))

    implicit object ExceptionHandler extends swaydb.IO.ExceptionHandler[Error.Fatal] {
      override def toException(f: Error.Fatal): Throwable =
        f.exception

      override def toError(e: Throwable): Error.Fatal =
        Error.Fatal(e)
    }
  }

  case class Fatal(exception: Throwable) extends Error.API
                                         with Error.Boot
                                         with Error.IO
                                         with Error.Segment
                                         with Error.Level
                                         with Error.Log
                                         with Error.Close
                                         with Error.Delete

}
