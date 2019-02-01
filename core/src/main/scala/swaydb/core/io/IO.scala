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

package swaydb.core.io

import com.typesafe.scalalogging.LazyLogging
import java.io.IOException
import java.nio.channels.{FileLock, WritableByteChannel}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.mutable
import swaydb.core.segment.SegmentException
import swaydb.core.util.TryUtil
import swaydb.data.slice.Slice

sealed trait IO[+T] {
  def isFailure: Boolean
  def isSuccess: Boolean
  def isAsync: Boolean
  def getOrElse[U >: T](default: => U): U
  def orElse[U >: T](default: => IO[U]): IO[U]
  def get: T
  def foreach[U](f: T => U): Unit
  def flatMap[U](f: T => IO[U]): IO[U]
  def map[U](f: T => U): IO[U]
  def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U]
  def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U]
  def toOption: Option[T]
  def flatten[U](implicit ev: T <:< IO[U]): IO[U]
  def failed: IO[Throwable]
  def toEither: Either[Throwable, T]
}

object IO extends LazyLogging {

  def apply[T](f: => T): IO[T] =
    try IO.Success(f) catch {
      case ex: Throwable =>
        IO.Failure(ex)
    }

  def safe[T](f: => IO[T]): IO[T] =
    try f catch {
      case ex: Throwable =>
        IO.Failure(ex)
    }

  final case class Failure[+T](exception: Throwable) extends IO[T] {
    override def isFailure: Boolean = true
    override def isSuccess: Boolean = false
    override def isAsync: Boolean = false
    override def get: T = throw exception
    override def getOrElse[U >: T](default: => U): U = default
    override def orElse[U >: T](default: => IO[U]): IO[U] = IO.safe(default)
    override def flatMap[U](f: T => IO[U]): IO[U] = this.asInstanceOf[IO[U]]
    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = this.asInstanceOf[IO[U]]
    override def foreach[U](f: T => U): Unit = ()
    override def map[U](f: T => U): IO[U] = this.asInstanceOf[IO[U]]
    override def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U] =
      IO.safe(if (f isDefinedAt exception) Success(f(exception)) else this)

    override def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U] =
      IO.safe(if (f isDefinedAt exception) f(exception) else this)

    override def failed: IO[Throwable] = Success(exception)
    override def toOption: Option[T] = None
    override def toEither: Either[Throwable, T] = Left(exception)
  }

  final case class Success[+T](value: T) extends IO[T] {
    override def isFailure: Boolean = false
    override def isSuccess: Boolean = true
    override def isAsync: Boolean = false
    override def get = value
    override def getOrElse[U >: T](default: => U): U = get
    override def orElse[U >: T](default: => IO[U]): IO[U] = this
    override def flatMap[U](f: T => IO[U]): IO[U] =
      IO.safe(f(value))

    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = value
    override def foreach[U](f: T => U): Unit = f(value)
    override def map[U](f: T => U): IO[U] = IO[U](f(value))
    override def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U] = this
    override def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U] = this
    override def failed: IO[Throwable] = Failure(new UnsupportedOperationException("IO.Success.failed"))
    override def toOption: Option[T] = Some(value)
    override def toEither: Either[Throwable, T] = Right(value)
  }

  object Async {
    def apply[T](value: => T): Async[T] =
      new Async(_ => value)
  }

  final case class Async[T](value: Unit => T) {
    private val listeners: mutable.Queue[T => Any] = mutable.Queue.empty
    @volatile private var _value: Option[T] = None

    def ready(): Unit =
      this.synchronized {
        listeners.dequeueAll {
          function =>
            val got = get
            try function(got) catch {
              case _: Throwable =>
              //who shall we do?
            }
            true
        }
      }

    def isFailure: Boolean = false
    def isSuccess: Boolean = false
    def isAsync: Boolean = true
    def get: T =
      _value getOrElse {
        val got = value()
        _value = Some(got)
        got
      }

    def getOrListen[U >: T](listener: T => U): IO[U] =
      this.synchronized {
        IO(get) recoverWith {
          case exception: Exception =>
            listeners += listener
            IO.Failure(exception)
        }
      }

    def getOrElse[U >: T](default: => U): U = IO(get).getOrElse(default)
    def flatMap[U](f: T => Async[U]): Async[U] = Async(f(get).get)
    def flatMapIO[U](f: T => IO[U]): Async[U] = Async(f(get).get)
    def flatten[U](implicit ev: T <:< Async[U]): Async[U] = get
    def map[U](f: T => U): Async[U] = IO.Async[U](f(get))
    def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U] = IO(get).recover(f)
    def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U] = IO(get).recoverWith(f)
    def failed: IO[Throwable] = Failure(new UnsupportedOperationException("IO.Async.failed"))
  }

  def write(bytes: Slice[Byte],
            to: Path): scala.util.Try[Path] =
    scala.util.Try(Files.newByteChannel(to, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) flatMap {
      channel =>
        try {
          write(bytes, channel) map {
            _ =>
              to
          }
        } finally {
          channel.close()
        }
    }

  def write(bytes: Slice[Byte],
            channel: WritableByteChannel): scala.util.Try[Unit] =
    try {
      val written = channel write bytes.toByteBuffer
      // toByteBuffer uses size of Slice instead of written,
      // but here the check on written ensures that only the actually written bytes get written.
      // All the client code invoking writes to Disk using Slice should ensure that no Slice contains empty bytes.
      if (written != bytes.written)
        scala.util.Failure(SegmentException.FailedToWriteAllBytes(written, bytes.written, bytes.size))
      else
        TryUtil.successUnit
    } catch {
      case exception: Exception =>
        scala.util.Failure(exception)
    }

  def copy(copyFrom: Path,
           copyTo: Path): scala.util.Try[Path] =
    scala.util.Try {
      Files.copy(copyFrom, copyTo)
    }

  def delete(path: Path): scala.util.Try[Unit] =
    scala.util.Try(Files.delete(path))

  def deleteIfExists(path: Path): scala.util.Try[Unit] =
    if (exists(path))
      delete(path)
    else
      TryUtil.successUnit

  def createFile(path: Path): scala.util.Try[Path] =
    scala.util.Try {
      Files.createFile(path)
    }

  def createFileIfAbsent(path: Path): scala.util.Try[Path] =
    if (exists(path))
      scala.util.Success(path)
    else
      createFile(path)

  def exists(path: Path) =
    Files.exists(path)

  def notExists(path: Path) =
    !exists(path)

  def createDirectoryIfAbsent(path: Path): Path =
    if (exists(path))
      path
    else
      Files.createDirectory(path)

  def createDirectoriesIfAbsent(path: Path): Path =
    if (exists(path))
      path
    else
      Files.createDirectories(path)

  def walkDelete(folder: Path): scala.util.Try[Unit] =
    try {
      if (exists(folder))
        Files.walkFileTree(folder, new SimpleFileVisitor[Path]() {
          @throws[IOException]
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
            if (exc != null) throw exc
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        })
      TryUtil.successUnit
    } catch {
      case exception: Throwable =>
        scala.util.Failure(exception)
    }

  def release(lock: FileLock): scala.util.Try[Unit] =
    scala.util.Try {
      lock.release()
      lock.close()
    }

  def stream[T](path: Path)(f: DirectoryStream[Path] => T): T = {
    val stream: DirectoryStream[Path] = Files.newDirectoryStream(path)
    try
      f(stream)
    finally
      stream.close()
  }

  def release(lock: Option[FileLock]): scala.util.Try[Unit] =
    lock.map(release).getOrElse(TryUtil.successUnit)
}
