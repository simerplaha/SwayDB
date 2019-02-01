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

import scala.collection.mutable

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

object IO {

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
}