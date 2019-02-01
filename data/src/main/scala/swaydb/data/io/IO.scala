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
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.reflect.ClassTag
import swaydb.data.slice.{Slice, SliceReader}

/**
  * Similar to [[scala.util.Try]] but adds another type [[IO.Async]].
  */
sealed trait IO[+T] {
  def isFailure: Boolean
  def isSync: Boolean
  def isAsync: Boolean
  def getOrElse[U >: T](default: => U): U
  def orElse[U >: T](default: => IO[U]): IO[U]
  def get: T
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

  val successUnit: IO.Sync[Unit] = IO.Sync()
  val successNone = IO.Sync(None)
  val asyncNone = IO.Async(None)
  val successFalse = IO.Sync(false)
  val successTrue = IO.Sync(true)
  val successZero = IO.Sync(0)
  val emptyReader = IO.Sync(SliceReader(Slice.emptyBytes))
  val successEmptyBytes = IO.Sync(Slice.emptyBytes)
  val successNoneTime = IO.Sync(None)
  val successEmptySeqBytes = IO.Sync(Seq.empty[Slice[Byte]])

  def apply[T](f: => T): IO[T] =
    try IO.Sync(f) catch {
      case ex: Throwable =>
        IO.Failure(ex)
    }

  object Catch {
    def apply[T](f: => IO[T]): IO[T] =
      try
        f
      catch {
        case ex: Exception =>
          IO.Failure(ex)
      }
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
            case IO.Sync(Some(value)) =>
              return IO.Sync(Some(value, item))
            case IO.Sync(None) =>

            case IO.Failure(exception) =>
              return IO.Failure(exception)
          }
      }
      IO.successNone
    }

    def mapIO[R: ClassTag](ioBlock: T => IO[R],
                           recover: (Slice[R], IO.Failure[Slice[R]]) => Unit = (_: Slice[R], _: IO.Failure[Slice[R]]) => (),
                           failFast: Boolean = true): IO[Slice[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[Slice[R]]] = None
      val results = Slice.create[R](iterable.size)
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        ioBlock(it.next()) match {
          case IO.Sync(value) =>
            results add value

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.exception))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Sync(results)
      }
    }

    def flattenIterableIO[R: ClassTag](ioBlock: T => IO[Iterable[R]],
                                       recover: (Iterable[R], IO.Failure[Slice[R]]) => Unit = (_: Iterable[R], _: IO.Failure[Iterable[R]]) => (),
                                       failFast: Boolean = true): IO[Iterable[R]] = {
      val it = iterable.iterator
      var failure: Option[IO.Failure[Slice[R]]] = None
      val results = ListBuffer.empty[R]
      while ((!failFast || failure.isEmpty) && it.hasNext) {
        ioBlock(it.next()) match {
          case IO.Sync(value) =>
            value foreach (results += _)

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.exception))
        }
      }
      failure match {
        case Some(value) =>
          recover(results, value)
          value
        case None =>
          IO.Sync(results)
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
          case IO.Sync(value) =>
            if (failure.isEmpty)
              result = value

          case failed @ IO.Failure(_) =>
            failure = Some(IO.Failure(failed.exception))
        }
      }
      failure match {
        case Some(failure) =>
          recover(result, failure)
          failure
        case None =>
          IO.Sync(result)
      }
    }
  }

  def orNone[T](block: => T): Option[T] =
    try
      Option(block)
    catch {
      case _: Exception =>
        None
    }

  final case class Failure[+T](exception: Throwable) extends IO[T] with IO.Async[T] {
    override def isFailure: Boolean = true
    override def isSync: Boolean = false
    override def isAsync: Boolean = false
    override def get: T = throw exception
    override def getOrElse[U >: T](default: => U): U = default
    override def orElse[U >: T](default: => IO[U]): IO[U] = IO.Catch(default)
    override def flatMap[U](f: T => IO[U]): IO[U] = this.asInstanceOf[IO[U]]
    override def flatMap[U](f: T => Async[U]): Async[U] = this.asInstanceOf[Async[U]]
    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = this.asInstanceOf[IO[U]]
    override def foreach[U](f: T => U): Unit = ()
    override def map[U](f: T => U): IO[U] = this.asInstanceOf[IO[U]]
    override def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U] =
      IO.Catch(if (f isDefinedAt exception) Sync(f(exception)) else this)

    override def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U] =
      IO.Catch(if (f isDefinedAt exception) f(exception) else this)

    override def failed: IO[Throwable] = Sync(exception)
    override def toOption: Option[T] = None
    override def toEither: Either[Throwable, T] = Left(exception)
    override def filter(p: T => Boolean): IO[T] = this
    override def toFuture: Future[T] = Future.failed(exception)
    override def toTry: scala.util.Try[T] = scala.util.Failure(exception)
  }

  final case class Sync[+T](value: T) extends IO[T] with IO.Async[T] {
    override def isFailure: Boolean = false
    override def isSync: Boolean = true
    override def isAsync: Boolean = false
    override def get = value
    override def getOrElse[U >: T](default: => U): U = get
    override def orElse[U >: T](default: => IO[U]): IO[U] = this
    override def flatMap[U](f: T => IO[U]): IO[U] = IO.Catch(f(value))
    override def flatMap[U](f: T => Async[U]): Async[U] = f(value)
    override def flatten[U](implicit ev: T <:< IO[U]): IO[U] = value
    override def foreach[U](f: T => U): Unit = f(value)
    override def map[U](f: T => U): IO[U] = IO[U](f(value))
    override def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U] = this
    override def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U] = this
    override def failed: IO[Throwable] = Failure(new UnsupportedOperationException("IO.Success.failed"))
    override def toOption: Option[T] = Some(value)
    override def toEither: Either[Throwable, T] = Right(value)
    override def filter(p: T => Boolean): IO[T] =
      IO.Catch(if (p(value)) this else IO.Failure(new NoSuchElementException("Predicate does not hold for " + value)))
    override def toFuture: Future[T] = Future.successful(value)
    override def toTry: scala.util.Try[T] = scala.util.Success(value)
  }

  object Async {
    def apply[T](value: => T): Async[T] =
      new Later(_ => value)
  }
  object Later {
    def apply[T](value: => T): Later[T] =
      new Later(_ => value)
  }

  sealed trait Async[+T] {
    def isFailure: Boolean
    def isSync: Boolean
    def isAsync: Boolean
    def flatMap[U](f: T => Async[U]): Async[U]
    def get: T
    def getOrElse[U >: T](default: => U): U
    def recover[U >: T](f: PartialFunction[Throwable, U]): IO[U]
    def recoverWith[U >: T](f: PartialFunction[Throwable, IO[U]]): IO[U]
    def failed: IO[Throwable]
  }
  final case class Later[T](value: Unit => T) extends Async[T] {
    private val listeners: mutable.Queue[T => Any] = mutable.Queue.empty
    @volatile private var _value: Option[T] = None

    def isFailure: Boolean = false
    def isSync: Boolean = false
    def isAsync: Boolean = true

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