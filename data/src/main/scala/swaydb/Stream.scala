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

import swaydb.Stream.StreamBuilder
import swaydb.Tag.Implicits._

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 */
object Stream {

  /**
   * Create and empty [[Stream]].
   */
  def empty[A, T[_]](implicit tag: Tag[T]): Stream[A, T] =
    apply[A, T](Iterable.empty)

  def apply[A, T[_]](streamer: Streamer[A, T])(implicit tag: Tag[T]): Stream[A, T] =
    new Stream[A, T] {
      override def headOption(): T[Option[A]] =
        streamer.headOption

      override private[swaydb] def next(previous: A): T[Option[A]] =
        streamer.next(previous)
    }

  /**
   * Create a [[Stream]] from a collection.
   */
  def apply[A, T[_]](items: Iterable[A])(implicit tag: Tag[T]): Stream[A, T] =
    new Stream[A, T] {

      private val iterator = items.iterator

      private def step(): T[Option[A]] =
        if (iterator.hasNext)
          tag.success(Some(iterator.next()))
        else
          tag.none

      override def headOption(): T[Option[A]] = step()
      override private[swaydb] def next(previous: A): T[Option[A]] = step()
    }

  class StreamBuilder[A, T[_]](implicit tag: Tag[T]) extends mutable.Builder[A, Stream[A, T]] {
    private val items: ListBuffer[A] = ListBuffer.empty[A]

    override def +=(x: A): this.type = {
      items += x
      this
    }

    def asSeq: Seq[A] =
      items

    override def clear(): Unit =
      items.clear()

    override def result: Stream[A, T] =
      new Stream[A, T] {

        private val iterator = items.iterator

        def step(): T[Option[A]] =
          if (iterator.hasNext)
            tag.success(Some(iterator.next()))
          else
            tag.none

        override def headOption: T[Option[A]] = step()
        override private[swaydb] def next(previous: A): T[Option[A]] = step()
      }
  }

  implicit def canBuildFrom[A, T[_]](implicit tag: Tag[T]): CanBuildFrom[Stream[A, T], A, Stream[A, T]] =
    new CanBuildFrom[Stream[A, T], A, Stream[A, T]] {
      override def apply(from: Stream[A, T]) =
        new StreamBuilder()

      override def apply(): mutable.Builder[A, Stream[A, T]] =
        new StreamBuilder()
    }
}

/**
 * A [[Stream]] performs lazy iteration. It does not cache data and fetches data only if
 * it's required by the stream.
 *
 * @param tag Implementation for the tag type.
 * @tparam A stream item's type
 * @tparam T wrapper type.
 */
abstract class Stream[A, T[_]](implicit tag: Tag[T]) extends Streamable[A, T] { self =>

  /**
   * Private val used in [[tag.foldLeft]] for reading only single item.
   */
  private val takeOne = Some(1)

  def headOption: T[Option[A]]
  private[swaydb] def next(previous: A): T[Option[A]]

  def take(count: Int): Stream[A, T] =
    if (count == 0)
      Stream.empty
    else
      new Stream[A, T] {

        override def headOption: T[Option[A]] =
          self.headOption

        //flag to count how many were taken.
        private var taken = 1
        override private[swaydb] def next(previous: A): T[Option[A]] =
          if (taken == count)
            tag.none
          else
            tag.foldLeft(Option.empty[A], Some(previous), self, 0, takeOne) {
              case (_, next) =>
                taken += 1
                Some(next)
            }
      }

  def takeWhile(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {
      override def headOption: T[Option[A]] =
        self.headOption map {
          head =>
            if (head.exists(f))
              head
            else
              None
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        tag.foldLeft(Option.empty[A], Some(previous), self, 0, takeOne) {
          case (_, next) =>
            if (f(next))
              Some(next)
            else
              None
        }
    }

  def drop(count: Int): Stream[A, T] =
    if (count == 0)
      self
    else
      new Stream[A, T] {
        override def headOption: T[Option[A]] =
          self.headOption flatMap {
            headOption =>
              headOption map {
                head =>
                  if (count == 1)
                    next(head)
                  else
                    tag.foldLeft(Option.empty[A], Some(head), self, count - 1, takeOne) {
                      case (_, next) =>
                        Some(next)
                    }
              } getOrElse tag.none
          }

        override private[swaydb] def next(previous: A): T[Option[A]] =
          self.next(previous)
      }

  def dropWhile(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {
      override def headOption: T[Option[A]] =
        self.headOption flatMap {
          headOption =>
            headOption map {
              head =>
                if (f(head))
                  tag.collectFirst(head, self)(!f(_))
                else
                  tag.success(headOption)
            } getOrElse tag.none
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        self.next(previous)
    }

  def map[B](f: A => B): Stream[B, T] =
    new Stream[B, T] {

      var previousA: Option[A] = Option.empty

      override def headOption: T[Option[B]] =
        self.headOption map {
          previousAOption =>
            previousA = previousAOption
            previousAOption.map(f)
        }

      /**
       * Previous input parameter here is ignored so that parent stream can be read.
       */
      override private[swaydb] def next(previous: B): T[Option[B]] =
        previousA
          .map {
            previous =>
              self.next(previous) map {
                nextA =>
                  previousA = nextA
                  nextA
              }
          }
          .getOrElse(tag.none[A])
          .map(_.map(f))
    }

  def foreach[U](f: A => U): Stream[Unit, T] =
    map[Unit](a => f(a))

  def filter(f: A => Boolean): Stream[A, T] =
    new Stream[A, T] {

      override def headOption: T[Option[A]] =
        self.headOption flatMap {
          previousAOption =>
            previousAOption map {
              a =>
                if (f(a))
                  tag.success(previousAOption)
                else
                  next(a)
            } getOrElse tag.none
        }

      override private[swaydb] def next(previous: A): T[Option[A]] =
        tag.collectFirst(previous, self)(f)
    }

  def filterNot(f: A => Boolean): Stream[A, T] =
    filter(!f(_))

  def flatMap[B](f: A => Stream[B, T]): Stream[B, T] =
    new Stream[B, T] {
      //cache stream and emits it's items.
      //next Stream is read only if the current cached stream is emitted.
      var innerStream: Stream[B, T] = _
      var previousA: A = _

      def streamNext(nextA: A): T[Option[B]] = {
        innerStream = f(nextA)
        previousA = nextA
        innerStream.headOption
      }

      override def headOption: T[Option[B]] =
        self.headOption flatMap {
          case Some(nextA) =>
            streamNext(nextA)

          case None =>
            tag.none
        }

      override private[swaydb] def next(previous: B): T[Option[B]] =
        innerStream.next(previous) flatMap {
          case some @ Some(_) =>
            tag.success(some)

          case None =>
            self.next(previousA) flatMap {
              case Some(nextA) =>
                streamNext(nextA)

              case None =>
                tag.none
            }
        }
    }

  /**
   * Converts the current Stream with Future API. If the current stream is blocking,
   * the output stream will still return blocking stream but wrapped as future APIs.
   */
  def toFuture(implicit ec: ExecutionContext): Stream[A, Future] =
    new Stream[A, Future]()(Tag.future) {
      override def headOption: Future[Option[A]] =
        self.tag.toFuture(self.headOption)

      override private[swaydb] def next(previous: A): Future[Option[A]] =
        self.tag.toFuture(self.next(previous))
    }

  /**
   * If the current stream is Future/Async this will return a blocking stream.
   *
   * @param timeout If the current stream is async/future based then the timeout is used else it's ignored.
   */
  def toIO[E: ErrorHandler](timeout: FiniteDuration): Stream[A, IO.ApiIO] =
    new Stream[A, IO.ApiIO] {
      override def headOption: IO.ApiIO[Option[A]] =
        self.tag.toIO(self.headOption, timeout)

      override private[swaydb] def next(previous: A): IO.ApiIO[Option[A]] =
        self.tag.toIO(self.next(previous), timeout)
    }

  /**
   * If the current stream is Async this will return a blocking stream.
   *
   * @param timeout If the current stream is async/future based then the timeout is used else it's ignored.
   */
  def toTry(timeout: FiniteDuration): Stream[A, Try] =
    new Stream[A, Try] {
      override def headOption: Try[Option[A]] =
        self.tag.toIO[Throwable, Option[A]](self.headOption, timeout).toTry

      override private[swaydb] def next(previous: A): Try[Option[A]] =
        self.tag.toIO[Throwable, Option[A]](self.next(previous), timeout).toTry
    }

  /**
   * Reads all items from the Stream and returns the last.
   *
   * For a more efficient one use swaydb.Map.lastOption or swaydb.Set.lastOption instead.
   */
  def lastOption: T[Option[A]] =
    foldLeft(Option.empty[A]) {
      (_, next) =>
        Some(next)
    }

  /**
   * Materializes are executes the stream.
   */
  def foldLeft[B](initial: B)(f: (B, A) => B): T[B] =
    tag(()) flatMap {
      _ =>
        tag.foldLeft(initial, None, self, 0, None)(f)
    }

  /**
   * Folds over all elements in the Stream to calculate it's total size.
   */
  def size: T[Int] =
    foldLeft(0) {
      case (size, _) =>
        size + 1
    }

  /**
   * Materialises/closes and processes the stream to a [[Seq]].
   */
  def materialize: T[Seq[A]] =
    foldLeft(new StreamBuilder[A, T]()) {
      (buffer, item) =>
        buffer += item
    } map (_.asSeq)
}
